// sandbox_fs Node 桥接：拿到沙箱 WebSocket 通道，并把 stdin/stdout 原样对接。
//
// 由 sandbox_fs.transport 调用：
//   node bridge.mjs --sandbox <名称> --cwd /workspace --command python3 \
//                   --arg -c --arg <bootstrap>
//
// 行为：
//   * 复用 sandbox CLI 已登录的凭据（@vercel/sandbox 的 getAuth/inferScope）
//   * 建立 WS，发送 {"type":"start", command, args, env, cwd}
//   * WS 二进制帧原样写到 stdout；stdin 字节原样作为 WS 帧发送
//   * 收到 {"type":"exit","code":n} 后以该码退出
//
// 退出码约定（供 Python 侧区分错误类型）：
//   3 = 桥接自身错误   4 = 未登录/鉴权失败   5 = 沙箱不可用

import { createRequire } from 'node:module'
import process from 'node:process'
import fs from 'node:fs'
import path from 'node:path'

const require = createRequire(import.meta.url)

function fail(msg, code = 3) {
  process.stderr.write(`[sandbox_fs-bridge] ${msg}\n`)
  process.exit(code)
}

// ---- 参数 --------------------------------------------------------------
const opts = { args: [], sandbox: 'sandbox', cwd: '/workspace', command: '' }
const argv = process.argv.slice(2)
for (let i = 0; i < argv.length; i++) {
  const a = argv[i]
  const next = () => {
    if (i + 1 >= argv.length) fail(`missing value for ${a}`)
    return argv[++i]
  }
  if (a === '--sandbox') opts.sandbox = next()
  else if (a === '--cwd') opts.cwd = next()
  else if (a === '--command') opts.command = next()
  else if (a === '--arg') opts.args.push(next())
  else fail(`unknown argument: ${a}`)
}
if (!opts.command) fail('--command is required')

// ---- 定位 @vercel/sandbox（优先环境变量，其次全局安装的 sandbox CLI）----
function locateSdk() {
  const envDir = process.env.SANDBOXFS_VERCEL_SDK
  if (envDir) {
    if (fs.existsSync(path.join(envDir, 'index.js'))) return path.join(envDir, 'index.js')
    if (fs.existsSync(envDir)) return envDir
  }
  // 从 PATH 里的 sandbox CLI 反推 node_modules
  const which = (name) => {
    for (const dir of (process.env.PATH || '').split(path.delimiter)) {
      const p = path.join(dir, name)
      try {
        if (fs.statSync(p).isFile()) return fs.realpathSync(p)
      } catch {}
    }
    return null
  }
  try {
    return require.resolve('@vercel/sandbox')
  } catch {}
  try {
    const cli = which('sandbox')
    if (cli) {
      const p = path.join(path.dirname(cli), '..', 'node_modules', '@vercel', 'sandbox', 'dist', 'index.js')
      if (fs.existsSync(p)) return p
    }
  } catch {}
  try {
    const cli = require.resolve('sandbox/bin/sandbox.mjs')
    const p = path.join(path.dirname(cli), '..', 'node_modules', '@vercel', 'sandbox', 'dist', 'index.js')
    if (fs.existsSync(p)) return p
  } catch {}
  return null
}

const sdkPath = locateSdk()
if (!sdkPath) fail('cannot locate @vercel/sandbox (set SANDBOXFS_VERCEL_SDK)', 4)
const sdkDir = path.dirname(sdkPath)

const { Sandbox } = await import(sdkPath)

async function loadAuthHelpers() {
  // 相对 sandbox.js 的 auth 子模块，两种布局都兼容
  for (const base of [sdkDir, path.join(sdkDir, '..')]) {
    const a = path.join(base, 'auth', 'index.js')
    const b = path.join(base, 'auth', 'project.js')
    if (fs.existsSync(a) && fs.existsSync(b)) {
      const idx = await import(a)
      const proj = await import(b)
      if (idx.getAuth && proj.inferScope) return { getAuth: idx.getAuth, inferScope: proj.inferScope }
    }
  }
  throw new Error('auth helpers not found')
}

let getAuth
let inferScope
try {
  ;({ getAuth, inferScope } = await loadAuthHelpers())
} catch (e) {
  fail(e.message, 4)
}

// ---- 鉴权 --------------------------------------------------------------
let token = process.env.VERCEL_TOKEN || process.env.VERCEL_AUTH_TOKEN || ''
let teamId = process.env.VERCEL_TEAM_ID || process.env.SANDBOXFS_TEAM_ID || undefined
let projectId = process.env.VERCEL_PROJECT_ID || process.env.SANDBOXFS_PROJECT_ID || undefined

if (!token) {
  let auth
  try {
    auth = getAuth()
  } catch (e) {
    fail('not logged in: ' + e.message, 4)
  }
  if (!auth || !auth.token) fail('not logged in; run `sandbox login`', 4)
  token = auth.token
}
if (!teamId || !projectId) {
  try {
    const scope = await inferScope({ token })
    teamId = teamId || scope.teamId
    projectId = projectId || scope.projectId
  } catch (e) {
    fail('cannot infer scope: ' + e.message, 4)
  }
}

// ---- 打开交互通道 ------------------------------------------------------
let sandbox
try {
  sandbox = await Sandbox.get({
    name: opts.sandbox,
    resume: true,
    token,
    teamId,
    projectId,
    __includeSystemRoutes: true,
  })
} catch (e) {
  fail(`sandbox "${opts.sandbox}" unavailable: ${e.message}`, 5)
}

let wsUrl
try {
  const { url, token: wsToken } = await sandbox.openInteractive()
  wsUrl = `${url}?token=${encodeURIComponent(wsToken)}`
} catch (e) {
  fail('openInteractive failed: ' + e.message, 5)
}

// 官方 CLI 在建 WS 前会先探测 /health；这里同样预检一次（失败不致命）。
try {
  const { hostname } = new URL(wsUrl)
  const r = await fetch(`https://${hostname}/health`)
  if (!r.ok) process.stderr.write(`[sandbox_fs-bridge] health precheck: ${r.status}\n`)
} catch (e) {
  process.stderr.write(`[sandbox_fs-bridge] health precheck failed: ${e.message}\n`)
}

async function loadWs() {
  // ws 是 sandbox CLI 的依赖；从 SDK 目录向上找 node_modules
  const tried = []
  try {
    return (await import('ws')).WebSocket
  } catch (e) {
    tried.push(e.message)
  }
  const cliPath = path.join(sdkDir, '..', '..', '..', 'ws', 'index.js')
  tried.push(cliPath)
  const wsPath = fs.existsSync(cliPath) ? cliPath : path.join(sdkDir, 'node_modules', 'ws', 'index.js')
  if (fs.existsSync(wsPath)) {
    const m = await import(wsPath)
    return m.WebSocket ?? m.default
  }
  fail('cannot locate the "ws" module: ' + tried.join(' | '), 3)
}

const WebSocket = await loadWs()
const ws = new WebSocket(wsUrl, { perMessageDeflate: false, maxPayload: 512 * 1024 * 1024 })

// ---- 数据泵 ------------------------------------------------------------
function writeOut(buf) {
  let off = 0
  while (off < buf.length) {
    try {
      off += fs.writeSync(1, buf, off, buf.length - off)
    } catch (e) {
      if (e.code === 'EAGAIN') continue
      if (e.code === 'EPIPE') process.exit(0)
      fail('stdout write failed: ' + e.message)
    }
  }
}

let done = false
let childExited = false
function shutdown(code) {
  if (done) return
  done = true
  if (code === undefined) childExited = true
  try { ws.close() } catch {}
  process.exit(typeof code === 'number' ? code : 0)
}

// 本地侧要断开：先断开 WS，再给远端一点时间收尸（避免"点了中断"变成杀进程）
function abort() {
  if (done) return
  done = true
  try { ws.close() } catch {}
  setTimeout(() => process.exit(0), 300).unref?.()
  setTimeout(() => process.exit(0), 300)
}
process.on('SIGINT', abort)
process.on('SIGTERM', abort)
process.stdin.on('end', () => setTimeout(() => abort(), 1500))

let outbox = []        // open 之前收到的输入先缓存，连上后按序补发
let outboxBytes = 0    // 累计长度（避免每次发送都遍历求和）
const OUTBOX_LIMIT = 64 * 1024 * 1024

function sendOut(buf) {
  if (process.env.SANDBOXFS_DEBUG) {
    process.stderr.write(`[sandbox_fs-bridge] out ${buf.length} bytes state=${ws.readyState}\n`)
  }
  if (ws.readyState === 1) {
    ws.send(buf)
    return
  }
  if (outboxBytes + buf.length <= OUTBOX_LIMIT) {
    outbox.push(Buffer.from(buf))
    outboxBytes += buf.length
  }
}

// stdin 上的输入帧在连接建立前就要开始接收（否则会丢数据）
// stdin 上的控制帧：\x00{"t":"in","n":N}\n 后紧跟 N 字节，转成 WS 帧
let pending = Buffer.alloc(0)
process.stdin.on('data', (chunk) => {
  pending = pending.length ? Buffer.concat([pending, chunk]) : chunk
  while (pending.length) {
    let header = null
    if (pending[0] === 0) {
      // 控制帧：\x00 + JSON + \n
      if (pending.length < 2) return          // 连第二个字节都还没到
      const nl = pending.indexOf(0x0a)
      if (nl < 0) return                      // 头还没收全，等后续数据
      try { header = JSON.parse(pending.subarray(1, nl).toString()) } catch { header = null }
      if (!header) { pending = pending.subarray(1); continue }  // 解析失败：丢 1 字节重新对齐
      pending = pending.subarray(nl + 1)
    }
    if (!header) {
      // 裸字节（直接管道用法，非控制帧）：原样透传
      sendOut(pending)
      pending = Buffer.alloc(0)
      return
    }
    const need = header.n | 0
    if (need < 0) continue
    if (pending.length < need) return         // 数据还没收全
    if (need > 0) sendOut(pending.subarray(0, need))
    pending = pending.subarray(need)
  }
})
process.stdin.on('error', () => {})
process.stdin.resume()

// start 帧的 env 必须是 ["K=V", ...] 数组（实测：SDK 3.9.x 服务端只认
// 数组，传对象不会报错但进程永远不启动、连接零回帧）。
ws.on('open', () => {
  process.stderr.write('[sandbox_fs-bridge] websocket open, starting ' + opts.command + '\n')
  ws.send(JSON.stringify({
    type: 'start',
    command: opts.command,
    args: opts.args,
    env: ['TERM=dumb'],
    cwd: opts.cwd,
    cols: 256,
    rows: 64,
  }))
  for (const buf of outbox) {
    if (buf.length && ws.readyState === 1) ws.send(buf)
  }
  outbox = []
  outboxBytes = 0
})

ws.on('message', (data, isBinary) => {
  if (process.env.SANDBOXFS_DEBUG) process.stderr.write(`[sandbox_fs-bridge] frame bin=${isBinary} len=${data.length}\n`)
  if (isBinary) {
    writeOut(data)
    return
  }
  const txt = data.toString()
  try {
    const msg = JSON.parse(txt)
    if (msg && msg.type === 'exit') shutdown(typeof msg.code === 'number' ? msg.code : 0)
    return
  } catch {}
  writeOut(Buffer.from(txt))
})

ws.on('error', (e) => fail('websocket error: ' + e.message))
ws.on('close', (code) => {
  if (done) return
  process.stderr.write(`[sandbox_fs-bridge] websocket closed by peer (${code})\n`)
  shutdown(childExited ? 0 : 255)
})
