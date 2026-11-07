# sandbox-fs

把远程沙箱（vercel-sandbox）的文件系统暴露成本地 Python API、命令行和 FTP 服务。

沙箱本身只提供 WebSocket + PTY 通道。sandbox-fs 在通道里自举一个常驻
worker，之后所有文件操作复用同一条连接，避免每次操作都重新握手。

## 特性

- POSIX 风格的 Python 文件系统 API（`listdir` / `read` / `write` / `makedirs` …）
- 匿名 FTP 服务，可用 FileZilla / WinSCP / lftp 直连
- 命令行工具：`ls` / `cat` / `put` / `get` / `rm` / `mv` / `run` / `stat`
- 大文件自动选择内联帧或 `sandbox cp`，内存占用与文件大小无关


## 命令参考

| 命令 | 说明 |
|---|---|
| `ftp` | 启动 FTP；无参数运行等同于 `ftp` 后台 + `ssh` 前台 |
| `ls [-l] PATH` | 列目录 |
| `cat PATH [-o 本地文件]` | 输出或下载文件 |
| `put LOCAL REMOTE` | 上传 |
| `get REMOTE LOCAL` | 下载 |
| `rm PATH [-r]` | 删除文件或目录 |
| `mkdir PATH` | 创建目录（自动建父级） |
| `mv SRC DST` | 移动 / 改名 |
| `stat PATH` | 查看元信息 |
| `run CMD` | 在沙箱里执行命令 |
| `ping` | 连接信息（worker pid、主机、可用空间） |
| `doctor` | 诊断环境与连接 |

全局参数：`--sandbox NAME`（默认 `sandbox`）、`--root PATH`（默认 `auto`，自动探测沙箱初始路径）、`--host` / `--port`（FTP 监听，默认 `0.0.0.0:2121`）、`-v` 打印调试日志。

`ftp` 子命令额外支持 `--user/--password`（具名账号）、`--no-anonymous`（关闭匿名）、`--foreground `（前台运行）。

一键脚本 `./start.sh` 等同于无参数运行；`./start.sh --no-ssh` 只起 FTP。

## 工作原理

`sandbox ssh` 每次握手耗时极长，无法逐操作连接；sandbox-fs 启动时在
沙箱里自举一个常驻 worker，之后所有请求在同一条 WebSocket 通道上按
帧 id 多路复用。

帧协议为定长头 + JSON 头 + 载荷（`SBOX` magic），DATA 帧用 END 标志
界定消息边界，支持流式读写与并发请求。通道有两个实测限制：上行单帧
不超过 256KB，下行单片不超过 96KB，超出会卡死连接，因此协议内部分片
传输，大文件写入自动转交 `sandbox cp`。

通道由 Node 桥接（`bridge.mjs`）承载：复用 sandbox CLI 的登录凭据，
把 WS 二进制帧与子进程 stdin/stdout 原样对接。

## 故障排查

- **连接失败 / 提示沙箱不存在**：`sandbox ls` 确认沙箱状态，必要时
  `sandbox create <名称>`。
- **提示 websocket closed by peer (1006)**：通常是 `cwd` 在沙箱中不
  存在，用 `--root` 指定真实目录。
- **鉴权失败**：运行 `sandbox login` 或设置 `VERCEL_TOKEN`。
- **长时间空闲后失效**：远程沙箱会被回收，重新执行命令即可自动重建。
- 调试通道可设环境变量 `SANDBOXFS_DEBUG=1`；运行 `python3 -m sandbox_fs doctor`
  做一次完整检查。


## 许可证

MIT
