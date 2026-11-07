#!/usr/bin/env bash
# 一键启动 sandbox-fs：确保沙箱在跑 → FTP 挂 0.0.0.0:2121
#
#   ./start.sh              # FTP 后台 + 进入 SSH 交互（Ctrl-C 停 FTP）
#   ./start.sh --no-ssh     # 只起 FTP（后台，关终端不退出）
#
# 停止 FTP: kill <启动时打印的 pid>
set -euo pipefail
cd "$(dirname "$0")"

SANDBOX="sandbox"
PORT=2121
ROOT="auto"                                  # 自动探测沙箱真实工作目录

# 确保沙箱可用：sandbox ls 不显示 timeout 归零的沙箱，但 exec 会唤醒它；
# 只有唤醒失败（真的不存在）才 create —— 直接 create 同名会被 400 拒绝。
# 该逻辑归 sandbox_fs.tool.ensure_sandbox 所有，这里只调用。
python3 -c "import sys; sys.path.insert(0,'$PWD');
from sandbox_fs.tool import ToolClient;
ToolClient(sandbox='$SANDBOX').ensure_sandbox();
print('沙箱 $SANDBOX 就绪')"

if [ "${1:-}" = "--no-ssh" ]; then
  exec python3 -m sandbox_fs --sandbox "$SANDBOX" --root "$ROOT" \
       ftp --host 0.0.0.0 --port "$PORT"
else
  exec python3 -m sandbox_fs --sandbox "$SANDBOX" --root "$ROOT" \
       --port "$PORT"                        # 无子命令 = FTP 后台 + SSH 前台
fi
