#!/usr/bin/env bash
# 启动湖表压实 HTTP 后端
# 用法: ./run.sh [host] [port] [data_dir]
set -euo pipefail
cd "$(dirname "$0")"

HOST="${1:-127.0.0.1}"
PORT="${2:-8080}"
DATA="${3:-./data}"

exec python3 -m lakecomp.server --host "$HOST" --port "$PORT" --data "$DATA"
