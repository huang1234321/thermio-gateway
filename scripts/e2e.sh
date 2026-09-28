#!/usr/bin/env bash
# 端到端主链 e2e 编排（验收 1：发现→导出→上行→缓存补传→下行面）。
#
# 环境隔离纪律（2026-09-26 指示；与 ingest 仓 scripts/integration.sh 同式）：
# - 以**唯一 compose project** 运行伞仓 deploy/docker-compose.dev.yml 的子集
#   （仅 emqx）：剥离固定 container_name/网络名，容器/网络/卷全部 project
#   作用域——与共享的 thermio-dev 栈（含在飞的 DAT-156 e2e）互不可见互不踩；
# - EMQX 端口动态选取（宿主 1883 常被占：改本项目映射，不动既有服务）；
# - 结束即 teardown（-v 清卷），无残留。
#
# 用法：scripts/e2e.sh [umbrella-repo-path]
#   umbrella 默认 ../thermio（与 thermio-gateway 同级检出），或设 THERMIO_UMBRELLA。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UMBRELLA="${1:-${THERMIO_UMBRELLA:-$(cd "$REPO_ROOT/.." && pwd)/thermio}}"
COMPOSE_FILE="$UMBRELLA/deploy/docker-compose.dev.yml"
if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "ERROR: 找不到 ${COMPOSE_FILE}（传伞仓路径或设 THERMIO_UMBRELLA）" >&2
  exit 2
fi
command -v nc >/dev/null || { echo "ERROR: 需要 nc（端口探测）" >&2; exit 2; }

# ── 动态端口（跳过本机占用；EMQX 四个映射口全部改本项目映射——
#    共享 thermio-dev 栈占着 8083 等既有位，错峰不抢占）──────────────────
PICKED=" "
pick_port() {
  local __var="$1" port="$2"
  while [[ "$PICKED" == *" $port "* ]] || nc -z 127.0.0.1 "$port" 2>/dev/null; do
    port=$((port + 1))
  done
  PICKED="$PICKED$port "
  printf -v "$__var" '%s' "$port"
}
pick_port EMQX_PORT 18830
pick_port EMQX_WS_PORT 28083
pick_port EMQX_MQTTS_PORT 28883
pick_port EMQX_DASH_PORT 28083
echo "emqx ports: mqtt=$EMQX_PORT ws=$EMQX_WS_PORT mqtts=$EMQX_MQTTS_PORT dash=$EMQX_DASH_PORT"

# ── 唯一 compose project（防同胞会话互踩：DAT-156 共用 thermio- 栈，错峰）──
RUN_ID="gweit$(date +%s)"
PROJECT="thermio-gw-it-$RUN_ID"
BUILD_DIR="$REPO_ROOT/build"
mkdir -p "$BUILD_DIR"
sed -E -e '/^[[:space:]]*container_name:/d' \
       -e '/^[[:space:]]*name: thermio-dev$/d' \
       "$COMPOSE_FILE" > "$BUILD_DIR/docker-compose.e2e.yml"
ENV_FILE="$BUILD_DIR/e2e.env"
cat > "$ENV_FILE" <<EOF
EMQX_MQTT_PORT=$EMQX_PORT
EMQX_WS_PORT=$EMQX_WS_PORT
EMQX_MQTTS_PORT=$EMQX_MQTTS_PORT
EMQX_DASHBOARD_PORT=$EMQX_DASH_PORT
EOF
COMPOSE="docker compose -p $PROJECT --env-file $ENV_FILE -f $BUILD_DIR/docker-compose.e2e.yml"
trap '$COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

echo "== 启动隔离栈（project=${PROJECT}：emqx）"
$COMPOSE up -d --wait emqx

echo "== pytest 主链（tests/e2e，标记 e2e）"
( cd "$REPO_ROOT" &&
  E2E_EMQX_HOST=127.0.0.1 E2E_EMQX_PORT=$EMQX_PORT \
  uv run pytest -m e2e -v --no-header tests/e2e/ )
