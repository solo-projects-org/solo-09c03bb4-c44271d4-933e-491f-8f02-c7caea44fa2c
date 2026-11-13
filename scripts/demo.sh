#!/usr/bin/env bash
# LIMS 入仓演示：END 先行 + 延迟检验值 + 重启续收 + 整笔入仓
set -euo pipefail
cd "$(dirname "$0")/.."

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
show() { curl -s "localhost:8000$1" | python3 -m json.tool; }

say "1. 启动 Kafka + 入仓服务（Swagger: http://localhost:8000/docs）"
docker compose up -d --build

say "2. 场景一：tx-1001 完整；tx-1002 先发 END、暂扣一条检验值；tx-1003 完整但须等待"
docker compose --profile tools run --rm generator demo1
sleep 2

say "3. 入仓进度：tx-1002 收 2/3 等待；tx-1003 已齐但被队头阻塞"
show /ingest/status

say "4. 一致性读：R002 仍是旧已提交状态（pending、无检验值）"
show /reports/R002

say "5. 重启入仓服务：状态从 SQLite 恢复，tx-1001 不重复应用"
docker compose restart app
sleep 3
show /ingest/status

say "6. 场景二：补发 tx-1002 暂扣的检验值 + 完整 tx-1004"
docker compose --profile tools run --rm generator demo2
sleep 2

say "7. 整笔入仓：R002 审核通过与两条检验值同时可见"
show /reports/R002

say "8. 最终进度"
show /ingest/status
