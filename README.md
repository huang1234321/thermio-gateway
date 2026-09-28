# thermio-gateway

thermio BACnet/IP 软采集服务（Python，ADR-001/016）：跑在**楼内小主机**上——
对下说 BACnet/IP（bacpypes3 协议栈），对上以**成品 MQTT 网关的身份**说 MQTT。
设计真源：伞仓 `docs/design/gateway.md` v1.0（DAT-128，PR #36）；契约对端：
`ingest.md` §3（上行信封，逐字段零差异）、`M2-import.md` §8.4/§9.2（config/read）、
`control-safety.md` §4（write/event）、`emqx.md` §2/§4（身份与 ACL）。

## 边界（§1）

- **对云端 = 一台成品网关**：6 个既有 topic，无任何「软采集特判」；
- **对 PG/TSDB/Kafka 零连接**：配置唯一来源 = `down/config` retained 消息 +
  本地点表（结构守护测试断言配置模型无 DSN 字段）；
- **不做业务逻辑**：只认 raw_name / 对象地址 / 单位 / 质量位；
- **不越 Kafka 缝**：边缘侧上行出口只有 MQTT。

## 能力面

| 能力 | 设计锚点 |
|---|---|
| 点位发现与点表导出（喂 M2 导入向导） | §4.1（`discover` 三相 + xlsx canonical 四列） |
| 轮询调度（RPM 优先 + 逐对象降级 + 节流） | §4.4 |
| MQTT 上行（telemetry_batch 信封零改动兼容） | §5（对齐表 §5.1） |
| 断网本地缓存与补传（SQLite WAL 4 天 FIFO） | §6（ADR-003 硬指标 3） |
| 下行面 config / read / write / offline_action | §7 |
| 同址部署形态与资源预算 | §8（试点推荐形态 a） |

## 布局（§12）

```
src/thermio_gateway/
  cli.py               子命令 run / discover / pointmap adopt / export / doctor
  config.py            env 配置（§11；fail-fast；无 DSN 字段）
  contracts.py         三套信封 pydantic 模型（ingest §3 / M2 §8.4 / CS §4）
  pointmap.py          raw_name 会合模型 + §3.3 生成规则 + adopt 版本迁移（§14.3-3）
  units.py             BACnet eng-units → ingest token 全集映射（§14.3-1）+ 双向换算
  cache.py             断网缓存 FIFO / 排水 / 保留期（§6）
  db.py                SQLite WAL 单文件底座（pointmap + cache + meta）
  mqtt_agent.py        paho-mqtt 2.x 接入（§2；asyncio 线程桥）
  service.py           常驻总装（排水循环 / 下行三面 / 地址解析 / 优雅退出）
  metrics.py           Prometheus 文本端点 :9101（§10，手写最小暴露格式）
  bacnet/stack.py      bacpypes3 封装（节流 §4.4；错误归一）
  bacnet/poll.py       轮询调度器（分组 / RPM 批 / 降级记忆 / 质量戳）
  bacnet/discover.py   发现三相 + 三件产物（报告/staging/xlsx）
  downlink/config.py   down/config 应用 + ack（§7.1）
  downlink/read.py     down/read 自检插队（§5.3/§7.2）
  downlink/write.py    write_cmd/read_cmd + cmd_id 幂等（§7.3）
  downlink/offline_action.py  断链安全值状态机（§7.4）
tests/                 L1 单测 + VLAN 进程内仿真（RPM 两型；tests/sim/）
tests/e2e/             主链 e2e（scripts/e2e.sh 自起隔离 compose 栈驱动）
scripts/e2e.sh         端到端编排（发现→导出→上行→缓存补传→下行面）
```

依赖钉版本（§9.2 + PY-03 uv 锁文件）：`bacpypes3==0.0.110`、`paho-mqtt==2.1.0`、
`openpyxl==3.1.5`、`pydantic`。

## 快速上手（部署期三步 + 常驻）

```bash
uv sync
cp .env.example .env          # 填 M1 登记产物（serial/clientid/凭证）与网口

thermio-gateway doctor                   # device-id 冲突/DB/MQTT 自检（§9.4）
thermio-gateway discover --out-dir ./out # 三件产物：报告 JSON + staging + xlsx
thermio-gateway pointmap adopt           # 确认后生效（漂移/新增清单见输出）
thermio-gateway run                      # 常驻（down/config retained 自动收敛采集集）
```

上行链路联通需云端 M2 侧 apply 推 `down/config`（retained 重连自动补课）。

## 配置（§11，全部环境变量）

见 `.env.example` 全表（占位值，SEC-KEY-06）。要点：

| 变量 | 默认 | 说明 |
|---|---|---|
| `MQTT_BROKER_URL` | — | 生产 `mqtts://...:8883` + `MQTT_CA_PATH`（显式信任链） |
| `BACNET_DEVICE_ID` | — | 本机 device 身份（BA 厂商分配段；doctor 自检冲突） |
| `BACNET_INTERFACE` | `0.0.0.0` | 绑定网口 IP（可达 BA VLAN；可选 `/掩码`） |
| `POLL_DEFAULT_INTERVAL_S` | 60 | 默认采集周期（pointmap 逐点 `interval_s` 覆盖） |
| `CACHE_RETENTION_DAYS` | 4 | > 硬指标 3 天，< ingest cagg 5 天窗（§6.1） |
| `REPLAY_INFLIGHT_MSGS` × `REPLAY_BATCH_POINTS` | 4 × 500 | 排水窗口（§6.2） |
| `OFFLINE_ACTION_DELAY_S` | 60 | 断链安全值触发窗（§7.4） |

## 开发

```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run pytest                       # L1 + VLAN 仿真（无外部服务依赖）
scripts/e2e.sh                      # 主链 e2e：自起伞仓 deploy compose 隔离栈
                                    #（唯一 compose project + 动态端口，与并发会话互不踩）
```

CI（`.github/workflows/ci.yml`）：lint（ruff）/ type（mypy）/ test（pytest）。
e2e 需真实 EMQX，本机/交付环境执行，不进 CI。

环境隔离纪律（2026-09-26 指示）：涉及中间件的开发/测试/联调一律用本项目
deploy compose 独立栈或一次性干净容器，禁止复用宿主机/其他项目既有服务。

## 容器化注意（§8.2）

BACnet 依赖 UDP 广播/多播（0xBAC0:47808）——docker 默认 bridge **不过广播**，
采集容器必须 `network_mode: host`（Linux）或 macvlan；这是 BACnet 容器化的
经典坑，部署 checklist 固定项。
