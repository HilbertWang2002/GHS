# GHS 本地实验调度器

GHS 在一台 Linux 主机上调度 CPU 和 NVIDIA GPU 实验。命令行与 MCP 共用一个 `ghs` 可执行文件，通过同一个 Unix socket 访问 daemon。每次尝试由 daemon 启动独立的执行器进程。用户可以在提交命令返回后关闭终端，再用原来的 ID 查询任务。

## 安装、登录自启动与首次提交

在工程根目录执行下列命令。默认数据目录为 `~/.local/state/ghs`，包含 socket、SQLite、批次清单和运行输出。构建需要支持 Rust 2024 的工具链和 SQLite 开发库，GPU 查询需要主机 NVML 驱动及可读的主机进程信息。示例使用 Python 3 标准库。

```sh
make install
export PATH="$HOME/.local/bin:$PATH"
systemctl --user daemon-reload
systemctl --user enable --now ghs.service
systemctl --user status ghs.service
```

`make install` 将程序安装为 `~/.local/bin/ghs`，将用户服务安装到 `${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/ghs.service`。启用后，systemd 会在用户登录时启动调度器。用 `journalctl --user -u ghs.service -f` 查看实时日志，用 `systemctl --user restart ghs.service` 重启服务。停止服务并关闭登录自启动可执行 `systemctl --user disable --now ghs.service`。停止 daemon 后，运行中的尝试会继续执行，下次启动时恢复接管。

手动运行时，`ghs daemon` 在前台启动服务，`ghs daemon --detach` 在后台启动并在 socket 就绪后返回 PID，日志写入数据目录的 `daemon.log`。同一数据目录同时只允许一个 daemon。

```sh
ghs submit experiments/sha256.json --request-id sha256-check-1
ghs list --project sha256
ghs compare --purpose validation --metric elapsed
```

示例对 32 MiB 数据计算 SHA-256，保存摘要和实测耗时。提交后会看到每个实验的本地短编号，例如 `@1`。同一数据库里的编号跨重启、重试和升级有效。相同请求 ID 和相同 JSON 内容返回原回执，修改清单后需使用新请求 ID。

`ghs list` 每行显示一个实验的编号、组别、状态、进度、开始时间、已用时、优先级、允许和实际使用的 GPU。同一次提交属于一组（如 `B1`）。批次描述采用「标题：说明」时，组别列也显示标题。开始时间取首次启动尝试的本地时间，已用时累计各次尝试的运行区间，并行的时间只计一次。

允许 GPU 取当前阶段的允许列表与调度池的交集，有多个并行阶段时取并集。运行时显示当前分配的卡号，结束后显示历史用过的卡号，排队时实际 GPU 显示 `—`。历史设备在当前主机上没有对应卡号时显示记录中的 UUID。

`ghs get @1` 查看实验细节，`ghs list --ids` 加上完整 UUID。脚本可用 `ghs list --json` 等命令读取结构化数据。MCP 同样支持本地短编号和完整运行 ID。

```sh
ghs status --processes
ghs status --watch
ghs status --json
ghs get @1
ghs events "$ATTEMPT_ID" --after 0
ghs log "$ATTEMPT_ID" --offset 0 --length 4096
ghs artifacts "$RUN_ID"
ghs cancel "$ATTEMPT_ID"
ghs retry "$STAGE_ID"
```

`ghs status` 顶部显示本机运行中和排队中的实验数量，包括 CPU 任务和验证任务，每个实验只计一次。每张卡占一行。常见状态为 `RUNNING`（运行 GHS 实验）、`IDLE`（空闲或外部显存低于阈值）和 `EXTERNAL`（外部显存达到阈值）。`CHECK` 表示归属或采样需核查。显存单位 G 按 1,073,741,824 字节换算。

进度取自任务写入的 `GHS_EVENTS`。预计剩余时间按当前阶段最近两次同单位、同总量的有效上报速度计算。样本不足或进度停滞时显示 `—`。CPU 阶段在 GPU 表格下方单列。

## 资源与队列

手动启动时用 `ghs daemon --config /绝对路径/ghs.toml` 指定配置。用户服务可通过 `systemctl --user edit ghs.service` 在 `[Service]` 段设置 `Environment=GHS_CONFIG=/绝对路径/ghs.toml`，然后重启服务。daemon 每轮分配前重新读取配置，状态接口读取同一份配置。未指定配置时，CPU 容量取当前进程可用并行度，所有可核实的物理 GPU 都在候选范围内。

```toml
[scheduler]
external_memory_block_percent = 5
cpus = 8
max_gpus = 4
poll_interval_seconds = 5
# 可从 ghs status --json 复制实际 UUID
# gpu_pool = ["GPU-..."]
```

- CPU 按并发槽预留，应用可读取 `GHS_CPUS` 设置线程数。GPU 按 UUID 独占预留，并通过 `CUDA_VISIBLE_DEVICES` 交给任务。CPU 阶段的可见 GPU 集合为空。
- `max_gpus` 限制同一 daemon 下所有批次合计预留的物理 GPU 张数。设为 4 时，若已用 3 张，申请 2 张的阶段会排队，之后申请 1 张的阶段仍可运行。单个阶段申请量超过上限时也会排队，调高上限后重新判断。降低上限保留运行中的尝试，设为 0 暂停新 GPU 分配，省略时不设数量上限。
- daemon 每轮核对尝试状态、重读配置并调度，然后等待 `poll_interval_seconds` 秒（默认 5）。有 GPU 请求通过名额检查时，同一轮共享一次 NVML 采样，之后每次分配都会更新预留。单次 `ghs status` 或 MCP `get_gpu_status` 会即时采样，`ghs status --watch` 按配置的周期刷新。结构化快照包含 `run_counts`、`active_attempts`、`reserved_gpus`、`max_gpus` 和 `poll_interval_seconds`。
- GPU 状态涵盖池外设备、计算及图形进程、当前预留和采集错误。每卡按外部 PID 去重后累加字节数，占比达到阈值即停止新分配。无法核实进程或显存时显示 `needs_review` 和原因。
- 运行的 `priority` 越大越先调度，同优先级按阶段登记顺序。允许资源充足的阶段先运行。前置阶段成功后才执行后继阶段。
- 每个 run 采用 fail-fast：任一阶段失败，立即停止该 run 的后续调度，未执行阶段标记为 `skipped`，运行中的其他阶段由原 runner 取消。其他 run 继续执行。清理期间 run 显示 `failing`（失败清理中），全部尝试确认退出后显示 `failed`。失败阶段、尝试 ID 和原因保存在 `ghs get` 的 `failure` 中。
- runner 在单独的 Linux 会话中运行，并作为 subreaper 等待整个后代进程树。取消请求由原 runner 执行，使用 SIGKILL 清理其后代。daemon 接到退出记录后释放预留。身份无法核实时保留资源并显示具体错误，不能把已请求取消当作已退出。
- 清理完成后，`ghs retry-run @1` 显式重试失败的整个 run：保留成功阶段，重新排队失败、取消和跳过的阶段。每次执行有独立尝试和日志；每次 run 重试创建新的通知代次。`ghs retry STAGE_ID` 保留用于单独取消的阶段，已触发 fail-fast 的 run 必须使用 `retry-run`。GHS 不自动重试实验。
- 重启 daemon 时使用原数据目录。服务核对主机启动 ID、runner PID、进程启动时间和退出记录。证据缺失的尝试进入 `needs_review` 并保留预留，可通过 `ghs get` 的原因、尝试目录中的 `runner.log` 和 `identity.json` 排查。

## 项目接入

任务清单为版本 1 的 JSON。批次包含 `project`、`runs`，可提供 `description`、默认 `purpose` 和作为对照的运行名称 `baseline`。每个运行包含唯一的 `name` 和 `stages`，可以提供 `parameters`、`code_ref`、整数 `priority` 及用途覆盖。用途必须在批次或运行上声明为 `validation` 或 `experiment`。

每个阶段的 `command` 为程序和参数数组，按原样执行。可选字段包括绝对工作目录 `cwd`、字符串环境映射 `env`、同一运行内的前置阶段名称 `depends_on`、资源 `resources` 和预期文件 `artifacts`。资源默认一份 CPU、零张 GPU。`resources.allowed_gpus` 可以进一步限制该阶段可用的 UUID。`artifacts` 中的路径相对于本次尝试的 `GHS_OUTPUT_DIR`，重试会获得新的空目录。省略 `cwd` 时，工作目录即输出目录。

项目程序可以直接写 stdout 和 stderr。使用结构化接口时，runner 提供以下变量。

| 环境变量 | 内容 |
| --- | --- |
| `GHS_RUN_ID`、`GHS_STAGE_ID`、`GHS_ATTEMPT_ID`、`GHS_PURPOSE` | 当前执行身份和用途 |
| `GHS_RUN_DIR` | 运行目录，内有记录参数、代码引用及阶段 ID 的 `run.json` |
| `GHS_OUTPUT_DIR` | 本次尝试的产物目录 |
| `GHS_EVENTS`、`GHS_RESULTS` | 可选的进度、指标 JSON Lines 文件，各记录独占一行 |
| `GHS_CPUS`、`CUDA_VISIBLE_DEVICES` | 实际预留的 CPU 数量及 GPU UUID |

进度事件示例，项目写入时应替换为当前执行 ID。序号从正整数开始严格递增，`time` 使用 Unix 毫秒时间戳，进行中的记录以换行发布。

```json
{"version":1,"seq":1,"time":1795000000000,"attempt_id":"实际尝试 ID","stage_id":"实际阶段 ID","completed":30,"total":100,"unit":"samples"}
```

指标记录必须有名称、数值和来源。单位、方向、测量位置、分组和口径可选。每个返回值都附带运行、阶段、尝试、结果文件和行号。

```json
{"version":1,"metric":"latency","value":12.4,"source":"artifacts/raw.csv","unit":"ms","direction":"min","location":"decode","group":"test","protocol":"single-image-v1"}
```

`ghs compare --batch-id "$BATCH_ID" --metric latency --group test` 默认查询正式实验。`--purpose validation` 单独比较验证运行。`--baseline-id` 可指定对照，省略时使用清单的 `baseline`。差值为当前值减对照值。单位、测量位置、方向或口径不一致时保留原值和差异原因。失败、未完成、缺少所选指标的运行也出现在回包中。

## MCP 与历史目录

将 MCP 客户端的 stdio 启动程序设为 `ghs`，参数设为 `["mcp"]`，即执行 `ghs mcp`。若客户端的 PATH 未包含安装目录，启动程序填写 `~/.local/bin/ghs` 展开后的绝对路径。通过环境变量 `GHS_DATA_DIR` 或参数 `--socket /绝对路径/ghs.sock` 使用与 CLI 相同的服务。客户端可用 `tools/list` 发现设计指南中的 14 个工具，`tools/call` 返回 `structuredContent` 和兼容文本。协议版本为 2025-06-18。

在本机 Codex 首次注册时执行下列命令。第二条命令可随时查看当前账户保存的启动路径和参数。

```sh
codex mcp add ghs -- "$HOME/.local/bin/ghs" mcp
codex mcp get ghs
```

批次位于 `batches/<batch_id>/`。每个运行目录包含 `run.json` 和 `attempts/<attempt_id>/`，后者保存 `attempt.json`、`identity.json`、`output.log`、`runner.log`、`exit.json`、可选的结构化文件和 `artifacts/`。复制完整的版本 1 运行目录后，可在另一数据库中只读登记。

```sh
ghs import /archive/copied-run
ghs reindex --run-id "$RUN_ID"
```

历史元数据缺少用途时，导入命令须指定 `--purpose` 和 `--purpose-source`。移动已登记的历史目录后，对新路径再次执行 `import` 即可更新索引。导入和重建索引只写入当前服务的 SQLite。

## 任务结束后唤醒 T3 Code

当前只支持 T3 Code，由提交者显式传入 **T3 thread ID**。T3 管理的 Codex、agy 会话也使用 T3 thread ID。`ghs mcp` 不会根据进程环境或底层 provider ID 猜测目标。

先在 daemon 使用的配置文件中增加命名实例。保留原来的 `[scheduler]` 设置，把示例路径替换为本机的绝对路径：

```toml
[t3code.local]
executable = "/home/user/.local/bin/t3"
base_dir = "/home/user/.t3"
```

`base_dir` 必须对应正在运行且当前用户有权访问的 T3 实例；路径含义与 `t3 --base-dir` 相同。GHS 配置该路径不会启动 T3 服务。T3 CLI 版本需要支持 `thread send --json`。不同实例使用不同配置名称；当前不实现远程桥接。daemon 使用自身服务环境和该实例现有的认证，不依赖交互式 shell 的 PATH。配置中的可执行文件和数据目录在提交时校验并快照保存；之后修改配置不会把已提交任务的通知转发到另一实例。

```sh
ghs submit experiments/sha256.json --request-id sha256-wake-1 \
  --wake-instance local --wake-session "$T3_THREAD_ID"
```

`T3_THREAD_ID` 在示例中是调用者已核实的目标 ID；不要求 T3 自动提供这个环境变量。两个 `--wake-*` 参数必须同时提供。省略时只运行实验。MCP 在 `submit_batch` 参数中增加可选的 `wake`，与 `manifest` 同级：

```json
{
  "runtime": "t3code",
  "instance": "local",
  "session_id": "目标 T3 thread ID"
}
```

相同 `request_id` 必须携带相同 manifest 和 wake；目标不同返回冲突。无法识别的平台、实例或非法目标会在入队前报错。

通知按 **run** 发送：首次 stage 失败时立即生成一次 `run.failed` 事件，不等待其他阶段清理，也不等待同批次其他 run；所有阶段成功时生成一次 `run.succeeded` 事件。清理完成不会再为同一次失败发送通知。通知包含事件 ID、run/batch ID、代次、输出目录、失败阶段、原因及事件发生时是否仍待清理。agent 收到后应查询当前状态。手动取消单个 attempt 不产生成功或失败唤醒。

完成状态和待投递事件在同一 SQLite 事务内保存。独立投递进程调用 `t3 thread send --base-dir ... --json THREAD_ID -`，通过 stdin 传递消息。T3 忙碌线程会把消息排到下一轮。投递不占实验 CPU/GPU 配额，不阻塞 daemon 调度，也不要求提交终端或 MCP 连接继续保持。

使用 `ghs get @1` 或 MCP `get_run` 查看 `wake.target`、`wake.events`。状态含义如下：

| 状态 | 含义 |
| --- | --- |
| `pending` | 已持久化，尚未尝试投递；重启后继续处理。 |
| `sending` | 投递进程正在执行；重启后核对原进程和回执。 |
| `accepted` | T3 返回目标匹配的 JSON 回执，保存 `delivery`、`messageId`、`runId`、`status`。只表示接受消息，不表示 agent 已完成后续工作。 |
| `failed` | 确认尚未启动投递命令就失败，错误可查询。 |
| `unknown` | 命令已启动，但退出异常、回执缺失/不匹配，或进程中断；无法确定 T3 是否已接受。 |

T3 当前 CLI 不提供请求幂等键，因此 `failed`、`unknown` 都不自动重发。GHS 对每个 run 代次只登记一次事件，但不宣称外部投递 exactly-once。先检查事件目录中的 `message.txt`、`stdout.json`、`stderr.log`、`worker.log` 和 `result.json`，以及目标 T3 会话，再手动重发：

```sh
ghs retry-wake "$EVENT_ID"                   # failed
ghs retry-wake "$EVENT_ID" --allow-duplicate # unknown：已核查并接受可能重复的消息
```

MCP 对应 `retry_wake(event_id, allow_duplicate)`。每次投递有独立目录，旧日志保留。需要重试实验时使用 `retry_run(run_id)` / `ghs retry-run @1`，其下一次成功或失败生成新的事件。

## 从源码升级

更新源码后，在工程根目录按顺序执行，测试通过后再安装。

```sh
cargo test --locked &&
  make install &&
  systemctl --user daemon-reload &&
  systemctl --user restart ghs.service &&
  ghs list
```

服务重启后会从原数据目录接管运行中的实验。Codex 的注册项指向固定的安装路径，一次注册即可用于后续版本。升级后重新连接 MCP（例如重启 Codex 客户端），新启动的 `ghs mcp` 进程就会使用新版程序。

本次升级会补充 run 控制与通知表。原有本地 run 中已经失败的阶段也会触发 fail-fast，尚未运行的同 run 阶段将跳过、仍活跃的阶段将取消；历史导入目录保持只读。已有任务没有登记唤醒目标时不会补发通知。

## 验证

`cargo test --locked` 运行字节边界测试和使用临时数据库、独立 socket、真实子进程的集成测试。真实 GPU 验收需要显式选择一张空闲测试卡、Python 3 和 CUDA 驱动。测试在该卡上短时申请约 6% 显存，验证内外部进程同卡、阈值热更新、恢复、取消及队列重新分配。

```sh
GHS_TEST_GPU_UUID="GPU-实际测试设备UUID" cargo test --locked --test service live_gpu_ownership_threshold_and_recovery -- --ignored
```

常规测试使用真实子进程验证并行阶段取消、失败锁定、重试、跨重启的通知记录及投递失败。回执解析使用协议样本；测试不会用伪造的 T3 成功命令代替真实唤醒。真实 T3 验收会发送消息并可能启动 agent，需要显式提供测试线程：

```sh
GHS_TEST_T3_EXECUTABLE=/absolute/path/to/t3 \
GHS_TEST_T3_BASE_DIR=/absolute/path/to/t3-data \
GHS_TEST_T3_THREAD_ID=explicit-test-thread-id \
  cargo test --locked --test service live_t3_wake_returns_a_matching_receipt -- --ignored
```
