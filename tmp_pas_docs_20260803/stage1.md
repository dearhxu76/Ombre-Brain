# OB HN-F1 Stage -1 执行补充协议

日期：2026-08-03  
状态：`DRAFT_FOR_OWNER_COMPLETION`  
执行授权：`false`  
所属协议：`OB HN-F1 自然来源与语言优先供给协议`  
适用范围：`Stage -1 / source-and-capacity preflight only`  
语义结果读取：`0`  
模型/API 调用：`0`  
positive proposer 调用：`0`  
单轴评审调用：`0`  
CMI 调用：`0`  
产品代码修改：`false`

## 0. 一句话结论

本补充协议把 HN-F1 的下一步收窄为一个不接触正文、不做语义判断的来源与固定容量预检：先由项目所有者冻结单一 cohort、单一 provenance stratum、donor mode、许可和数据治理安排，再在受限区用匿名元数据检查固定的 P0、M1、Rtech 数量与假设性 slot 分区是否尚未被证明必然不可能。它不形成 actual roster。

当前没有任何真实来源盘点结果，不能声称已经具备 768 个节点，也不能开始语言评分、结构评分、H proposer、Spark、CMI 或产品实现。

## 1. 本补充协议回答什么

唯一问题是：

> 在不读取任何正文、向量、相似度、语言分、关系分、axis 判定或模型输出的前提下，一个事前冻结的来源 cohort 与 provenance stratum，是否能提供足够的合规、互不复用事件节点，并在匿名元数据层不排除固定批次、donor mode、源文档／时间块隔离以及 Rtech 固定数量上界的可行性？

Stage -1 只检查 Rtech 的固定数量、替补 schema 可执行性和 metadata-only 必要上界；actual Rtech 一对一 slot map 依赖 proposer 后的角色与正式 slot，只能在 S1-B 冻结。

本阶段只回答必要的来源容量可行性，不回答：

- Q–T 是否构成严格正例；
- H 是否只破坏一个轴；
- 文本是否通过 singleton 语言门；
- 自动 selector 是否准确；
- Spark 是否有帮助；
- CMI 是否改善回答；
- 任何记忆是否应被相信、拒绝、采用或转化为行动。

## 2. 权威关系与变更边界

1. `rule.md` 仍是 OB 哲学边界唯一真源。
2. `OB_HN-F1_自然来源与语言优先供给协议_2026-08-03.md` 是 HN-F1 主协议。
3. 本文件只补足主协议第 9 节步骤 1–5 与第 22 节的 Stage -1 执行字段；冲突时按 `rule.md`、主协议、本补充协议的顺序解释。
4. 本文件不改变 P0/M1/Rtech 固定规模、语言门、严格正例门、单轴门、最终 12/2 配额或停止码语义。
5. 任一语义结果可读后，不得用本补充协议追加来源、缩小批次、改变 donor mode、修改排除规则或重建 roster。

## 3. 三个必须分开的冻结对象

不能把“来源池中有 768 条记录”直接写成“正式 roster 已可执行”。必须依次冻结三个不同对象：

### 3.1 `source_universe_lock`

只包含来源、许可、同意、招募前时间、去标识、事件簇闭包、重复与 denylist 合格的匿名事件全集。此时：

- 不分配 Q/T/H；
- 不分配 assigned axis；
- 不建立 Q–T 边；
- 不读取正文或语义结果；
- 只允许使用第 6 节列出的匿名元数据。

### 3.2 `allocation_rule_lock`

在任何 semantic proposer 调用前冻结：

- P0/M1/Rtech 的确定性分区规则；
- 跨批次不复用规则；
- donor mode 与上限；
- 技术替补 slot 形状及一对一映射规则；
- exact capacity solver、版本、参数、最优性证明与 tie-break domain；
- actual roster 的不可变 commitment schema。

### 3.3 `actual_roster_commitment`

positive proposer 完成后、任何语言／结构评分可读前，才冻结实际 Q/T/H 角色名单、Q–T 单元、四轴分配、P0/M1/Rtech 批次和 Rtech 一对一替补映射。

因此 Stage -1 可以得出 `source_capacity_preflight`，但在 proposer 前不能声称 `actual_roster_complete=true`。

## 4. 固定容量账本

每个 Q–T 单元占用 1 个 Q 节点和 1 个 T 节点；固定 H 池为 Q–T 单元数的 4 倍。所有节点、事件簇、源文档和时间块跨 P0/M1/Rtech 完全不复用。

| 批次 | 固定 Q–T 单元 | Q 节点 | T 节点 | H 节点 | 唯一事件节点 |
|---|---:|---:|---:|---:|---:|
| P0 | 24 | 24 | 24 | 96 | 144 |
| M1 | 96 | 96 | 96 | 384 | 576 |
| Rtech | 8 | 8 | 8 | 32 | 48 |
| 合计 | 128 | 128 | 128 | 512 | **768** |

这里的 768 是固定 roster 所需的唯一事件节点数，不是 768 位贡献者，也不是效果样本量。上游合格来源池必须至少有 768 个候选事件节点；正式 roster 则必须恰好冻结 768 个节点。`总数 >= 768` 只是必要条件，不能替代分层、隔离和 exact matching。

同一源文档的多个 span、同一事件的多次转述、重叠时间块或被细切的同一事件不能被重复计数。事件边界争议只能合并为 super-cluster 或保守排除，不能靠更细切分补足容量。

## 5. Stage -1 的准入顺序

对每个候选事件节点按固定优先级检查：

1. 是否属于评分前冻结的唯一 cohort；
2. 是否属于评分前冻结的唯一 provenance stratum；
3. 是否明确早于招募／任务知情时点；
4. 来源是否已在冻结收集窗口内闭包；
5. 是否有可追溯原始记录；
6. 人工／系统派生类型是否符合所选 cohort；
7. 研究使用、人工评审、外部模型、聚合公开和未来复用权限是否分别有效；
8. 去标识与 PII 复核是否通过且未改变行动、否定、时序、角色或结果方向；
9. 高风险材料门是否通过或不适用；
10. 事件簇边界是否闭包；
11. 是否不是 exact/near duplicate、既往批次记录、同一事件簇或 denylist 命中；
12. 是否仍为 active，未撤回；
13. 是否为该事件簇唯一 canonical record。

任一字段为 `pending`、`unknown` 或空值时，只能标记为 `PENDING`，不能计入合格容量。排除原因使用冻结代码，不写自由文本理由。

## 6. 受限 registry 与桌面安全投影必须分开

以下逐条字段只允许存在于 ACL 受限 registry，不能进入桌面工作簿：

```text
record_id
donor_group_id
source_document_id
event_cluster_id
time_block_id
cohort_code
origin_class
provenance_grade
pre_recruitment_status
source_frozen_status
traceability_status
recorded_at_precision
human_or_system_derived
consent_status
human_review_allowed
external_model_review_allowed
public_aggregate_allowed
future_reuse_allowed
license_consent_version_code
provider_allowlist_code
language_code
source_medium
register_bin
length_bin
transformation_class
redaction_status
redaction_version_code
pii_review_status
high_risk_gate_status
cluster_boundary_status
duplicate_status
prior_batch_denylist_hit
prior_batch_reuse_status
withdrawal_status
canonical_cluster_record
```

所有受限 registry ID 必须是本研究内随机、不可逆、不可跨研究复用的不透明 ID，不能编码姓名、日期、平台、来源类型或纳入顺序。即使这些 ID 本身不透明，把 `record_id`、donor、文档、事件簇和时间块逐行关联起来仍会形成可链接图，因此不得作为桌面“安全投影”。

桌面盘点表只允许保存：

```text
protocol/amendment/workbook version
batch-level snapshot_id
单一 cohort/stratum/donor-mode 的冻结代码
政策与批准 scope 的版本和聚合状态
合格/待定/排除节点总数
唯一 donor/文档/事件簇/时间块总数
不暴露单个 donor 的最大占比等聚合量
固定容量目标与聚合余量
metadata-only exact upper-bound receipt
批级 root anchor ID
唯一 Stage -1 状态或停止码
```

桌面表不得包含任何逐条 `record_id`，也不得包含 donor、文档、事件簇、时间块之间的逐条对应关系。由受限区执行器先完成逐条校验，再经隐私复核只导出不可反推成员资格的聚合 receipt。

桌面、Git、终端和普通日志中禁止出现：

- 原始或去标识正文、标题、摘要、EvidenceSpan；
- 原始路径、URL、文件名、消息 ID；
- 姓名、邮箱、账号、联系方式、精确位置；
- 逐条 hash、可回链密钥、重识别映射；
- 自由文本备注、排除理由叙述；
- Q/T/H、assigned axis、关系标签、向量、相似度或语义分数；
- 模型输入／输出、raw API response、token usage；
- API Key、密码、token、私钥或真实配置值。

逐条撤回映射、正文 registry、文件完整性证明和实际 solver 分配只能留在 ACL 受限研究目录。

## 7. Stage -1 可读取与禁止读取的信号

### 7.1 允许读取

```text
batch
opaque_record_id
donor_group_id
source_document_id
event_cluster_id
time_block_id
cohort
provenance_stratum
consent/policy status
source_precedes_recruitment
deidentification/PII status
duplicate/denylist/reuse flags
compatibility-bin codes
rtech_slot_map metadata
```

### 7.2 禁止读取

```text
正文与标题
向量与 embedding
相似度和检索 rank
语言分与失败理由
Q–T 结构判断
assigned-axis 成败
H 相关性或单轴分
proposer 解释
任何模型输出
HN-F0/ARN 的逐项结果
```

Stage -1 的正式 receipt 必须同时声明：

```text
semantic_scores_read=0
model_calls=0
positive_proposer_calls=0
hnf1_supply_feasible=unknown
cmi_authorized=false
```


## 8. 项目所有者批准矩阵

以下项目必须在各自 checkpoint 前填写并冻结；后置执行件不是 S1-A 的容量门前置，避免在最便宜的来源可行性检查前先建完整语义系统。`UNSET`、`PENDING` 或相互冲突时，不得越过对应 checkpoint。

| 编号 | 冻结项 | 最迟 checkpoint | 当前值 |
|---|---|---|---|
| A01 | 正式 cohort code | S1-A | `PROVISIONAL_OWNER_DIRECTION: HN-F1-N` |
| A02 | 正式 origin class | S1-A | `natural_observed` |
| A03 | 单一 provenance stratum（N3 或 N2） | S1-A | `PROVISIONAL_OWNER_DIRECTION: N3` |
| A04 | donor mode | S1-A | `PROVISIONAL_OWNER_DIRECTION: cross_donor` |
| A05 | 招募截止时点／代码 | S1-A | `UNSET` |
| A06 | 来源收集窗口／闭包版本 | S1-A | `UNSET` |
| A07 | 来源纳入与排除规则版本 | S1-A | `UNSET` |
| A08 | HN-F0/ARN/ePiC/AnaloBench/既往 hard negative denylist 版本 | S1-A | `UNSET` |
| A09 | 受限研究目录标识与 ACL owner | S1-A | `UNSET` |
| A10 | 数据管理员角色组 | S1-A | `UNSET` |
| A11 | source steward／事件切分角色组 | S1-A | `UNSET` |
| A12 | 去标识执行角色组 | S1-A | `UNSET` |
| A13 | 独立 PII/span 复核角色组 | S1-A | `UNSET` |
| A14 | 事件簇手册版本 | S1-A | `UNSET` |
| A15 | exact/near duplicate 规则与阈值版本 | S1-A | `UNSET` |
| A16 | 同意政策与许可版本 | S1-A | `UNSET` |
| A17 | 人工评审使用范围 | S1-A | `UNSET` |
| A18 | 外部服务商／endpoint／模型逐项 allowlist；无授权时为 NONE | S1-A | `UNSET` |
| A19 | 外部服务训练、日志、保留期、地域与转包商约束 | S1-A | `UNSET` |
| A20 | 高风险材料规则与处理人 | S1-A | `UNSET` |
| A21 | 撤回、保留和研究副本销毁规则 | S1-A | `UNSET` |
| A22 | tokenizer 与 compatibility matrix 版本 | proposer 前 | `UNSET` |
| A23 | positive proposer 类型、instructions、预算、排序和并列规则 | proposer 前 | `UNSET` |
| A24 | H proposer 类型、instructions、预算、排序和并列规则 | proposer 前 | `UNSET` |
| A25 | 模型家族 A/B 与独立人工轨安排 | P0 前 | `UNSET` |
| A26 | 跨阶段 reviewer 隔离与 assignment | P0 前 | `UNSET` |
| A27 | 语义 schemas、builders、reconcilers、allocator、final solver 与 tie-break 版本 | proposer 前 | `UNSET` |
| A28 | metadata-only capacity solver、参数与最优性证明 | S1-A | `UNSET` |
| A29 | 审计者、reconciler 执行者、最终解码者隔离 | P0 前 | `UNSET` |
| A30 | P0/M1/Rtech 实际 roster commitment | S1-B | `UNSET: NOT_YET_AVAILABLE_BEFORE_S1-B` |
| A31 | Rtech 一对一 slot map | S1-B | `UNSET: NOT_YET_AVAILABLE_BEFORE_S1-B` |
| A32 | S1-A preflight commitment、completion anchor schema 与不可覆盖存储位置；S1-B 另建 data anchor | S1-A / S1-B | `UNSET` |

项目所有者填写本表不等于自动授权执行。开始 S1-A 前，必须先冻结完整 amendment、来源与隐私政策、metadata-only solver 配置、completion anchor schema／存储位置，并由所有者明确签署当前补充协议的 `execution_authorized=true`；这些事前对象写入 `s1a_preflight_execution_commitment`。S1-A 盘点与 solver 完成后，才原子生成 completion receipt/root anchor 并判定 `READY` 或停止码，不能要求盘点前先产生含盘点结果的 root。该授权不包含 positive proposer、S1-B、P0 或任何语义调用；这些阶段必须按主协议另行满足冻结条件并取得明确授权。

`PROVISIONAL_OWNER_DIRECTION` 只记录“按推荐方向继续准备”的可撤回方向，不等于 `APPROVED`、`FROZEN`、数据处理许可或执行授权。A02 的 `natural_observed` 是协议既有固定定义，不表示本轮新增批准。其余字段未补齐真实版本、角色、许可、实现 hash、存储位置和明确批准前，仍按 `UNSET` 处理。

## 9. donor mode 的 metadata-only 容量上界门

### 9.1 `cross_donor`

下表角色只是 metadata-only solver 为容量上界建立的假设性 slot 分区，不是 positive proposer 产生的 actual Q/T/H 角色。每批、每个假设角色的单 donor 节点上限为 `floor(0.20 × 该批该角色固定节点数)`：

| 批次/角色 | 节点数 | 单 donor 上限 | 角色边际最低 donor 数 |
|---|---:|---:|---:|
| P0 Q | 24 | 4 | 6 |
| P0 T | 24 | 4 | 6 |
| P0 H | 96 | 19 | 6 |
| M1 Q | 96 | 19 | 6 |
| M1 T | 96 | 19 | 6 |
| M1 H | 384 | 76 | 6 |
| Rtech Q | 8 | 1 | 8 |
| Rtech T | 8 | 1 | 8 |
| Rtech H | 32 | 6 | 6 |

边际计数足够仍不代表三元组可匹配。每条 Q/T/H 的 donor 必须三者互异，且在最终匹配大小为 `M` 时，每位 donor 最多出现在 `floor(0.20 × M)` 个选中三元组中。因此 donor 数 `D` 必须满足：

```text
D × floor(0.20 × M) >= 3M
```

这一区分产生两个不同口径，不能混写：

- **数学硬下限**：因为 `floor(0.20 × M) <= 0.20M`，P0 与 M1 均必有 `D >= 15`；15 位 donor 分别可在 `M=10`、`M=15` 时达到计数等式。Rtech 因 Q/T 单角色上限为 1，仍至少需要 8 位 donor。因此 contributor 不跨 P0/M1/Rtech 复用时，跨批次硬下限是 `15 + 15 + 8 = 38` 位 donor；少于 38 位必然不可行。
- **保守规划阈值**：若要求方案在最低通过匹配数本身就可行，P0 的 `M=6` 要 18 位 donor，M1 的 `M=12` 要 18 位 donor，Rtech 要 8 位，合计 44 位。该阈值避免依赖额外有效匹配把 `floor(0.20 × M)` 从 1/2 提升到 2/3，但它不是当前协议的数学必要条件。

```text
planning_donor_floor_p0=18
planning_donor_floor_m1=18
planning_donor_floor_rtech=8
planning_donor_floor_total=44
planning_floor_status=PROPOSED_UNAPPROVED
```

因此 38–43 位不能在普查阶段被硬判为容量失败，只能视为高脆弱区；达到 44 位也仍不充分。S1-A 的匿名元数据 exact upper-bound solver 只能继续排除明显不可能的来源分配，不能证明语义 proposer 后的实际角色或匹配数。S1-B 的 actual roster 与 Rtech map 验证也不能单独证明语言／结构门后的最终 `G` 与 `M*`；38–43 位的实际可行性最终只能由后续 P0/M1 语义图上的相应 exact matching 结果确认。

### 9.2 `within_donor_ecological`

每个 Q/T/H 三元组来自同一 donor，但三个事件必须来自不同源文档、不同事件簇和互不重叠时间块。Stage -1 尚无语义 Q–T pairing，因此设 donor `d` 在批次 `b` 中仅按匿名元数据、忽略语义可形成的潜在 Q–T slot 乐观上界为 `U_UB(b,d)`，可用 H 节点上界为 `H_UB(b,d)`：

```text
UB_metadata(b) = Σ_d min(U_UB(b,d), H_UB(b,d))
```

至少需要 `UB_metadata(P0) >= 6`、`UB_metadata(M1) >= 12`，且 Rtech 固定数量上界不被元数据约束排除。该模式理论上可以由少数 donor 提供大量不同事件，因此必须报告 donor 数和每位 donor 的聚合局部上界，不能把事件数冒充独立人数，也不能把乐观上界写成 actual Q–T pairability 或跨来源供给可行。

最终最低配额的 `6/12` 上界不能代替固定批次容量门。metadata-only solver 还必须证明，在完全忽略类比语义的乐观假设下，来源元数据至少可以容纳 P0 `24 Q / 24 T / 96 H`、M1 `96 Q / 96 T / 384 H`、Rtech `8 Q / 8 T / 32 H` 的假设性 slot 数量，并满足批次、donor、文档、事件簇和时间块约束。任一固定角色数量上界不足即停止；全部满足仍只表示“未被元数据排除”。

### 9.3 metadata-only exact upper-bound receipt

桌面只保存不含具体分配的聚合 receipt：

```text
solver_run_id
snapshot_id
solver_version
solver_status=exact|error
donor_mode
metadata_only_p0_upper_bound_feasible=yes|no
metadata_only_m1_upper_bound_feasible=yes|no
metadata_only_rtech_upper_bound_feasible=yes|no
metadata_only_full_disjoint_upper_bound_feasible=yes|no
optimality_proved=yes|no
```

这些字段只表示在不读取语义、尚未运行 positive proposer 时，匿名来源元数据没有证明固定设计必然不可能；它们不能证明实际 Q–T pairability、Q/T/H 角色 roster、四轴分配或 Rtech 一对一 slot map 已经存在。solver error、超时、非精确结果或无法证明最优性属于技术停止，不能伪装成来源容量失败。

## 10. Rtech 的 Stage -1 上界与下游承诺边界

Rtech 只处理评分前客观技术损坏、评分前撤回、评分前发现的 duplicate 或 manifest/schema 无效。它不能修复语言差、结构失败、unknown、单轴失败、匹配不足或结果不理想。

S1-A 只冻结 `8 qt_unit / 32 h_node` 的目标数量、替补 schema、允许原因集合，以及 donor／文档／事件簇／时间块约束下的 metadata-only 乐观上界。它不创建 formal slot，不运行 proposer，也不生成 actual 一对一 map。

未来在 S1-B、任何 P0 语义结果可读前，必须恰好冻结：

- 8 个 `qt_unit` reserve commitment；
- 32 个 `h_node` reserve commitment；
- 每个 reserve 对唯一 `replaces_formal_slot_id`；
- 同角色、同 provenance stratum、同 compatibility key 与相同 donor mode；
- 不可变的允许激活原因集合；
- 一对一、不复用、不临场改配的 slot map。

实际启用必须另写不可覆盖 activation receipt，且 `any_semantic_result_read=false`。语义结果可读后永不替补。

## 11. 冻结顺序与检查点

### Checkpoint S1-A：来源全集就绪

1. 项目所有者完成 A01–A21、A28 和 A32 的 S1-A 部分；
2. 受限区完成来源闭包、预切分、去标识、双人复核、重复审计和 registry；
3. 生成 `source_universe_lock`；
4. 桌面安全投影只输出聚合容量、状态计数和批级 receipt；
5. 若不足或无效，按第 12 节停止，不运行 proposer。

S1-A 是本补充协议当前 Stage -1 的终点。通过后最多允许进入规则冻结与 positive proposer 准备，不能声明实际 roster 已完成，也不能启封 P0。

### Checkpoint S1-B：actual roster 就绪（下游交接，不属于当前 Stage -1 执行范围）

1. 所有机器 schema、builder、solver、proposer 和预算已冻结；
2. positive proposer 只在合格、尚未分角色的既有事件簇池中运行；
3. 冻结恰好 24/96/8 个 Q–T 单元和 96/384/32 个 H 节点；
4. 冻结四轴、blind IDs、actual roster 和 Rtech slot map；
5. 验证 768 个节点、事件簇、源文档和时间块跨批完全不复用；
6. 写入 root anchor；
7. 在读取任何语言评分前再次确认 `semantic_scores_read=0`。

本补充协议只规定 S1-B 必须如何与 S1-A 衔接，不授权执行 S1-B。未来只有 S1-B 通过且项目所有者另行明确执行授权，才允许按主协议启封 P0 singleton 语言门。

## 12. 唯一状态与停止码

| 状态／停止码 | 触发条件 | 含义 |
|---|---|---|
| `STAGE1_NOT_READY_METADATA_INCOMPLETE` | 盘点字段、批准或冻结项尚未完成 | 准备状态，不是实验失败 |
| `STOP_HNF1_PROTOCOL_NOT_EXECUTABLE` | 尝试开始语义阶段时，A01–A32 必填项仍未冻结 | 协议不可执行 |
| `STOP_HNF1_SOURCE_OR_CONSENT_INVALID` | 来源、许可、同意或招募前时间依据系统性无效 | 来源治理停止 |
| `STOP_HNF1_SOURCE_CAPACITY_LT_FIXED_BATCH` | S1-A 的 metadata-only exact 上界已证明固定 768／批次隔离必不可行；或未来 S1-B 无法形成 actual roster／Rtech map | 固定来源容量停止 |
| `STOP_HNF1_TECHNICAL_EXECUTION_FAILURE` | solver/builder/schema 无法产生 exact、唯一、可验证 receipt | 本轮无结论，不是供给失败 |
| `INVALID_HNF1_BLINDING_BREACH` | role、axis、source role、position key、上游结果或私有映射泄漏给不应看到它的评审者 | 整轮失效 |
| `INVALID_HNF1_RESULT_DEPENDENT_PROTOCOL_CHANGE` | 读取结果后改来源、规模、规则、proposer、solver 或替补 | 整轮失效 |
| `READY_HNF1_STAGE1_SOURCE_CAPACITY_ONLY` | S1-A 的来源、许可、固定规模必要容量与匿名元数据 exact preflight 全部满足 | 只表示来源容量准备完成；actual roster 仍未知，不是 HN-F1 通过 |

`READY_HNF1_STAGE1_SOURCE_CAPACITY_ONLY` 必须和以下字段共同出现：

```text
semantic_scores_read=0
model_calls=0
positive_proposer_calls=0
actual_roster_complete=unknown
hnf1_supply_feasible=unknown
separate_cmi_protocol_may_be_drafted=false
cmi_authorized=false
```

未获授权的正文暴露属于隐私／数据安全事件，必须立即停止处理并按评分前冻结的 incident procedure 处置；只有同时把 role、axis、source role、position key、上游结果或其他盲法信息泄漏给不应看到它的评审者时，才使用 `INVALID_HNF1_BLINDING_BREACH`。若事件使来源、许可或同意依据无效，则按最早阶段触发 `STOP_HNF1_SOURCE_OR_CONSENT_INVALID`，不能用盲法停止码掩盖隐私事件。

## 13. 规模调整规则

本协议内固定的 24/96/8 Q–T 与 96/384/32 H 不允许因盘点结果不足而缩小，也不能通过把每个 T 的冻结候选提交上限从 8 提高到 9、挪用 Rtech、跨 cohort 合并或放宽 donor 规则救门。

只有同时满足以下条件，才可以在新协议版本中调整规模：

- 来源全集尚未冻结，仍处于事前指定的收集窗口与闭包规则内；
- 尚未读取 proposer、语言、结构、axis 或其他语义输出；
- 新版本重新冻结规模、阈值、预算、停止规则和 root anchor。

如果调整动机来自本轮已观察到的容量结果，本轮必须先以固定门停止。后续只能作为明确标记的 development protocol；若要保留确认性结论，应使用新的独立来源窗口／批次。任何 P0 结果可读后修改规则，都必须使用全新的 P0/M1/Rtech。

## 14. 无正文盘点工作簿规范

正式工作簿至少包含以下 sheet：

1. `00_CONTROL`：协议、快照、cohort、stratum、donor mode、政策版本和固定容量；
2. `01_APPROVALS`：批准 scope、角色组、决定和政策版本；
3. `02_AGGREGATE_COUNTS`：合格／待定／排除总数、唯一 donor／文档／事件簇／时间块总数和固定排除码聚合；
4. `03_CAPACITY_GATE`：固定目标、聚合余量、metadata-only upper-bound receipt 和唯一状态；
5. `04_CODEBOOK`：枚举、排除码优先级、ID 规则与门槛常量。

工作簿不得设置逐条 inventory sheet，也不得出现 record/donor/document/event/time-block 的逐行关联。它还不得包含宏、隐藏 sheet、隐藏行列、图片、批注、超链接、外部连接、Power Query、pivot cache、原始路径或任何正文。冻结后不得覆盖保存；每次变更生成新的 `snapshot_id/workbook_version`，旧版本标记为 `SUPERSEDED`。

工作簿只能辅助审核安全投影，不能承担正文 registry、撤回映射或实际 solver 分配。工作簿显示 `READY` 仍不等于项目所有者批准执行。

## 15. S1-A 与 S1-B 必须使用不同 root anchor

### 15.0 S1-A preflight execution commitment

开始受限盘点前先生成不含任何观察容量结果的 commitment：

```text
protocol_hash
amendment_hash
approval_snapshot_hash
policy_bundle_hash
denylist_hash
metadata_only_exact_solver_binary_hash
metadata_only_exact_solver_config_hash
completion_anchor_schema_hash
immutable_storage_location_code
execution_authorized=true
semantic_scores_read=false
model_calls=0
positive_proposer_calls=0
```

该 commitment 只授权 S1-A 的受限来源盘点和 metadata-only solver，不表示 source universe、registry snapshot 或容量 receipt 已经存在。

### 15.1 S1-A source-capacity completion anchor

盘点与 solver 完成后才原子生成当前 Stage -1 的 completion anchor，至少承诺：

```text
protocol_hash
amendment_hash
s1a_preflight_execution_commitment_hash
source_universe_lock_hash
control_snapshot_id
restricted_registry_snapshot_id
approval_snapshot_hash
policy_bundle_hash
denylist_hash
metadata_only_exact_solver_binary_hash
metadata_only_exact_solver_config_hash
metadata_only_upper_bound_receipt_hash
created_at
semantic_scores_read=false
model_calls=0
positive_proposer_calls=0
actual_roster_commitment_hash=NOT_YET_CREATED
rtech_slot_map_hash=NOT_YET_CREATED
```

completion anchor 发布前不得判定 `READY` 或正式停止码。桌面安全投影只能保存批级 root anchor ID 和允许公开的聚合状态，不保存逐条 hash 或可回链路径。

### 15.2 S1-B data anchor（下游交接）

未来在另行授权的 positive proposer 完成后、任何语言评分可读前，必须新建 S1-B data anchor，至少增加：

```text
parent_s1a_anchor_hash
allocation_rule_lock_hash
positive_proposer_commitment_hash
positive_proposer_receipt_hash
actual_roster_commitment_hash
rtech_slot_map_hash
axis_assignment_commitment_hash
blind_id_commitment_hash
semantic_scores_read=false
```

不得回写或覆盖 S1-A anchor，也不得用 `NOT_YET_CREATED` 占位值声称 S1-B 已完成。

## 16. 当前实际状态与下一步

当前实况：

- A01、A03、A04 已记录为拟推进方向：`HN-F1-N / N3 / cross_donor`；它们尚未成为 owner approval；A02 的 `natural_observed` 是协议固定定义；
- A05–A06、A09–A13、A15–A19、A21 仍缺真实治理信息；A22–A27、A29 属下游执行件，A30–A31 尚不能产生；
- A07–A08、A14、A20、A28、A32 仍为 `UNSET`；协议中的规则或 schema 不能代替实际版本、责任人、solver 实现和不可覆盖存储位置；
- `effective_external_allowlist=NONE_UNTIL_EXPLICITLY_APPROVED`，因此当前禁止任何外部模型或服务商调用；
- 没有导入、读取或统计任何真实来源；
- 没有接触真实 OB vault 或研究副本；
- 没有运行模型、proposer、语言门、结构门、单轴门或 CMI；
- 768 是固定设计需求，不是已观察容量；
- `execution_authorized=false`。

下一步不是收集正文，也不是调用模型，而是按第 20 节补齐 `HN-F1-PAS` 的现实管理员信息、实现与 hash，再由项目所有者针对最终 commitment 逐项审批。只有 PAS 获得 hash-bound 授权、完成普查并输出 `PAS_PLAUSIBLE_NOT_VERIFIED` 后，才值得继续补齐第 18 节的 A01–A21、A28、A32；随后数据管理员才可在 ACL 受限目录内建立 S1-A 只读 registry。只有 S1-A 通过后，才允许冻结 A22–A27、A29 并准备另行授权的 proposer/S1-B。

## 17. 项目所有者批准栏

```text
provisional_direction_status=RECORDED_NOT_FROZEN
provisional_direction_recorded_at=2026-08-03
provisional_direction_cohort_code=HN-F1-N
protocol_fixed_origin_class=natural_observed
provisional_direction_provenance_stratum=N3
provisional_direction_donor_mode=cross_donor
effective_external_allowlist=NONE_UNTIL_EXPLICITLY_APPROVED
owner_decision=PENDING
approved_protocol_version=UNSET
approved_amendment_hash=UNSET
approved_cohort_code=UNSET
approved_provenance_stratum=UNSET
approved_donor_mode=UNSET
execution_authorized=false
approved_at=UNSET
```

未明确把 `owner_decision` 改为 `APPROVED` 且把 `execution_authorized` 改为 `true` 前，本文件始终只是执行准备草案。

## 18. S1-A 剩余阻断清单

### 18.1 已记录但未冻结

- A01 拟推进方向：`HN-F1-N`；
- A02 协议固定定义：`natural_observed`；
- A03 拟推进方向：`N3`；
- A04 拟推进方向：`cross_donor`；
- 固定容量：P0 144、M1 576、Rtech 48，共 768 个事件节点；
- `cross_donor` 数学硬下界：P0 15、M1 15、Rtech 8，批次隔离后至少 38 位；
- 待批准的保守规划阈值：P0 18、M1 18、Rtech 8，共 44 位；它不是数学必要条件。

这些方向若在真实来源盘点前被项目所有者明确批准，可以进入正式 freeze。PAS 只有在第 19.6 节列出的合法乐观上界违反 donor 总数、事件总数、批次互斥分区或可合法计数条件时，才可作当前框架的容量 no-go；38–43 位属于高脆弱且未决，达到 44 位也只达到拟议的保守规划阈值。任何结果都不能被用来观察容量后静默切换为 N2、`within_donor_ecological` 或缩小批次。

### 18.2 可预拟但尚未批准的规范版本

以下值只作为 `PROPOSED_UNAPPROVED` 供项目所有者审阅，不改变第 8 节矩阵中的 `UNSET`：

```text
A07_proposed=HN-F1-S1A-SOURCE-ELIGIBILITY-v0.1-DRAFT
A08_proposed=HN-F1-S1A-DENYLIST-POLICY-v0.1-DRAFT
A14_proposed=HN-F1-S1A-EVENT-CLUSTER-MANUAL-v0.1-DRAFT
A18_proposed=NONE
A19_proposed=NO_EXTERNAL_TRANSFER_WHILE_A18_NONE
A20_proposed=HN-F1-S1A-HIGH-RISK-EXCLUDE-ALL-v0.1-DRAFT
A28_proposed=HN-F1-S1A-METADATA-UB-SOLVER-SPEC-v0.1-DRAFT
A32_proposed=HN-F1-S1A-ANCHOR-SCHEMA-v0.1-DRAFT
proposal_status=PROPOSED_UNAPPROVED
```

A07/A08/A14 只把主协议既有来源、denylist 与事件簇规则版本化；实际 cutoff、来源全集、受限 denylist root 和执行角色仍不可代填。A20 的安全建议是所有高风险材料整条排除且禁止外发；若要纳入任何高风险类别，必须另行列明类别、许可、贡献者同意、处理人、双人 PII 复核、ACL、保留／撤回和外发范围。A28 只是 solver contract，不存在可执行二进制、配置 hash 或最优性 receipt。A32 只是 anchor schema，桌面或 Git 不能被冒充为不可覆盖存储。

### 18.3 必须由项目所有者或数据治理负责人提供的事实

1. A05：明确招募截止时点及其批级代码；
2. A06：来源收集窗口、全集闭包时点和确定性截断顺序；
3. A09：ACL 受限、非 Git、非桌面同步的研究目录标识与 owner；
4. A10–A13：数据管理员、source steward、去标识者、独立 PII/span 复核者的角色组与隔离关系；
5. A15：exact/near duplicate 规则、阈值、tokenizer 和版本；
6. A16–A19：同意政策、人工评审范围、逐服务商／endpoint／模型 allowlist，以及训练、日志、保留期、地域与转包商限制；
7. A20：高风险材料规则与处理人；安全建议是 `EXCLUDE_ALL_HIGH_RISK`，但在项目所有者明确批准前不能写成正式默认；
8. A21：撤回、保留期限、研究副本销毁和 tombstone 规则；
9. A28：metadata-only solver 的实际实现、版本、二进制 hash、参数、exact 状态与最优性证明；
10. A32：preflight commitment 与 completion anchor 的不可覆盖存储位置。

### 18.4 当前不能生成的对象

在第 18.3 节未补齐并得到明确授权前，不生成：

- `s1a_preflight_execution_commitment`；
- `source_universe_lock`；
- 受限来源 registry；
- metadata-only solver receipt；
- S1-A completion anchor；
- actual roster、Rtech map、blind pack 或任何模型输出。

当前唯一合法状态是：

```text
stage1_status=STAGE1_NOT_READY_METADATA_INCOMPLETE
hnf1_pas_status=PAS_NOT_AUTHORIZED
provisional_direction_status=RECORDED_NOT_FROZEN
effective_external_allowlist=NONE_UNTIL_EXPLICITLY_APPROVED
semantic_scores_read=0
model_calls=0
positive_proposer_calls=0
actual_roster_complete=unknown
hnf1_supply_feasible=unknown
cmi_authorized=false
```

## 19. `HN-F1-PAS` 无正文来源可得性普查（未执行）

- 全称：`Pre-S1-A Source Availability Screen`
- 状态：`PAS_NOT_AUTHORIZED`
- 执行授权：`false`
- 接触正文：`0`
- 模型/API 调用：`0`
与历史 HN-F0 的关系：`none`；PAS 不是 HN-F0 的续跑、重命名或结果补丁。

### 19.1 唯一目的与证据等级

PAS 只回答一个规划问题：在投入完整 S1-A 治理、registry 和 exact solver 之前，ACL 受限、假名化且不接触正文的来源自报元数据，是否足以排除“当前 `HN-F1-N / N3 / cross_donor` 固定设计显然不可供给”，以及是否达到值得继续准备 S1-A 的保守门槛。

PAS 是非实验、非正式 Stage、非验证性普查；所有数量均标记为 `SELF_REPORTED_UNVERIFIED`。它不能证明来源合格、S1-A 通过、HN-F1 可行或 Spark 有效。PAS 的“唯一源文档／独立事件／非重叠时间块数”只是 donor 对招募前既有材料给出的区间，不是经正文核查、事件簇去重、PII、许可和 exact solver 后的合格节点。

### 19.2 与 S1-A 的边界

- PAS 位于 S1-A 之前，不生成 `source_universe_lock`、正式 registry、Q/T/H 角色分配或 Rtech map；
- PAS 不使用 `STOP_HNF1_*`、`INVALID_HNF1_*` 或 `READY_HNF1_STAGE1_SOURCE_CAPACITY_ONLY`；
- `PAS_PLAUSIBLE_NOT_VERIFIED` 最多允许项目所有者继续补齐 A01–A21、A28、A32，不能自动授权 S1-A；
- `PAS_NO_GO_CURRENT_FRAME` 只停止当前冻结招募框架的准备，不是 HN-F1 正式来源容量停止；
- PAS 结果不能用于缩小 24/96/8、把每个 T 的候选上限从 8 改为 9、混合 N2、切换 donor mode，或追加来源后继续声称同一 confirmatory 协议。

### 19.3 启动 PAS 前必须冻结的最小项

在联系第一位候选 donor 前，必须由项目所有者明确批准一个只覆盖 PAS 的 execution commitment。以下代码块只是 `PROPOSED_UNAPPROVED_TEMPLATE`，其中的“批准后要求值”不代表当前已获授权：

```text
template_status=PROPOSED_UNAPPROVED
PAS01_direction=HN-F1-N|N3|cross_donor
PAS02_recruitment_frame_cutoff_and_one_person_one_donor_dedup_rule=<frozen;version>
PAS03_screen_window_and_closure_rule=<frozen>
PAS04_restricted_directory_acl_and_storage_topology=<frozen;non_git;non_desktop;non_auto_sync>
PAS05_plain_language_notice_and_metadata_consent_version=<frozen>
PAS06_data_admin_and_access_roles=<frozen>
PAS07_retention_withdrawal_and_incident_rule=<frozen>
PAS08_high_risk_inclusion_and_nonsensitive_disposition_enum=<false;frozen>
PAS09_external_form_or_provider_allowlist=NONE
PAS10_count_band_codebook_finite_bounds_and_unknown_nonresponse_rule=<frozen;version;hash>
PAS11_planning_donor_floor=P0:18|M1:18|Rtech:8|total:44
PAS11_basis=CONSERVATIVE_MINIMUM_CARDINALITY_ROBUSTNESS
PAS11_planning_floor_status_required_after_owner_approval=APPROVED
PAS12_partition_checker_version_hash_tie_break_and_error_rule=<frozen>
PAS13_aggregate_release_and_small_cell_rule=<frozen>
PAS14_commitment_id_hash_immutable_location_approved_at_and_by=<frozen>
pas_execution_authorized_required_after_owner_approval=true
```

该 commitment 不能沿用当前补充协议中的 `execution_authorized=false` 作为默认许可，也不能授权收正文、去标识正文、调用模型或执行 S1-A。招募框架、截止时间和候选来源必须在第一位 donor 得知具体容量目标前冻结；此后新写、补写或为任务整理的材料不得计入招募前区间。

### 19.4 受限区逐 donor 最小字段

PAS 只在 ACL 受限区保存枚举值与预先冻结的计数档位，不设自由文本字段：

```text
pas_snapshot_id
screen_donor_id
screen_response_status=complete|pending|incomplete|declined|withdrawn
willing_to_participate=yes|no|pending
pre_recruitment_material_possible=yes|no|unknown
external_timestamp_evidence_possible=yes|no|unknown
human_primary_authorship_possible=yes|no|unknown
traceable_original_possible=yes|no|unknown
non_high_risk_possible=yes|no|unknown
human_internal_review_permission_intent=yes|no|pending
deidentification_permission_intent=yes|no|pending
recontact_permission=yes|no|pending
count_response_status=none|partial|complete
eligible_intersection_unique_source_document_count_lower_band=<frozen enum|unknown>
eligible_intersection_unique_source_document_count_upper_band=<frozen enum|unknown>
eligible_intersection_distinct_event_count_lower_band=<frozen enum|unknown>
eligible_intersection_distinct_event_count_upper_band=<frozen enum|unknown>
eligible_intersection_nonoverlap_time_block_count_lower_band=<frozen enum|unknown>
eligible_intersection_nonoverlap_time_block_count_upper_band=<frozen enum|unknown>
eligible_joint_unique_node_count_lower_band=<frozen enum|unknown>
eligible_joint_unique_node_count_upper_band=<frozen enum|unknown>
responded_before_cutoff=yes|no
withdrawal_status=active|withdrawn
primary_screen_disposition_code=<frozen enum|none>
```

`count_response_status` 只描述上述八个计数档位字段的作答完整度，不表示整个问卷是否响应，也不是资格、同意或授权字段。`none` 要求八个档位 token 全部为 `UNKNOWN`；`partial` 要求至少一个 `UNKNOWN` 与至少一个非 `UNKNOWN`；`complete` 表示计数问题已逐项作答，但允许显式回答 `UNKNOWN`，因此不等于有效上下界已经消解。`screen_response_status=pending` 时只能为 `none`，`screen_response_status=complete` 时只能为 `complete`。

`screen_donor_id` 与 `pas_snapshot_id` 必须是研究内不透明随机 ID，不编码来源平台、身份或敏感属性，也不能跨研究复用。身份—ID 映射只能由独立招募台账保存，与 PAS 回答表分离并采用更窄 ACL，以便依法完成撤回、再联系和“一人一 donor”机械去重；身份键、重复项映射及去重理由不得导出到 PAS 回答表或桌面。`eligible_intersection_*` 只能计 donor 自报为同时满足“招募前既有、外部时间证据可得、人类主要创作、原始来源可追溯、非高风险且原则上允许内部人工研究／去标识”的独立事件、唯一源文档和非重叠时间块；只满足其中一部分的普通材料不能进入保守下界或乐观合格上界。数据管理员不得阅读正文来“帮忙估算”，也不得把同一文档的消息数、摘要、副本或同一事件的多次转述直接当作独立事件。

`eligible_joint_unique_node_count_*` 是 PAS 判断容量的主计数字段。每一个自报计数单元必须同时绑定一个独立事件、一个未在该 donor 其他计数单元复用的源文档、一个互不重叠时间块，并同时满足全部资格交集。三个 `eligible_intersection_*` 边际字段只作诊断；即使它们分别很大，也不能替代联合节点数。

PAS10 必须分别冻结“自报原始数量”和“协议有效容量”的解释。PAS 不需要猜测原始数量的有限最大值；粗分区中任一 donor 在任一批次最多只能贡献 `max(27,114,8)=114` 个节点，因此可以事前定义 `C_eff=min(C_raw,114)`。最高档 `114+` 的原始上界仍是 unknown，但协议有效上下界均为 114；完全未知或未响应的原始数量映射为有效区间 `[0,114]`。这个 114 只是选择规则推出的容量上限，不能写成 donor 实际拥有 114 个材料。只有在 PAS02 已冻结有限且可穷尽的候选 frame、PAS10 的映射已批准且每个 frame member 恰好出现一次时，才能用该有效上界判断 hard no-go。

`primary_screen_disposition_code` 只能来自宽泛、非敏感的流程枚举 `withdrawn|duplicate|late|declined|explicit_no|incomplete|none`；不得编码健康状况、性、未成年人身份或其他高风险类别。互斥优先级冻结为 `withdrawn > duplicate > late > declined > explicit_no > incomplete > none`：`late/duplicate` 只写在该字段；窗口关闭时仍为 `pending` 的回答先机械改为 `incomplete`。未知枚举或不符合该映射的组合属于输入无效。前五种是明确排除，`incomplete` 只是证据未完成，不把乐观上界置零。

### 19.5 PAS 中禁止收集

- 原始或去标识正文、标题、摘要、截图、附件；
- 文件路径、URL、消息 ID、逐条时间戳或逐条 hash；
- PAS 回答表中的姓名、邮箱、账号、联系方式或精确位置；
- Q/T/H、axis、类比候选、故事关系或任务 prompt；
- 模型输入／输出、embedding、相似度或任何语义分；
- API Key、密码、token、私钥或真实配置值。

联系信息若招募流程确实需要，必须由独立招募台账保存，不能与 PAS 回答同表，也不能进入桌面聚合投影。默认不使用外部表单、云端处理或模型；若确有需要，必须修改 PAS09、完成数据流与服务商审查并重新取得明确授权。

### 19.6 硬 no-go、脆弱区与保守可继续区

先定义两个界：

- `conservative lower bound`：只计所有资格枚举均为 `yes`、`screen_response_status=complete` 与 `count_response_status=complete` 同时成立、在截止前完整回答且未撤回的 donor，并使用各字段下界档位冻结下端点；其他未明确排除情形的 `LB=0`。用于 donor 人数门时，该 donor 的联合节点下界还必须至少为 1；
- `optimistic upper bound`：资格枚举的 unknown 可以按 `yes` 计为潜在可行；数量 unknown 或未响应者按 PAS10 的协议有效区间 `[0,114]` 纳入上界，但不能据此声称其原始数量已知。用于 donor 人数门时，联合节点有效上界还必须至少为 1。不得计入已拒绝、已撤回、重复身份、截止后回答或明确不合格者。招募 frame 不有限、不穷尽或成员数不确定时，仍会阻止 hard no-go 结论。

只有窗口按冻结规则关闭、有限招募框架已穷尽、身份去重完成，且所有未决项已经消解或按 PAS10 的事前协议有效上界纳入乐观场景，才能输出 `PAS_NO_GO_CURRENT_FRAME`。以下任一条件足以构成当前框架的硬 no-go：

PAS12 的粗分区 checker 必须先按 donor 和假设批次计算可贡献节点界：

```text
batch_cap(P0)=27
batch_cap(M1)=114
batch_cap(Rtech)=8
node_bound(b,d)=min(joint_unique_node_bound(d), event_bound(d), unique_document_bound(d), nonoverlap_time_block_bound(d), batch_cap(b))
```

乐观 no-go 使用联合节点、三个诊断边际和 batch cap 的 upper bound；保守可继续使用对应 lower bound。被计入某批 donor 人数的 `d` 必须在 checker 中实际分配至少 1 个 `node_bound(b,d)`，零容量 donor 不得虚增 15/15/8 或 18/18/8。

1. 乐观合格 donor 上界少于 38；
2. 乐观联合唯一节点上界少于 768，或独立事件、唯一源文档、非重叠时间块三个诊断边际上界中任一种合计少于 768；
3. 即使忽略语义与实际去重损耗，也无法把 donor 不重叠地分为：P0 至少 15 位且 `Σ node_UB(P0,d) >= 144`、M1 至少 15 位且 `Σ node_UB(M1,d) >= 576`、Rtech 至少 8 位且 `Σ node_UB(Rtech,d) >= 48`；
4. 只有重复计入跨批次 donor、违反招募截止或使用不可合法处理的数据，才能达到上述乐观上界。

第 3 项只允许使用 PAS12 冻结的确定性枚举或 exact integer feasibility checker；checker error、超时、非确定、招募 frame 不可穷尽、有效上下界规则未冻结，或不能证明 no-go 时，状态必须为 `PAS_INCONCLUSIVE`。这些批级 cap 只排除明显的 donor 集中问题，PAS 仍未检查实际角色分布、文档／事件簇／时间块去重结果、Q–T 语义配对或三元组可匹配性。

38–43 位处于**高脆弱区**：它在数学上可能通过，但 P0 和／或 M1（取决于实际批次 donor 分配）必须获得足够高的最终匹配数才能放宽 donor cap，PAS 无正文数据无法证明这一点，因此不能判 no-go，也不能判保守可继续。

只有同时满足以下条件，才可输出 `PAS_PLAUSIBLE_NOT_VERIFIED`：

1. 保守 donor 下界至少 44；
2. 保守联合唯一节点下界至少 768，且独立事件、唯一源文档和非重叠时间块三个诊断边际下界也分别合计至少 768；
3. 保守、不重叠的粗分区可达到 P0 至少 18 位且 `Σ node_LB(P0,d) >= 144`、M1 至少 18 位且 `Σ node_LB(M1,d) >= 576`、Rtech 至少 8 位且 `Σ node_LB(Rtech,d) >= 48`；
4. 许可、ACL、撤回、招募截止和 S1-A 后续核验路径均存在。

这里的 44 是第 9.1 节拟议的 `PROPOSED_UNAPPROVED` 保守规划阈值，不是数学必要条件；采用它作为 PAS 的“可继续”门必须通过 PAS11 显式批准，并与 PAS01–PAS14 一起冻结。若乐观上界未触发硬 no-go，但保守下界未达到上述门槛，统一输出 `PAS_INCONCLUSIVE`。

### 19.7 PAS 状态机

| 状态 | 精确含义 |
|---|---|
| `PAS_NOT_AUTHORIZED` | PAS01–PAS14 未全部冻结或尚未获准联系 donor；这是当前默认状态 |
| `PAS_IN_PROGRESS` | 已获授权且窗口开放；尚不得作最终容量判断 |
| `PAS_INCONCLUSIVE` | 窗口已关闭但响应／证据仍不足、上下界跨门槛、处于 38–43 位脆弱区，或 checker 无法确定结论 |
| `PAS_NO_GO_CURRENT_FRAME` | 冻结且穷尽的当前招募框架在最乐观合法上界下仍违反第 19.6 节硬必要条件 |
| `PAS_PLAUSIBLE_NOT_VERIFIED` | 保守下界达到经批准的规划门；只表示值得继续准备 S1-A |
| `PAS_VOID_SCOPE_OR_PRIVACY_BREACH` | 发生越界收正文、身份混表、未授权外发、截止后扩样或其他使 PAS 证据不可用的事件 |

PAS 永远不输出 `PASS`、`READY`、任何 `STOP_HNF1_*`、`INVALID_HNF1_*`、`hnf1_supply_feasible=true` 或 `cmi_authorized=true`。

合法迁移只有：`PAS_NOT_AUTHORIZED -> PAS_IN_PROGRESS -> {PAS_INCONCLUSIVE | PAS_NO_GO_CURRENT_FRAME | PAS_PLAUSIBLE_NOT_VERIFIED | PAS_VOID_SCOPE_OR_PRIVACY_BREACH}`；越界或隐私事件可从任何状态进入 `PAS_VOID_SCOPE_OR_PRIVACY_BREACH`。窗口开放时只能是 `PAS_IN_PROGRESS`。同一 `pas_snapshot_id` 的关闭状态不可回退或覆盖；若要补响应、扩招、改规则或重开窗口，必须新建 PAS 协议版本与 snapshot。若终态后才发现隐私事件，只能以 append-only 记录将该 snapshot 标记为 VOID，不得删除原状态记录。

### 19.8 桌面只允许聚合 receipt

经独立隐私复核和小单元抑制后，桌面最多保存：

```text
pas_snapshot_id
pas_execution_commitment_id
pas_execution_commitment_hash
receipt_generated_at
recruitment_frame_code
count_band_codebook_hash
screen_window_closed=yes|no
recruitment_frame_exhausted=yes|no
complete_response_count
pending_or_unknown_count
declined_or_withdrawn_count
nonresponse_count
unique_contacted_donor_count
duplicate_response_excluded_count=<nonnegative integer|SUPPRESSED>
effective_upper_bounds_resolved=yes|no
raw_count_unknown_or_114_plus_count=<nonnegative integer|SUPPRESSED>
confirmed_possible_donor_count
maximum_possible_donor_count
conservative_event_lower_bound_total
optimistic_event_upper_bound_total
conservative_unique_document_lower_bound_total
optimistic_unique_document_upper_bound_total
conservative_nonoverlap_time_block_lower_bound_total
optimistic_nonoverlap_time_block_upper_bound_total
conservative_joint_unique_node_lower_bound_total
optimistic_joint_unique_node_upper_bound_total
conservative_p0_m1_rtech_partition_summary
optimistic_p0_m1_rtech_partition_summary
donor_partition_disjoint=yes|no|unknown
partition_checker_version_and_hash
partition_checker_result=feasible|infeasible|unknown
planning_floor_status=PROPOSED_UNAPPROVED|APPROVED
planning_floor_approval_hash
pas_status
text_collected=0
model_calls=0
external_provider_calls=0
```

每个 `pas_snapshot_id` 必须与唯一一个 `pas_execution_commitment_id/hash` 绑定；receipt 必须由该 commitment 冻结的规则生成。桌面 receipt 不保存逐 donor 行、ID、身份映射、身份去重键、细粒度分布或未抑制的小样本组合；不能从聚合表反推出某位 donor 是否参与、撤回或拥有多少材料。

`nonresponse_count` 必须由 PAS03 冻结的招募／响应规则机械推导，不得简单等同于 `count_response_status=none`；后者也可能表示已经回答其他问题、但未提供任何计数档位。当前候选 aggregate receipt 含未经小单元抑制的精确聚合与数学结果，只允许留在 ACL 区；它不是本节的 PAS13 桌面投影，不能填写 `PAS13_small_cell_dry_run_receipt_hash`，也不能复制到桌面冒充公开安全 receipt。

### 19.9 结果后的唯一动作

- `PAS_NOT_AUTHORIZED/PAS_IN_PROGRESS/PAS_INCONCLUSIVE`：保持 `STAGE1_NOT_READY_METADATA_INCOMPLETE`，不启动 S1-A；
- `PAS_NO_GO_CURRENT_FRAME`：停止当前冻结招募框架下 `HN-F1-N / N3 / cross_donor` confirmatory 方向的准备，不收正文、不开发下游语义系统；扩大来源框架、探索 N2 或 `within_donor_ecological` 必须新版本重启，不能把本轮 PAS 当确认性证据；
- `PAS_PLAUSIBLE_NOT_VERIFIED`：项目所有者仍需完整冻结 A01–A21、A28、A32 并另行授权 S1-A；PAS donor 数以及事件／文档／时间块区间不能直接复制成 S1-A 合格计数；
- `PAS_VOID_SCOPE_OR_PRIVACY_BREACH`：停止并隔离 PAS 产物，按冻结事件响应规则处理；不得用受影响数据作任何容量结论。

### 19.10 当前状态

目前 PAS01–PAS14 均未完整冻结，项目所有者也没有批准 PAS execution commitment；在本文件记录的工作流范围内未联系 donor、未收任何 metadata 或正文，也没有受限 PAS registry。现实全局零状态仍需管理员另行签署 attestation。当前只能记录：

```text
hnf1_pas_status=PAS_NOT_AUTHORIZED
pas_execution_authorized=false
planning_floor_status=PROPOSED_UNAPPROVED
zero_state_scope=THIS_DOCUMENTED_WORKFLOW_ONLY
zero_state_admin_attestation=UNSET
donors_contacted=0
metadata_records_collected=0
text_collected=0
model_calls=0
external_provider_calls=0
stage1_status=STAGE1_NOT_READY_METADATA_INCOMPLETE
hnf1_supply_feasible=unknown
cmi_authorized=false
```

## 20. `HN-F1-PAS` 项目所有者冻结包（待补齐、待审批、未执行）

### 20.1 零状态与授权防火墙

```text
package_status=DRAFT_FOR_OWNER_COMPLETION
owner_review_decision=PENDING
zero_state_scope=THIS_DOCUMENTED_WORKFLOW_ONLY
zero_state_admin_attestation=UNSET
pas_execution_authorized=false
donor_contact_authorized=false
donors_contacted=0
metadata_records_collected=0
text_collected=0
vault_accessed=0
external_provider_calls=0
model_calls=0
s1a_execution_authorized=false
qth_assignment_authorized=false
cmi_authorized=false
```

“继续”“准备”“下一步”“完善方案”只授权继续编写冻结包，不等于批准 PAS 执行。唯一有效的执行授权必须满足：PAS01–PAS14 没有 `UNSET` 或 `REQUIRES_REAL_ADMIN_INPUT`，所有实际版本与 hash 已生成，机械启动门全部通过，项目所有者针对最终 `pas_execution_commitment_hash` 另行签署第 20.9 节的精确声明。

PAS 获批后只可联系冻结 frame 内的 donor，收集第 19.4 节列出的假名化枚举与计数档位，按批准的保留／撤回规则在受限区处理，运行冻结的本地 exact checker，并生成、隐私复核 ACL receipt 与经抑制桌面投影。正文、标题、路径、URL、截图、逐条时间戳／hash、真实 vault、Q/T/H、axis、语义字段、模型/API、外部 provider、S1-A、Spark 与 CMI 永不随 PAS 授权自动开放。

### 20.2 当前 14 项都不能被本地文件自动冻结

以下分组只区分“已经有推荐草案”和“还缺什么”，不表示任何一项已经批准：

1. **有推荐值但缺所有者／隐私负责人明确批准**：PAS01、PAS08、PAS09、PAS11、PAS13。
2. **必须由现实管理员提供运营事实**：PAS02–PAS07、PAS13。
3. **已有候选源码但仍必须冻结版本、生成正式测试／dry-run receipt 与获批 hash**：PAS10、PAS12、PAS13、PAS14。候选源码 hash 只证明当前字节存在，不等于 owner 已批准或正式 receipt 已进入不可变存储。

这些是可重叠依赖分类；进入多个分组的项目必须同时满足所有依赖。

因此当前没有任何一项可写成 `FROZEN` 或 `APPROVED`，合法总状态仍是 `PAS_NOT_AUTHORIZED`。

### 20.3 PAS01–PAS14 可审批决策表

```text
PAS01_status=PROPOSED_UNAPPROVED
PAS01_recommended_direction=HN-F1-N|N3|cross_donor
PAS01_owner_decision=PENDING

PAS02_status=REQUIRES_REAL_ADMIN_INPUT
PAS02_recruitment_frame_code=UNSET
PAS02_frame_is_finite_and_exhaustible=UNSET
PAS02_frame_composition_basis=UNSET
PAS02_cutoff_at=UNSET
PAS02_contact_order_rule=UNSET
PAS02_identity_dedup_rule_version=UNSET
PAS02_identity_controller_role_code=UNSET

PAS03_status=REQUIRES_REAL_ADMIN_INPUT
PAS03_window_open_at=UNSET
PAS03_window_close_at=UNSET
PAS03_timezone_recommended=Asia/Shanghai
PAS03_contact_attempt_limit_recommended=1
PAS03_late_response_rule_recommended=EXCLUDE_AND_REQUIRE_NEW_SNAPSHOT
PAS03_nonresponse_effective_bound_recommended=LB:0|UB:114
PAS03_closure_owner_role_code=UNSET
PAS03_frame_exhaustion_rule=UNSET

PAS04_status=REQUIRES_REAL_ADMIN_INPUT
PAS04_identity_store_opaque_id=UNSET
PAS04_response_store_opaque_id=UNSET
PAS04_checker_workspace_opaque_id=UNSET
PAS04_immutable_receipt_store_opaque_id=UNSET
PAS04_acl_matrix_hash=UNSET
PAS04_encryption_backup_restore_attestation=UNSET
PAS04_required_topology=NON_GIT|NON_DESKTOP|NON_AUTO_SYNC

PAS05_status=REQUIRES_REAL_ADMIN_INPUT
PAS05_notice_version_and_hash=UNSET
PAS05_questionnaire_version_and_hash=UNSET
PAS05_metadata_consent_version_and_hash=UNSET
PAS05_language_versions=UNSET
PAS05_withdrawal_channel_code=UNSET
PAS05_privacy_approval_record=UNSET

PAS06_status=REQUIRES_REAL_ADMIN_INPUT
PAS06_owner_role_code=UNSET
PAS06_recruitment_controller_role_code=UNSET
PAS06_data_admin_role_code=UNSET
PAS06_checker_operator_role_code=UNSET
PAS06_independent_privacy_reviewer_role_code=UNSET
PAS06_withdrawal_incident_controller_role_code=UNSET
PAS06_role_conflict_and_compensating_control_record=UNSET

PAS07_status=REQUIRES_REAL_ADMIN_INPUT
PAS07_declined_nonresponse_retention_deadline=UNSET
PAS07_active_response_retention_deadline=UNSET
PAS07_identity_mapping_retention_deadline=UNSET
PAS07_aggregate_receipt_retention_rule=UNSET
PAS07_withdrawal_sla_and_executor=UNSET
PAS07_incident_isolation_notification_and_disposal_rule=UNSET

PAS08_status=PROPOSED_UNAPPROVED
PAS08_high_risk_inclusion_allowed_recommended=false
PAS08_screen_disposition_enum_recommended=withdrawn|duplicate|late|declined|explicit_no|incomplete|none
PAS08_owner_and_privacy_decision=PENDING

PAS09_status=PROPOSED_UNAPPROVED
PAS09_external_form_or_provider_allowlist_recommended=NONE
PAS09_data_flow_attestation=UNSET

PAS10_status=CANDIDATE_NORMALIZER_IMPLEMENTED_NOT_FROZEN
PAS10_proposed_version=HN-F1-PAS-COUNT-v0.1-DRAFT
PAS10_codebook_hash=UNSET
PAS10_question_wording_hash=UNSET
PAS10_candidate_codebook_sha256=74d47470f04cdb8a22d96fdebcfe33733475092f5f70bfda2792a398b763c915
PAS10_candidate_machine_contract_sha256=6a137ee5b91ce6877707d95a97ab8f0b3129328ea3d7d9131859290aa43a32cf

PAS11_status=PROPOSED_UNAPPROVED
PAS11_planning_donor_floor=P0:18|M1:18|Rtech:8|total:44
PAS11_basis=CONSERVATIVE_MINIMUM_CARDINALITY_ROBUSTNESS
PAS11_owner_decision=PENDING

PAS12_status=CANDIDATE_MATH_KERNEL_IMPLEMENTED_NOT_GOVERNANCE_INTEGRATED
PAS12_proposed_version=HN-F1-PAS-PARTITION-v0.1-DRAFT
PAS12_implementation_hash=UNSET
PAS12_schema_hash=UNSET
PAS12_test_receipt_hash=UNSET
PAS12_candidate_algorithm_version=pareto-dp-three-batch.v2
PAS12_candidate_implementation_sha256=8edcaf3775f0ec6563694c9b48cd3b52c3ea0550823031fc2b8cdf0435940adb
PAS12_candidate_machine_schema_sha256=2fc1981966230ff9e7086a17991149004cced5b338fabcd35286aa33cc40d307
PAS12_candidate_output_schema_version=hnf1-pas-output-schema-contract.v0.1-draft
PAS12_candidate_output_schema_sha256=4cd44911d3691d064b496c8d2d502b4ace1af908569719f36bc755e4f4ea7752
PAS12_candidate_test_source_sha256=a00919da3f7cbc4be94f97aeb044df5eddd8ecb2e82a51a2abc881f05687d856
PAS12_candidate_pair_commit_protocol=aggregate-last.v1
PAS12_candidate_deadline_scope=NORMALIZATION_AND_LB_UB_SHARED_COOPERATIVE_BUDGET
PAS12_candidate_external_watchdog_required=true

PAS13_status=PROPOSED_UNAPPROVED
PAS13_small_cell_k_recommended=5
PAS13_complementary_suppression_required_by_protocol=true
PAS13_release_rule_hash=UNSET
PAS13_small_cell_dry_run_receipt_hash=UNSET
PAS13_owner_and_privacy_decision=PENDING

PAS14_status=REQUIRES_FINAL_RUNTIME_ARTIFACTS
PAS14_commitment_id=UNSET
PAS14_commitment_hash=UNSET
PAS14_immutable_store_opaque_id=UNSET
PAS14_approved_by_restricted_record=UNSET
PAS14_approved_at=UNSET
PAS14_expiry_or_revocation_code=UNSET
```

桌面审批摘要只保存上述 opaque code、版本、hash、状态与非敏感时间。真实姓名、联系方式、联系人名单、存储路径、身份键和 ACL 成员只允许存在于更窄 ACL 的现实执行附件；本轮不创建该附件。

### 20.4 PAS10 拟议计数合同 v0.1

PAS 记录的不是原始精确数量，而是协议有效计数：

```text
C_eff=min(C_raw,114)
```

`114` 来自 `max(batch_cap(P0), batch_cap(M1), batch_cap(Rtech))`，只表示单 donor 对粗分区的最大可能贡献。四类数量字段——联合唯一节点、独立事件、唯一源文档、非重叠时间块——拟共用以下档位：

```text
B00_ZERO     = raw [0,0]       | effective [0,0]
B01_ONE      = raw [1,1]       | effective [1,1]
B02_2_3      = raw [2,3]       | effective [2,3]
B03_4_5      = raw [4,5]       | effective [4,5]
B04_6_7      = raw [6,7]       | effective [6,7]
B05_8_11     = raw [8,11]      | effective [8,11]
B06_12_17    = raw [12,17]     | effective [12,17]
B07_18_26    = raw [18,26]     | effective [18,26]
B08_27_31    = raw [27,31]     | effective [27,31]
B09_32_47    = raw [32,47]     | effective [32,47]
B10_48_63    = raw [48,63]     | effective [48,63]
B11_64_95    = raw [64,95]     | effective [64,95]
B12_96_113   = raw [96,113]    | effective [96,113]
B13_114_PLUS = raw [114,unknown] | effective [114,114]
UNKNOWN      = raw [unknown,unknown] | effective [0,114]
```

拟议的确定性解释规则是：

1. `primary_screen_disposition_code` 属于 `withdrawn|duplicate|late|declined|explicit_no`，或任一必要资格明确为 `no`：全部合格 `LB=0, UB=0`；这是最高优先级。`incomplete` 不属于明确排除。
2. 对某个计数字段已经选择合法档位时：只有必要资格全为 `yes`、`screen_response_status=complete`、`count_response_status=complete`、截止前且 active，才使用档位有效下／上端点；若没有显式排除但资格为 pending/unknown、回答 incomplete，或计数状态不是 complete，则该字段 `LB=0`、`UB=所选档位有效上端点`。
3. 对应计数字段本身为 `UNKNOWN`、`count_response_status=none`，或有限 frame member 完全 nonresponse，且没有显式排除时：该字段 `LB=0, UB=114`；这只防止假 no-go，不表示 donor 有 114 个材料。`none` 要求八个档位 token 全部为 `UNKNOWN`；`partial` 要求未知与非未知并存；`complete` 允许显式 `UNKNOWN`。`pending` 只能配 `none`，`complete` 只能配 `complete`。
4. donor 下界人数只计 `joint_node_LB>=1` 者；上界人数只计未明确排除且 `joint_node_UB>=1` 者。
5. `LB>UB`、未知枚举、codebook/hash 不符、同一 donor 多行、状态—排除码不符合冻结优先级，或联合节点界超过任一诊断边际界：输入无效，只能 `PAS_INCONCLUSIVE`。
6. 档位端点、字段定义、donor-facing wording 或 unknown/nonresponse 规则有任何变化，都必须新建 PAS 版本与 snapshot，不能观察回答后修改。

`recontact_permission` 只治理未来能否再次联系，不参与现有材料的容量资格；该选择已经进入候选机器合同，但尚未获得 PAS01–PAS14 批准。候选 PAS10 machine contract hash 覆盖枚举、unknown/nonresponse、处置优先级、资格、`joint<=各诊断边际` 与拒绝额外 schema 字段等规则。

当前已有候选 PAS10 归一化实现，但 codebook 仍是 `CANDIDATE_NORMALIZER_IMPLEMENTED_NOT_FROZEN`；在真实问卷 wording、正式版本、正式 test receipt 与 owner hash-bound 批准生成前不能用于真实执行。上面的 candidate hash 不能复制到 `PAS10_codebook_hash`。

### 20.5 PAS12 拟议精确粗分区合同 v0.1

PAS12 只读冻结的假名化枚举与数值端点，模型/API 调用固定为 0。冻结常量拟为：

```text
batch_order=[P0,M1,Rtech]
node_target={P0:144,M1:576,Rtech:48}
hard_donor_floor={P0:15,M1:15,Rtech:8}
planning_donor_floor={P0:18,M1:18,Rtech:8}
batch_cap={P0:27,M1:114,Rtech:8}
```

对场景 `s in {UB,LB}`：

```text
C_s(d)=min(
  joint_unique_node_s(d),
  distinct_event_s(d),
  unique_document_s(d),
  nonoverlap_time_block_s(d)
)
capacity_s(b,d)=min(C_s(d),batch_cap(b))

x[b,d] in {0,1}
n[b,d] in Z>=0

sum_b x[b,d] <= 1
x[b,d] <= eligible_s(d)
x[b,d] <= n[b,d]
n[b,d] <= capacity_s(b,d) * x[b,d]
sum_d n[b,d] == node_target[b]
sum_d x[b,d] >= donor_floor_s[b]
```

UB 使用 `15/15/8`，LB 的当前候选合同使用 PAS11 拟议但尚未获 owner 批准的 `18/18/8`；正式运行只能在 PAS11 批准并把该阈值绑定到最终 commitment 后沿用。状态判断必须按以下顺序短路，较后的分支不能覆盖较前的状态：

```text
privacy_or_scope_breach
  -> PAS_VOID_SCOPE_OR_PRIVACY_BREACH

PAS01-PAS14 not frozen/approved OR hash-bound authorization absent
  -> PAS_NOT_AUTHORIZED

window_open
  -> PAS_IN_PROGRESS

window_closed BUT (
  finite frame not exhausted OR identity dedup incomplete OR
  frozen read-only snapshot absent OR effective bounds unresolved OR
  snapshot-to-commitment binding invalid OR
  governance conditions not satisfied OR
  not exactly one effective row per frame member OR
  unknown/nonresponse not mapped by frozen PAS10 OR
  input/schema/hash invalid OR checker error/timeout/non-exact
)
  -> PAS_INCONCLUSIVE

all_terminal_preconditions_satisfied =
  no privacy/scope breach AND PAS01-PAS14 frozen/approved AND
  hash-bound authorization present AND window_closed AND
  finite frame exhausted AND identity dedup complete AND
  frozen snapshot bound to commitment AND effective bounds resolved AND
  governance conditions satisfied AND
  exactly one valid effective row per frame member AND exact checker completed

all_terminal_preconditions_satisfied
AND LB exact feasible AND UB exact infeasible
  -> CHECKER_INPUT_OR_IMPLEMENTATION_INVALID
  -> PAS_INCONCLUSIVE

all_terminal_preconditions_satisfied AND UB exact infeasible
  -> PAS_NO_GO_CURRENT_FRAME

all_terminal_preconditions_satisfied
AND UB exact feasible AND LB exact feasible
AND governance conditions satisfied
  -> PAS_PLAUSIBLE_NOT_VERIFIED

all_terminal_preconditions_satisfied AND UB exact feasible AND LB exact infeasible
  -> PAS_INCONCLUSIVE

all_terminal_preconditions_satisfied AND any other unmatched exact combination
  -> PAS_INCONCLUSIVE
```

不得用 LB 不可行产生 no-go，也不得用 UB 可行产生 plausible。当前候选实现的 witness 只承诺 `SHA256(commitment_id | screen_donor_id)` 驱动的确定性、非规范性运行记录：输出按 `P0 < M1 < Rtech` 排列，同批 donor 顺序确定，但早停 witness 不承诺对全部可行解作全局字典序最小化。状态不得依赖选择了哪个 witness。若以后必须冻结唯一的全局最小 witness，应另行实现、测试并重新批准 tie-break；不能把旧草案的“拟最小化”冒充当前行为。

当前仓库已存在 `tools/hnf1_pas_partition_checker.py` 与 `tests/test_hnf1_pas_partition_checker.py`：前者是无网络、无模型、无正文的候选 PAS10 归一化器与 PAS12 精确数学核，后者覆盖档位、frame／implementation／schema 绑定、独立穷举、固定边界、资源 fail-closed、输出结构和 CLI 边界。它只计算 LB/UB 数学结果，不处理 PAS01–PAS14、隐私越界、窗口关闭、frame 穷尽、身份去重、授权或治理状态，因此不是完整 PAS checker，也不能输出正式 `pas_status`。

候选 math commitment 绑定 snapshot hash、frame member set hash、PAS10 codebook／machine contract hash、问卷 wording hash、预期实现与 machine schema hash、算法版本、成员数和资源上限。frame set hash 为 `SHA256(domain || sorted ASCII opaque IDs, each followed by LF)`。这些普通 SHA-256 绑定只支持核对运行所声称的 artifacts 是否与 commitment 中的字节摘要一致；它们本身不证明运行真实发生、数学结果正确、来源经过认证、commitment 在观察 snapshot 前已经过 PAS02/PAS14 批准，或 PAS01–PAS14 已冻结。数学结果仍须用原 commitment、snapshot 与冻结实现重跑核验。

候选输出采用 `aggregate-last.v1` 两文件逻辑提交协议：执行器先完整写入并发布 `ACL_PRIVATE_MATHEMATICAL_RUN_RECORD`，最后发布 `ACL_ONLY_NOT_PAS13_PUBLIC` aggregate；aggregate 是唯一完成标记。缺少 aggregate、`pair_complete!=true`、exact output schema／共同字段／LB-UB 场景投影不一致、或 `private_run_record_canonical_sha256` 不匹配时，文件组必须视为未提交或状态不确定，不能作为 receipt。消费者必须同时读取两份文件并验证；`validate_output_pair_structure` 只做结构与两文件内部一致性检查，不提供签名、授权真实性或 against-inputs 数学结果重放。要核验数学结果，必须以原 commitment、snapshot 和冻结实现重新运行；拥有写权限者仍可能重建一组自洽文件。

`aggregate-last.v1` 不是跨文件系统事务，也不替代目录 fsync、备份恢复、不可覆盖存储或现实 ACL 证明。若进程在 private run record 发布后、aggregate 发布前崩溃，private 可能作为 `uncommitted partial` 留存；不得静默删除或当作正式结果，应按冻结恢复／隔离规则处理。任何含 `STATE_UNCERTAIN` 的输出错误码都必须隔离整个 restricted root，不能依据较窄错误名猜测其他文件安全。代码内 Desktop／同步目录／Git／reparse／hardlink 检查和 `0o600` 只是事故防护；正式执行必须由现实管理员证明非 Git、非桌面、非自动同步的 ACL root、可信单写者、只读代码／输入、无不受信并发写者、外部 hard watchdog 与 CPU／内存限制。这些前置条件当前均未建立。

`max_seconds` 从进入 `_run_checker_validated` 候选数学核时开始，覆盖 snapshot 的语义校验／归一化、聚合摘要、LB 与 UB 两个场景以及 solver 内 witness、derivation digest 和终态工作；LB/UB 共用同一个 absolute monotonic cooperative deadline，不是每个场景各自一份预算。public `run_checker` 在此之前读取并散列实现、解析／校验 commitment、散列并解析 snapshot，在数学核返回后还会再次读取实现；CLI 的输入读取与最终文件发布也在该预算之外。任何数学核检查点发现越过 deadline 都只能形成 resource error 与 `MATHEMATICAL_INCONCLUSIVE`；该预算不是硬 wall-clock，正式运行仍需独立进程 watchdog。完整 PAS governance 层未来才可把数学不确定映射为 `PAS_INCONCLUSIVE`。

候选 aggregate 含未经小单元抑制的精确聚合与数学结果，`contains_direct_donor_id_fields=false` 只表示没有 donor-ID 专用字段，不保证任意 opaque 字符串值不会碰撞；它只能留在 ACL 区，不能作为 PAS13 桌面投影。当前已有候选数学核，但尚未集成 PAS 治理状态机、正式 hash-bound execution authorization、不可变正式 test receipt 或 PAS13 隐私投影；正式 `PAS12_implementation_hash`、`PAS12_schema_hash` 与 `PAS12_test_receipt_hash` 继续为 `UNSET`，PAS12 仍不能冻结。

### 20.6 拟议角色、存储与数据流隔离

推荐角色：

1. `Owner/PI`：批准最终 commitment hash；默认只看协议和聚合结果。
2. `Recruitment controller`：唯一可看身份／联系方式台账与去重键；不能看回答计数内容。
3. `PAS data admin`：只看假名化回答；不能看身份映射，也不能读取正文。
4. `Checker operator`：只读冻结计数 snapshot；输出 exact 聚合 receipt。
5. `Independent privacy reviewer`：独立审查 receipt、小单元抑制与桌面投影。
6. `Withdrawal/incident controller`：以最小必要权限协调撤回、隔离和事件响应，访问必须留在受限日志。

recruitment controller 与 data admin/checker 的分离，以及 privacy reviewer 与桌面投影生成者的分离，都是不可补偿的硬门；现实人员不足时必须保持 `PAS_NOT_AUTHORIZED`。只有 Owner、withdrawal/incident controller 等未参与身份—回答 join 或桌面投影生成的角色，才可在 PAS06 明确记录冲突、有限兼任与补偿控制；不能静默兼任。

推荐四区拓扑与单向数据流：

```text
frozen contact frame
  -> identity ledger（最窄 ACL）
  -> 一次性 screen_donor_id
  -> pseudonymous response store
  -> frozen read-only snapshot
  -> local exact checker workspace
  -> ACL exact receipt
  -> independent privacy review
  -> suppressed desktop projection
```

四区都必须非 Git、非桌面、非自动云同步。身份 ledger 与 response store 不得 join 后导出；桌面只写 opaque store code 与 hash，不写真实路径或 ACL 成员。本轮没有建立其中任何一区。

### 20.7 告知、撤回、保留与聚合发布草案

PAS05 的中性告知／问卷必须说明：研究目的、只收哪些枚举和档位、完全自愿、拒绝无不利后果、谁能访问、无外部 provider、保留与撤回边界、可能发布经抑制聚合、不会保证进入后续研究，以及 PAS consent 不等于正文、模型或 S1-A consent。问卷不披露 38/44/768、Q/T/H 或具体目标轴，避免不必要的目标锚定；这不能用于隐瞒真实用途、风险或数据流。

撤回与保留必须满足：

- 联系前、回答中和 PAS 终态前均可撤回；recruitment controller 通过窄 ACL 映射通知 data admin 标记 withdrawn，checker 排除；
- 终态后撤回不覆盖旧 receipt；追加 withdrawal/tombstone 事件并将旧 snapshot 标为不可继续用于 S1-A 规划，若研究继续必须新 snapshot 重算；
- 只可按批准政策处理 PAS 身份／响应研究记录，绝不能删除或改变 OB 真实记忆；
- declined/nonresponse、active response、身份映射和聚合 receipt 必须分别给出真实保留截止，不能写“长期”或让模型代填法律期限。

PAS13 的推荐草案为 `k=5`：任何状态、档位或交叉格小于 5 时写 `SUPPRESSED`；若总数减其他格可反推隐藏格，必须作互补抑制。精确阈值判定只留 ACL receipt；桌面只发布 PAS 状态、必要 hash 和无法反推个体的聚合。该规则仍需所有者与独立隐私 reviewer 批准。

### 20.8 冻结前机械启动门

启动前必须逐项得到 `true`：

```text
all_PAS01_to_PAS14_resolved_and_approved=false
finite_exhaustible_deduplicated_frame_confirmed=false
real_role_and_acl_matrix_confirmed=false
notice_questionnaire_consent_hashes_verified=false
count_codebook_and_wording_hash_verified=false
exact_checker_implementation_and_tests_verified=false
small_cell_dry_run_no_reidentification=false
effective_external_provider_access_blocked=true
PAS09_owner_and_data_flow_verified=false
immutable_commitment_anchor_verified=false
zero_state_admin_attestation_verified=false
owner_hash_bound_signature_verified=false
```

任一项为 false、unknown、缺文件、hash 不符或 checker 非 exact，状态都必须保持 `PAS_NOT_AUTHORIZED`。此时不得创建真实 response registry、发送邀请或收 metadata。

### 20.9 项目所有者最终批准栏

只有所有现实字段、版本、实现、测试和 hash 齐全后，才可生成以下签署记录：

```text
owner_decision=<PENDING|APPROVED|REJECTED>
approval_scope=HN-F1-PAS_ONLY
approved_pas_protocol_version=<exact>
approved_pas_execution_commitment_id=<exact>
approved_pas_execution_commitment_hash=<exact>
approved_recruitment_frame_code=<opaque>
approved_window_code=<opaque>
approved_role_access_matrix_hash=<exact>
approved_notice_questionnaire_consent_hash=<exact>
approved_count_codebook_hash=<exact>
approved_checker_hash=<exact>
approved_release_rule_hash=<exact>
pas_execution_authorized=<false until explicit signature>
donor_contact_authorized=<false until explicit signature>
text_collection_authorized=false
vault_access_authorized=false
external_provider_authorized=false
model_calls_authorized=false
s1a_execution_authorized=false
qth_assignment_authorized=false
cmi_authorized=false
approved_by=<restricted signature record>
approved_at=<timestamp>
approval_expiry=<timestamp or frozen code>
```

有效批准声明必须针对最终 hash，精确表达以下边界：

> 我已审阅上述最终 commitment hash，并批准仅按该不可变版本执行 HN-F1-PAS；授权包括联系冻结招募框架、收集协议列出的假名化枚举与计数档、按批准的保留／撤回规则在受限区处理、运行冻结的本地 exact checker，以及生成并独立隐私复核 ACL 聚合 receipt 与经抑制桌面投影。正文、vault、外部服务、模型、S1-A、Q/T/H、Spark 与 CMI 均不授权。

在签署记录与这句 hash-bound 声明实际存在前，`owner_decision=PENDING`、`pas_execution_authorized=false`、`donor_contact_authorized=false`。

### 20.10 当前下一步

当前完成了可审批合同草案，以及未冻结的 PAS10 候选归一化器和 PAS12 候选数学核；没有完整治理 checker、正式 receipt 或现实执行。要继续到真正 freeze，必须按顺序补齐：

1. 项目所有者决定保留当前 `deterministic non-normative witness`，还是要求实现原草案的全局字典序最小 tie-break；任何改变都要重算候选 hash 与回归；
2. 将最终候选 codebook、machine/output schema、implementation 和测试源码锁为不可变 artifact，在外部 watchdog／资源隔离下生成可复核的正式合成 test receipt；当前 test source hash 不能冒充 test receipt hash；
3. 现实管理员补齐有限 frame、截止／窗口、一人一 donor 去重、非 Git/非桌面/非自动同步受限存储、单写者、现实角色／ACL、告知／问卷／consent、撤回／保留／事件响应，即 PAS02–PAS07；
4. 项目所有者对 PAS01、PAS08、PAS09、PAS11作显式政策决定；
5. 独立隐私 reviewer 完成 PAS13 小单元与互补抑制 dry-run，批准发布规则并生成不可变 receipt；候选 ACL aggregate 不能替代这一步；
6. 所有正式 hash 与现实 attestation 齐全后，生成 PAS14 commitment，供项目所有者逐 hash 批准。

以上步骤完成前，正式 PAS10／PAS12／PAS13／PAS14 字段保持 `UNSET/PENDING`，`PAS_NOT_AUTHORIZED` 不变；当前不联系 donor、不收 metadata／正文、不运行真实数据、不启动 S1-A、Spark 或 CMI。
