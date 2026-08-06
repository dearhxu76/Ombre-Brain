# OB HN-F1 自然来源与语言优先供给协议

日期：2026-08-03
状态：`DRAFT_FOR_OWNER_REVIEW`
执行授权：`false`
实验模型/API 调用：`0`
单轴评审调用：`0`
CMI 调用：`0`
产品代码修改：`false`

## 0. 一句话结论

HN-F1 不继续评审 HN-F0 的 45 条语言合格边，也不运行 CMI。它重新建立一个以 `natural_observed` 为主、先检查 Q/T/H 单篇可读性、再判断严格关系和单轴破坏的新供给门。

只有全新冻结批次最终形成至少 12 个 Q/T/H 节点均唯一、四轴各至少 2 个的有效三元组，才允许另写 CMI 协议；本协议本身始终保持 `cmi_authorized=false`。

## 1. 为什么必须另开 HN-F1

HN-F0 的聚合事实是：

- 严格开发正例 26 个；
- T singleton 语言通过 11/26；
- H singleton 语言通过 71/172；
- 语言边通过 45/208；
- 45 条边只覆盖 11 个唯一 T 和 42 个唯一 H；
- T/H 唯一匹配上界 11，小于冻结门槛 12；
- 单轴评审和 CMI 均为 0。

HN-F0 只能作为预算敏感性参考，不能用于逐项修规则、重选失败文本或把同批边界例救回。HN-F1 的新批次不得包含 HN-F0、ARN、ePiC、AnaloBench 及此前人工 hard negative 的 exact duplicate、near duplicate、改写版或同一事件簇。

HN-F1 要回答的仍只是供给问题：自然来源中是否存在足够多的、语言可比且结构上只破坏一个冻结轴的独立成文 H。它不评价 Spark 的帮助度、自动 selector、比较器、CMI 或产品表现。

## 2. OB 哲学与安全边界

1. 记忆只提供候选材料，不能替当前模型联想、判断、接受、质疑、拒绝或行动。
2. Spark 只能产生待核查候选，不能产生规范性权重、拒绝、permit 或计划。
3. HN-F1 不连接、写入、归档、遗忘、合并、重标记或删除真实 OB 记忆。
4. 真实 vault 不得挂为可写测试目录；任何研究材料只能来自所有者主动选择的只读导出副本。
5. 评审结果、置信度、边和轴不得写回 OB。
6. 原始私密文本无论同意范围如何都不得进入 Git、公开交付目录、终端输出或日志。只有在贡献者明确授权指定服务商后，去标识文本才可进入该服务商的模型上下文；否则只允许本地人工评审。
7. 本轮不新增 MCP 工具、环境变量、数据库表、Dashboard 功能或产品模块。

## 3. `natural_observed` 的操作定义

材料只有同时满足以下条件，才可标记为 `natural_observed`：

- 在贡献者获知本次 HN-F1 招募、材料征集要求、候选角色或四轴任务之前已经存在；仅仅事先知道 OB 或 Spark 概念不构成排除；
- 产生于普通对话、工作记录、日记、事件记录或日常 OB 使用，而不是为正例、负例或某一轴专门写作；
- 有可追溯的原始记录、时间信息和版本；
- 未为了实验进行润色、扩写、补全、因果重构或结果反转；
- 具有明确的研究许可或贡献者同意；
- 去标识后仍能保留行动、顺序、角色和结果判断所需的原文证据。

`natural_observed` 只描述单个节点文本的产生方式，不表示事件已经得到外部事实核验，也不表示它天然是结构关系 gold。Q、T、H 必须逐节点判定；任一节点不是 N3/N2 时，整个三元组不得称为全自然观察来源。

## 4. 来源等级与 cohort 隔离

| 等级 | 定义 | HN-F1 用途 |
|---|---|---|
| N3 | 招募前已存在，有外部时间戳，以人为主要作者，原文可追溯 | HN-F1-N 主分析优先 |
| N2 | 招募前已存在，贡献者声明来源和时间，原文可追溯但无独立时间戳 | HN-F1-N 可用，单独分层 |
| N1 | OB 自然运行产生的摘要、grow、dream 等系统派生文本，并能回链原始 span | 仅 HN-F1-OB 生态复现；属于 `natural_system_derived`，不计入 `natural_observed` 或 HN-F1-N |
| P1 | 有明确许可的公开第一人称或事件记录 | 独立公开来源 cohort，不与 N3/N2 合并 |
| E0 | 招募后按问题回忆或重新讲述的经历 | elicited 诊断，不算 natural_observed |
| A0 | 看不到 Q、T、轴和既有候选的独立人工写作 | HN-F1-A fallback，不能并入 HN-F1-N |
| S0 | 为 HN-F1、某个 Q、候选角色或目标轴而由 LLM 生成、扩写、改写或定向反转 | 禁止进入正式供给门；不与招募前自然产生且可回链的 N1 混同 |

“网页可访问”不等于允许研究复用。论坛、社交媒体、聊天记录和私人文档必须有明确许可；来源、许可或创建时间不清即排除。

HN-F1-N、HN-F1-P、HN-F1-A 和 HN-F1-OB 必须分别运行、分别报告，不能为了跨过 12/2 门槛合并计数。N3 与 N2 也作为独立 analysis stratum；12/2 必须在一个评分前指定的 provenance stratum 内成立，不能把两个不足的层相加。

### 4.1 贡献者模式必须预先二选一

- `cross_donor`：Q、T、H 来自三个不同贡献者；同一贡献者不能跨 P0、M1、Rtech 或未来 confirmation 复用。它回答跨来源供给问题。
- `within_donor_ecological`：每个三元组的 Q、T、H 必须来自同一贡献者，但三者来自不同源文档、不同事件簇和互不重叠的时间块；P0 与 M1 仍按时间块隔离。它只回答个人 OB 生态内供给问题，不能与 `cross_donor` 合并，也不能称为贡献者级独立复制。

cohort 建立前必须冻结其中一种模式。`cross_donor` 的每个固定批次按 Q、T、H 三种角色分别限制单一贡献者节点数不超过 `floor(0.20*该角色固定节点数)`；最终匹配还执行第 18 节的三元组级 20% 上限。`within_donor_ecological` 必须报告贡献者数、每位贡献者的单元数并按贡献者分层，不能把事件数冒充独立人数。

### 4.2 A0 fallback 不是自然来源补丁

只有 HN-F1-N 的预注册收集窗口结束，并以 `STOP_HNF1_SOURCE_CAPACITY_LT_FIXED_BATCH` 停止后，才可另写协议开启 HN-F1-A。两者不得合并。

HN-F1-A 必须同时满足：作者看不到 Q、T、assigned axis、映射、评分标准和既有候选；只收到中性的独立事件写作要求；不得使用 LLM、模板改写或复制既有故事，并提交作者声明；整个文本池在角色分配前一次冻结；作者不兼任去标识者、proposer 或评审者，并有评分前冻结的贡献上限。正式 HN-F1-A 的 Q、T、H 都必须从同一 A0 池事后选择并服从冻结 donor mode；自然 Q 配 A0 T/H 或任何其他 mixed-origin 组合只能作为诊断，不能称为全 A0 或计入正式门。任何面向特定 Q 或轴的定向写作只能标为 `constructed_counterfactual`，进入独立方法学实验，不能计入 HN-F1 供给门。

## 5. 数据治理与去标识

### 5.1 同意范围必须拆开

每条贡献记录分别声明：

- 是否允许内部人工评审；
- 是否允许发送给指定外部模型服务商；
- 获准的具体 API endpoint、模型版本、数据是否用于训练、服务日志／保留期限、处理地域和转包商；
- 是否允许公开原文片段；
- 是否允许未来实验复用；
- 保留期限与撤回方式。

默认权限只允许本地、去标识的内部研究。没有明确外部模型授权时，不得发送给任何云模型服务商。

### 5.2 只读导出

- 数据管理员从所有者明确选择的来源制作只读研究副本；原始副本保存在 ACL 受限、非 Git、非桌面交付、非自动云同步的研究路径，实验程序不直接读取真实 `buckets/`。
- 获批 source steward 先在原始受限区按第 6 节做不含 Q/T/H/axis 的事件预切分和闭包；只有预切分冻结后才逐事件簇去标识。第二名只看受限原文与去标识版本的复核者确认 span 对齐和无跨簇占位符链接。
- 身份映射、原始路径和 HMAC 密钥单独保存，不进入评审包或正式交付目录。
- 自动研究脚本只能读取冻结的去标识副本；原始材料只允许获批的数据管理员和去标识人员访问。
- 研究撤回只改变研究副本的资格和保留状态，并留下不含正文的 tombstone；不得改变或删除 OB 记忆真源。
- 若同意条款要求销毁研究副本，只能由数据管理员按条款处置该副本；实验脚本不得自行物理删除。这不等于删除真实记忆。

### 5.3 去标识规则

去标识人员看不到 Q/T/H 身份、assigned axis 和预期关系，只允许：

- 删除姓名、账号、联系方式、精确地址、凭据、私密 URL 和可反查标识；
- 使用 `[PERSON_1]`、`[ORG_1]` 等中性占位符；
- 保留否定词、相对时间、行动方向、先后顺序和结果方向；
- 记录每一步 transformation。

占位符编号在每个事件簇内重新开始，不能借稳定编号跨故事关联贡献者。禁止用 `[MANAGER]`、`[VICTIM]` 等泄漏功能角色的占位符，禁止润色、摘要、补写或“修顺”因果。transformation log 只能记录变换类型和位置，不能复制凭据或其他秘密字面值。

涉及未成年人、医疗、性、精确位置或第三方私密叙述时必须另过冻结的高风险许可门；核心第三方身份、稀有事件组合或秘密无法安全遮蔽时整条排除。P1 公开材料同样适用，公开可见不等于没有隐私风险。如果去标识会破坏结构，该条直接排除。

## 6. 事件簇、来源和贡献者隔离

事件簇在任何人看到候选角色或 assigned axis 前冻结。判断依据是同一个具体行动—结果链，不是相同日期、主题、人物或情绪。

候选事件必须能从原文分别定位：

- X：行动前情境、约束或角色；
- A：有方向的行动或状态变化；
- Y：发生在 A 之后的可观察结果。

缺 X、缺 A、结果与行动不可分离或时序不清时，保守排除。共干预不自动排除，但若使行动—结果链或任何候选关系的证据不可识别，按 boundary/fail 处理；来源切分者此时看不到 assigned axis。

其他硬约束：

- 同一事件的多个消息、记忆副本、摘要、supersedes 链和近重复必须合并为一个簇；
- 两名来源切分者不一致时，只允许合并为 super-cluster 或排除，不允许事后细切；
- 同一事件簇、源文档或近重复不能同时充当 Q、T、H，也不能跨批次复用；
- Q、T、H 的贡献者关系严格服从评分前冻结的 `cross_donor` 或 `within_donor_ecological` 模式，不允许逐项例外；
- `cross_donor` 模式中，同一贡献者的材料不能跨 Pilot、Main、技术 reserve 或未来 confirmation；
- `within_donor_ecological` 模式中，贡献者可以重复，但事件簇、源文档和时间块不能跨批次复用，且结果必须按贡献者分层。

## 7. 最小来源清单

正式来源 manifest 只保存匿名元数据，不保存正文：

```text
record_id
donor_group_id
source_document_id
event_cluster_id
origin_class
provenance_grade
source_created_before_recruitment
recruitment_cutoff_at
source_frozen_at
consent_obtained_at
recorded_at_precision
human_or_system_derived
traceable_source_span
consent_scope
external_provider_allowlist
license_or_consent_version
language
source_medium
register_bin
length_bin
transformation_log
redaction_version
event_cluster_manual_version
pii_review_status
donor_mode
raw_text_hmac
redacted_text_sha256
prior_batch_denylist_hit
withdrawal_status
```

`raw_text_hmac` 的密钥不进入 manifest。逐条 `redacted_text_sha256` 也只留在受限区，不能作为对外成员资格探针。桌面／Git／公开目录只保留不跨批次复用的随机 opaque ID、批级 commitment/root、协议和聚合计数；撤回 tombstone 不含正文、逐条 hash 或可链接 ID。匿名 metadata manifest 与受限 redacted-text registry 分开保存。

任何含去标识正文、逐字 EvidenceSpan、角色映射、blind pack、模型输入／输出、raw API receipt 或 item-level 评审结果的文件仍具有再识别风险，必须留在同一 ACL 受限研究目录，不得进入桌面、Git、公开附件或普通终端日志。对外只能发布经隐私复核的聚合计数和批级承诺。

## 8. 固定批次与预算

HN-F1 采用 Pilot 与固定 Main，不允许根据语义结果追加样本。

| 批次 | 固定 Q–T 候选单元 | 轴分层 | 固定 H 节点数 | 每 T 固定提交上限 | 用途 |
|---|---:|---:|---:|---:|---|
| P0 Pilot | 24 | 每轴 6 | 96 | 8 | 验证来源、语言和流程是否具备最低供给能力 |
| M1 Main | 96 | 每轴 24 | 384 | 8 | HN-F1-N 正式供给门 |
| Rtech | 8 | 每轴 2 | 32 | 8 | 仅评分前客观技术损坏／评分前撤回的预排序替补 |

约束：

- P0、M1、Rtech 分别恰好冻结 24/96/8 个 Q–T 单元和 96/384/32 个 H 节点；更大的原始来源池按评分前冻结的确定性顺序截断，名单外记录永不补入本轮；
- 每个 H 最多进入两条候选边；
- P0、M1、Rtech 的 Q、T、H、事件簇、源文档和时间块完全不复用；贡献者是否复用严格服从冻结的 donor mode；
- P0 永久排除在 Main 计数之外；
- M1 与 Rtech 的完整 roster、轴分配、顺序和一对一替补映射必须在 P0 任一语义结果可读前冻结；
- 每个 Rtech slot 在不可变 commitment 中固定 `reserve_slot_type=qt_unit|h_node`、`replaces_formal_slot_id` 和允许的 activation reason 集合。`qt_unit` 替换整对 Q–T 并继承正式 slot 的 assigned axis；`h_node` 只替换一个 H slot。替补必须同角色、同 provenance stratum、同匹配键、满足相同贡献者模式，且一个替补只能映射一个正式 slot；
- 实际启用另写不可覆盖的 activation receipt，记录 `actual_reason`、证据 hash、决定时间、目标 pack hash 和 `any_semantic_result_read=false`；绝不回写 commitment；
- “评分前”指该 cohort 任一语义评分可读之前。只有哈希／文件损坏、评分前撤回、评分前发现的 exact/near duplicate 或 manifest/schema 无效可以启用预映射替补；语言差、结构失败、`unknown`、匹配不足或结果不理想都不算技术故障；
- 任一语义结果可读后永不替补；映射缺失或 Rtech 耗尽触发 `STOP_HNF1_SOURCE_CAPACITY_LT_FIXED_BATCH`；
- 贡献者可在任何阶段撤回；相关事件簇和全部下游边立即失效。语义评分已可读时只在原冻结图上重算，不补记录、不换边、不启用 Rtech；
- P0 暴露规则问题时，必须升级协议版本并使用全新的 P0/M1/Rtech，不能修 prompt 后重跑旧文本。

选择 96 个 Main 单元只是固定工程容量设计，不是 power calculation。到达某一阶段的固定 M1 项必须全部处理完，不能在凑到 12 条后提前停止。HN-F0 的通过率只用于说明必须给最终 12 条留下充足损耗空间；不能将其当作 HN-F1 的独立同分布成功概率。

### 8.1 Q–T 候选单元如何产生

Q–T 候选 pairing 不是由语言或结构评审者临场挑选。评分前冻结的 `positive proposer` 只能在去标识、来源合格且角色尚未分配的既有事件簇池中建立边，不得生成、改写、拼接或补写文本，也看不到 assigned axis、H 池、语言结果或 HN-F0 逐项结果。其身份、instructions、候选呈现／分批方式、调用或人工预算、排序和并列规则必须在 P0 任一评分前冻结；所有 Q、T 节点一对一且不复用，失败后不重新配对。

该步骤允许为供给可行性建立 `oracle_positive_supply`，但不能据此声称自动 selector 能找到正例。若使用模型 proposer，模型服务授权和版本必须已获批准；若使用人工 proposer，作者不能兼任 proposer，proposer 不能兼任后续人工评审。

## 9. 全流程不可逆顺序

1. 项目所有者批准来源全集、来源 cohort、贡献者模式、收集窗口、同意范围、模型服务范围和人工评审安排。
2. 先完成只读导出、受限区事件预切分与闭包、逐簇去标识、红acted 双人复核、exact/near duplicate 审计和受限来源 registry；此时不分配 Q/T/H 或 assigned axis，也不读取语言／结构分数。
3. 做只基于来源、同意和固定容量的审计；容量不足立即停止。
4. 在运行任何 semantic proposer 或读取任何语义输出前，先冻结本协议核心、reviewer instructions、全部 pack builder/reconciler/matcher、positive proposer 与 H proposer 的实现／instructions／预算／排序、`surface_compatibility_rule`、solver、blind/position-key schema、denylist 和规则 root anchor。
5. 运行已冻结的 positive proposer 建立 Q–T 候选单元；随后只冻结实际 P0、M1、Rtech roster、技术替补 slot map、四轴分配、blind IDs 与 data root anchor。此后不得修改第 4 步规则。
6. 只启封并完成 P0 的 Q/T/H singleton 语言门；未激活的 Rtech 和 M1 不评分。P0 只检查来源与语言兼容容量，不参与 Main 结果。
7. P0 通过后才启封并完整执行 M1 singleton 语言门；只发布不含分数／理由的 eligible text whitelist 和机械 `G_surface` edge allowlist。
8. 执行 M1 语言容量门；失败时严格正例、H proposer 和单轴调用均为 0。
9. 只对 Q、T 均语言合格的冻结候选单元执行严格 Q–T 正例门。
10. 由评分前冻结的 axis-aware H proposer 对每个严格正例的 `G_surface` allowlist 产生完整排序，再由冻结 allocator 建立承诺边；不得生成、编辑或补写文本。
11. 单轴评审前计算 Q/T/H、事件簇、贡献者模式和四轴配额约束下的精确匹配上界；上界不足立即停止，单轴评审调用为 0。
12. 对全部已承诺边执行 role/axis 双盲单轴评审，不得达到 12 后提前停止。
13. 在 valid edge graph 上重新计算唯一配额匹配，得到唯一最终停止码。
14. 即使供给门通过，也只允许另写 CMI 协议；本轮不运行 CMI。

任何步骤失败后禁止追加第 9 个 H、换 T、换 H、换轴、放宽阈值、润色失败文本、回收 unknown/boundary 或并入其他来源 cohort。

## 10. 四轴预分配

每个 Q–T 候选单元在语义评分前按固定 domain 和匿名 pair ID 做确定性平衡分配：

```text
role_mapping
causal_direction
temporal_order
outcome_polarity
```

P0 每轴 6，M1 每轴 24，Rtech 每轴 2。assigned axis 只存在私有 position key；语言评审、正例评审和单轴评审都看不到。失败后不重新分轴。

## 11. Singleton 语言门

### 11.1 三条并行 primary 轨

正式 HN-F1 需要：

- 模型家族 A；
- 训练路线明显不同的模型家族 B；
- 不看模型输出的人工盲评轨。

三轨使用相同冻结文本集合，但 blind ID、顺序和批次独立，彼此不能看到输出。结果取严格交集，不做多数投票、不让人工充当事后仲裁者、不用第三审升级失败项。

语言门、严格正例门和单轴门使用相互隔离的新会话；正式 HN-F1 的人工评审者也必须按阶段隔离，任何评审者都看不到上游输出。同一人工评审者跨阶段复用触发 `INVALID_HNF1_BLINDING_BREACH`；若事前只能复用，必须另立 development protocol，不能先按正式协议运行后再选择降级。

正式运行缺少任一模型家族或人工轨时触发 `STOP_HNF1_PRIMARY_REVIEW_TRACK_INCOMPLETE`。另开的不完整轨运行只能标记为 `model_assisted_development`，不得宣称 HN-F1 正式通过，也不得据此起草 CMI。

### 11.2 语言包

每项只含：

```json
{
  "blind_text_id": "opaque",
  "text": "去标识后的冻结原文"
}
```

三个数值字段均使用 1–5 的冻结量表：1 表示无法使用，3 表示需要明显补推或编辑，4 表示可独立明确理解但可有轻微瑕疵，5 表示清楚、连贯且自包含；2 为 1 与 3 之间。不得把文学性、文体偏好或标准书面语程度当作更高分依据。

禁止出现 node/source、Q/T/H、pair、axis、rank、retrieval channel、来源等级、预期通过数和其他评审结果。

### 11.3 语言结果

```json
{
  "blind_text_id": "opaque",
  "interpretability": 1,
  "coherence": 1,
  "self_contained": 1,
  "grammar_or_edit_artifact": "yes|no",
  "test_constructed_suspicion": "yes|no|uncertain",
  "reason_code": "none|fragment|grammar|contradiction|missing_context|editing_artifact|other"
}
```

通过条件不可补偿：

- 三轨的 9 个 `interpretability/coherence/self_contained` 分数全部 `>=4`；
- 三轨 `grammar_or_edit_artifact=no`；
- `test_constructed_suspicion` 只作诊断，不能变成 gold；
- 正常方言、口语或非标准语法本身不构成失败，只要无需补写即可明确理解；
- 去标识造成角色、行动、时间或结果缺失时必须失败；
- 失败文本不得修后重投同批。

T/H 边还必须满足评分前冻结的 exact compatibility matrix：

- `origin_class`、`provenance_grade`、`human_or_system_derived`、`language`、`source_medium`、`register_bin` 完全匹配；
- `length_bin` 兼容，并由同一个冻结 tokenizer 复核 token 长度相对差不超过 15%；
- 每条 primary 轨的三项 T/H 分数差均不超过 1。

评分前冻结的 language edge reconciler 在三轨 singleton 结果完整后，按 `surface_compatibility_rule` 把满足以上全部 singleton 与配对表面约束的 T/H 边机械发布为不可变 `G_surface`。它还强制源文档、事件簇和冻结贡献者模式的逐边关系；不读取故事关系、assigned axis、proposer 判断或任何结构结果。下游只获得 opaque eligible IDs 和 allowlist，不获得语言分数或失败理由。

## 12. P0 Pilot 停止门

P0 只验证来源和语言供给，不做 CMI，不形成主分析证据。

同时满足以下条件才允许进入 M1：

- 24 个 Q–T、96 个 H 的 provenance、同意、去标识和事件簇闭包全部完成；
- 至少 6 个 Q/T 均通过 singleton 语言门的候选单元，四轴各至少 1；
- 至少 24 个 H 通过 singleton 语言门；
- 在完整 `G_surface` 上，使用与第 15/18 节相同的 donor mode、全局 donor cap、事件簇唯一、精确 solver 和 tie-break 后，存在至少 6 条 Q/T/H 节点均唯一的匹配，四轴各至少 1。

任一失败触发 `STOP_HNF1_PILOT_LANGUAGE_SUPPLY`。若要修改规则，只能升协议版本并换全新来源；旧 P0 不进入未来主批。

若本门或后续任一配额门无法得到 exact 最优性证明，触发 `STOP_HNF1_TECHNICAL_EXECUTION_FAILURE`，不得以 substantive supply stop 代替。

## 12A. M1 Main 语言容量停止门

M1 的固定 96 个 Q–T 单元和 384 个 H 必须完成全部 singleton 语言评审后才读取本门结果，不能在出现 12 条时提前停止。只有同时满足以下条件才进入严格正例门：

- 至少 12 个 Q/T 均通过 singleton 语言门的候选单元，四轴各至少 2；
- 至少 12 个 H 通过 singleton 语言门；
- 完整 `G_surface` 上，使用与第 15/18 节相同的 donor mode、全局 donor cap、事件簇唯一、精确 solver 和 tie-break 后，存在至少 12 条 Q/T/H 节点均唯一的配额匹配，四轴各至少 2。

任一失败触发 `STOP_HNF1_MAIN_LANGUAGE_SUPPLY`；严格正例、H proposer 和单轴评审调用均保持 0。该匹配只是语言供给上界，不证明正例或单轴 H 成立。

## 13. 严格 Q–T 正例门

正例评审包使用第 16.1 节的统一匿名双故事形状；Q 与 T 在 `story_1/story_2` 间按轨独立平衡随机，不显示来源、proverb、axis 或预期关系。输出使用第 16.2–16.3 节的 exact review item schema；只有私有 position key 能把 `blind_item_id` 回链 Q–T 单元和位置角色。

每个故事的 `episode_story_1/2` 必须分别含非空 X/A/Y span 且 `temporal_clarity=clear`。Q–T 只有在三条 primary 轨分别满足以下全部条件时才通过：

- 四轴均为 `preserve`，没有 `unknown`；
- 至少两条有双侧逐字 span 的相互约束关系；
- 至少一条是有向 causal、temporal、control 或 mechanism 关系；
- 至少共享一个映射功能角色；
- `genericity=specific`；
- 有双侧 span 支撑的 `foil_discriminator`，能排除“努力有回报”“要谨慎”等空泛原则；
- 不需要补写原文未陈述的事件；
- 两侧事件簇独立。

任一轨出现 unknown、generic、同事件簇、缺 span、X/A/Y 不完整或时序不清均由冻结 reconciler 排除；评审者不输出自由裁量的 boundary/fail，总状态完全机械派生，不做第三审升级。

严格正例少于 12，或任一轴少于 2，触发 `STOP_HNF1_STRICT_POSITIVE_LT_12_OR_AXIS_LT_2`，H 单轴调用保持 0。

## 14. H 候选提交边界

HN-F1 是“独立成文 H 是否存在”的供给门，不是自动 selector 评测。H proposer 是醒目标记的 `axis-aware oracle_supply`：它可以看到严格 Q–T、assigned axis，以及每个 T 的 `G_surface` opaque allowlist 与冻结文本，但：

- proposer 的身份、instructions、最大调用预算和选择规则必须在语言评分前冻结；
- proposer 看不到语言分数、失败理由、single-axis 结果或预期通过数，只能读 eligible IDs 与冻结文本；
- H 必须来自评分前冻结、早于任务成文的 H 池；
- proposer 只能建立边，不得生成、改写、拼接、翻转或润色 H；
- proposer 必须对每个严格正例的全部 allowlist 边给出完整、无并列缺口的冻结排序；不能主观少看、少报或只交 1–7 条。若固定预算无法覆盖全部输入，协议在评分前即不可执行；
- 冻结 allocator 在 `每个 Q–T 单元度数<=8、每个 H 度数<=2` 下先最大化总边数，再依次最大化度数至少 1、2、…、8 的 Q–T 单元数，随后最大化冻结 proposer preference，最后按 edge hash 决定并列；
- Q、T、H 必须来自不同事件簇和源文档，并严格遵守冻结贡献者模式；allocator 的实际提交量完整报告，不用 Rtech 或其他 cohort 补足。

由人或模型做 axis-aware proposal 时，结果只能称为 `oracle_supply`。自动筛选效果必须在未来另设冻结 selector 协议，不能从 HN-F1 产率推出。

## 15. 单轴评审前的 single-axis-review 零调用上界

只读取：严格正例 whitelist、`G_surface` allowlist、已承诺边、assigned axis 私钥和隔离元数据。不得读取评分、理由、检索 channel 或 proposer 解释。

构造整数约束二部图：每个冻结 Q–T 单元容量为 1，每个 H 容量为 1，边继承 Q–T 单元的 assigned axis；同时强制源文档、事件簇、冻结 donor mode 与 `cross_donor` 20% 上限，求满足每轴至少 2 时的最大可行匹配。solver 版本、精确模式、超时策略和第 18 节 hash tie-break 必须评分前冻结；超时、error 或非精确解触发技术停止，绝不能宣称上界不足。

若不存在总数至少 12、四轴各至少 2 的匹配，触发：

`STOP_HNF1_PRE_AXIS_QUOTA_MATCHING_UB_LT_12_OR_AXIS_LT_2`

此时单轴调用为 0，不得靠评审救回上游失败边。

## 16. Role/axis 双盲单轴门

### 16.1 评审项

正例门和单轴门都使用同一匿名双故事输入；两侧位置按评审轨独立平衡随机：

```json
{
  "blind_item_id": "opaque",
  "story_1": "冻结文本",
  "story_2": "冻结文本"
}
```

每个 Q 的 T control 和所有 H candidate 全局打散。禁止并排展示 T/H，禁止显示 Q 或 candidate 位置角色、candidate role、assigned axis、来源、rank、语言分、正例结果或 proposer 说明。

同一阶段的模型调用使用新会话；正例门评审者不得收到单轴包，单轴评审者不得收到正例输出。正式人工轨按颜色批次和阶段隔离；任何人工评审者跨语言／正例／单轴阶段复用均触发 `INVALID_HNF1_BLINDING_BREACH`，不能只降级后继续正式结论。

### 16.2 EvidenceSpan

```json
{
  "start_char": 0,
  "end_char": 10,
  "quote": "原文逐字片段"
}
```

索引基于冻结显示原文的 Unicode code point、左闭右开 `[start_char,end_char)`；每个 EvidenceSpan 的 `quote` 必须非空并满足 `quote == text[start_char:end_char]`。对象只允许这三个键。span 数组在 `unknown` 时可以为空；一旦出现 span，其错位、空 quote、重复键、额外字段、NaN/Infinity 或错误类型均 fail-closed。

### 16.3 AxisCard

`mapped_role_pairs` 的每个元素严格为：

```json
{
  "story_1_role_spans": [],
  "story_2_role_spans": [],
  "role_function_story_1": "非空文本",
  "role_function_story_2": "非空文本"
}
```

两个 span 数组都至少含一个合法 EvidenceSpan。AxisCard 严格为：

```json
{
  "label": "preserve|break|unknown",
  "comparability": "comparable|not_comparable|unknown",
  "story_1_spans": [],
  "story_2_spans": [],
  "mapped_role_pairs": [],
  "relation_story_1": "",
  "relation_story_2": "",
  "contrast_code": "same_relation|role_swap|causal_reversal|temporal_reversal|polarity_reversal|missing_evidence|different_variable|ambiguous_mapping|mixed_signal|other",
  "unknown_reason": null
}
```

`unknown_reason` 的合法值仅为 `null|missing_evidence|different_variable|ambiguous_mapping|mixed_signal|other`。

规则：

- `preserve/break` 必须 `comparability=comparable`、双侧有逐字 span、`unknown_reason=null`；
- `preserve/break` 的 `mapped_role_pairs` 至少一个，两个 relation 字段非空；`preserve` 只能配 `same_relation`；
- `break` 的 code 必须与当前轴一一对应：role=`role_swap`、causal=`causal_reversal`、temporal=`temporal_reversal`、outcome=`polarity_reversal`；
- `unknown` 允许双侧 span 数组为空，但 `unknown_reason` 必须是非 null 枚举，`comparability` 不能是 `comparable`；
- `unknown` 的 contrast code 只能是 `missing_evidence|different_variable|ambiguous_mapping|mixed_signal|other`，不得使用 preserve/break code；
- 缺端点、主体不明、指标不同或只能靠补写推断时必须 `unknown`，绝不能算 `break`；
- outcome break 要求同一映射主体、同一结果指标和可比前后基线，仅方向相反；
- causal/temporal break 要求同一映射端点确有反向；
- role break 要求功能角色确实倒置，换人物、职业或场景不算；
- temporal 反转若同时改变因果方向，两张轴卡必须分别记 `break`；机械解码由“第二个 break”推出 cross-axis entanglement，不接受评审者自报的 overall 标签。

`grounded_relations` 的每个元素严格为：

```json
{
  "relation_type": "causal|temporal|control|mechanism|role|other",
  "story_1_spans": [],
  "story_2_spans": [],
  "relation_story_1": "非空文本",
  "relation_story_2": "非空文本"
}
```

两个 span 数组都至少含一个合法 EvidenceSpan。`foil_discriminator` 严格为：

```json
{
  "status": "present|absent|unknown",
  "story_1_spans": [],
  "story_2_spans": [],
  "statement": "文本"
}
```

`present` 时双侧 span 和 statement 均非空；其他状态不得被解码为通过。每侧 EpisodeEvidence 严格为：

```json
{
  "x_spans": [],
  "a_spans": [],
  "y_spans": [],
  "temporal_clarity": "clear|unclear|unknown"
}
```

只有三个 span 数组都非空且 `temporal_clarity=clear` 才能通过。每个评审结果严格为：

```json
{
  "blind_item_id": "opaque",
  "episode_story_1": {},
  "episode_story_2": {},
  "axes": {
    "role_mapping": {},
    "causal_direction": {},
    "temporal_order": {},
    "outcome_polarity": {}
  },
  "grounded_relations": [],
  "genericity": "specific|generic|unknown",
  "foil_discriminator": {},
  "requires_unstated_completion": false,
  "information_missing": false
}
```

两个 episode 值必须是完整 EpisodeEvidence；`axes` 中四个值必须是完整 AxisCard；`grounded_relations` 至少两条，其中至少一条为 causal/temporal/control/mechanism。整个对象不得出现 overall pass、boundary、fail、broken axis、额外键或自由扩展枚举。事件簇和 donor 独立性不让评审者猜测，由私有 registry 机械校验。

### 16.4 私有机械解码

- T control：三轨四轴全部 `preserve`；任一非 preserve 使该 T 的全部 H 边失效。
- H candidate：三轨都必须恰好在私有 assigned axis 为 `break`，其余三轴为 `preserve`。
- 任一 `unknown`、第二 break、信息缺失、补写、generic、`foil_discriminator.status!=present`、关系数不足、事件簇或 donor mode 不合规均失败。
- 三轨取严格交集，不投票、不求平均、不做裁决升级。

## 17. 批处理与泄漏控制

- A/B/human 三轨使用不同 blind ID、顺序和位置键；
- 用 source–candidate 二部图的确定性边着色分批，使单个批次内 Q 和 candidate 均不重复；
- 模型轨每个颜色批次使用全新无历史上下文；
- 人工轨的 reviewer-group assignment 在包生成前冻结，每项恰好一份人工判断；同一人工评审者在单轴阶段最多看到同一 Q 一次、同一 candidate 一次，同一 donor 在一个批次最多出现一次。违反时该项 human track 无效，不得进入正式匹配；
- 评审者看不到其他批次产率、另一评审输出、预期通过数和停止状态；
- instructions 必须明示 candidate 可能在 0、1 或多个轴上为 preserve/break/unknown，不暗示存在“一条 T 加若干恰好单轴 H”；
- 客观 transport error 或截断仅在原输出未被人工读取、自动 validator 判为 schema-invalid 时允许以相同输入完整重跑至多 1 次；保留全部 raw receipt，只接收首个完整 schema-valid response，不能局部补答、换 prompt 或在多个有效答案中挑选。两次均无有效响应则触发 primary track incomplete。
- positive/H proposer 适用同一“一次技术重试、首个完整 schema-valid response”规则；完整排名缺边、rank 不连续或第二次仍无效触发 `STOP_HNF1_TECHNICAL_EXECUTION_FAILURE`，不得换 proposer 或删边继续；
- allocator 和 exact solver 只能在无有效 receipt 的机器崩溃下用相同二进制、版本、输入字节和参数重跑至多 1 次；首个 exact 结果立即冻结。`solver_status=error`、超时、非精确解、两次结果不一致或 allocator 无合法输出都触发同一技术停止码，不能解释为供给失败。

## 18. 最终唯一配额匹配

在 valid edge graph 上求解：

```text
每个 Q <= 1
每个 T <= 1
每个 H <= 1
每轴 >= 2
最大化总匹配数
```

求解器同时强制 Q/T/H 节点、事件簇和源文档唯一。`cross_donor` 要求每条边三位 donor 互异；对候选基数 M，每位 donor 最多出现在 `floor(0.20*M)` 个选中三元组中。`within_donor_ecological` 要求每条边三位 donor 相同且不施加 20% 人数上限，但必须按 donor 分层报告。混合 donor 结构不合法。

并列时先求满足全部配额与 donor 约束的最大基数 `M*`，再按 `SHA256(protocol_domain|Q_hash|T_hash|H_hash)` 升序逐边检查：若强制纳入该边后仍存在包含此前已锁定边、规模为 `M*` 且满足全部约束的 completion，则锁定纳入，否则锁定排除。最终得到唯一的字典序最小最优集；不得使用语言分、结构分、理由、检索 rank 或 proposer 偏好。

最终通过门：

- 总匹配数至少 12；
- role、causal、temporal、outcome 各至少 2；
- Q/T/H 节点和事件簇均唯一；
- 来源 cohort 不混合；
- 冻结贡献者模式满足；
- 三条 primary 评审轨完整。

不足触发 `STOP_HNF1_SINGLE_AXIS_MATCH_LT_12_OR_AXIS_LT_2`。

达到门槛只输出带作用域的结论：

```text
cohort=<HN-F1-N|HN-F1-P|HN-F1-A|HN-F1-OB>
provenance_stratum=<N3|N2|P1|A0|N1>
donor_mode=<cross_donor|within_donor_ecological>
unique_donor_count=<integer>
hnf1_supply_feasible=true
separate_cmi_protocol_may_be_drafted=true
cmi_authorized=false
```

`hnf1_supply_feasible=true` 只适用于上述被冻结的 cohort、provenance stratum 和 donor mode；尤其 within-donor 或单 donor 结果不得外推为一般自然来源 HN-F1 可行。

## 19. 停止码

| 停止码 | 精确触发 |
|---|---|
| `STOP_HNF1_PROTOCOL_NOT_EXECUTABLE` | 第 22 节任一必填项在首条语义评分前未冻结 |
| `STOP_HNF1_SOURCE_OR_CONSENT_INVALID` | cohort、许可、同意或来源时间的系统性依据无效，无法由预注册技术替补修复 |
| `STOP_HNF1_SOURCE_CAPACITY_LT_FIXED_BATCH` | 评分前固定 roster 不足，或一对一 Rtech 映射缺失／耗尽 |
| `STOP_HNF1_PRIMARY_REVIEW_TRACK_INCOMPLETE` | 正式运行缺模型家族 A、不同家族 B、独立人工轨中的任一轨，或任一 required item 在唯一技术重试后缺少任一轨 schema-valid 结果；不得删项继续 |
| `STOP_HNF1_TECHNICAL_EXECUTION_FAILURE` | positive/H proposer 排名不完整、allocator 失败、exact solver error／超时／无最优性证明，或其他冻结执行器无法产生唯一合法 receipt |
| `STOP_HNF1_PILOT_LANGUAGE_SUPPLY` | P0 未同时达到第 12 节的 6/1 配额语言门 |
| `STOP_HNF1_MAIN_LANGUAGE_SUPPLY` | M1 未同时达到第 12A 节的 12/2 配额语言门 |
| `STOP_HNF1_STRICT_POSITIVE_LT_12_OR_AXIS_LT_2` | 严格 Q–T 少于 12 或任一轴少于 2 |
| `STOP_HNF1_PRE_AXIS_QUOTA_MATCHING_UB_LT_12_OR_AXIS_LT_2` | 单轴调用前的精确配额匹配上界少于 12 或任一轴少于 2 |
| `STOP_HNF1_SINGLE_AXIS_MATCH_LT_12_OR_AXIS_LT_2` | 有效边最终唯一配额匹配少于 12 或任一轴少于 2 |
| `INVALID_HNF1_BLINDING_BREACH` | role、axis、来源角色、上游结果或位置键泄漏给不应看到它的评审者 |
| `INVALID_HNF1_RESULT_DEPENDENT_PROTOCOL_CHANGE` | 读取结果后改样本、prompt、阈值、轴、proposer、solver 或替补规则 |

若同时出现多个问题，`INVALID_*` 优先并使整轮失效；否则按第 9 节不可逆顺序输出最早阶段的失败码，同阶段按本表从上到下取唯一停止码。技术停止只表示本轮无结论，不能当作供给门未通过。所有并发问题仍需在失效记录中完整列出。

任一停止都是数据／流程／供给结论，不得表述成 Spark、自动 selector 或 CMI 的效果失败。

## 20. 冻结链和最小产物

所有 JSON 必须使用 UTF-8、拒绝 duplicate key、NaN 和 Infinity，并执行 exact schema/type/enum 校验。

### 20.1 最小 exact artifact contracts

每个 pack／response 顶层只能有以下五个键，全部必填：

```json
{
  "schema_version": "评分前冻结的常量",
  "protocol_sha256": "64位小写十六进制",
  "input_commitment_sha256": "64位小写十六进制",
  "producer_commitment_sha256": "64位小写十六进制",
  "items": []
}
```

同一 artifact 内主键必须唯一，item 顺序按该 artifact 评分前冻结的主键字典序；只有 `language_result` 和 `pair_review_result` 强制与对应 review input 的 ID 集完全相等。派生 artifact 按各自 machine schema 对上游全集做 completeness 校验。禁止额外顶层键和 item 键。各 `items` 元素的 exact contract 为：

| artifact | item 必填键与类型 |
|---|---|
| `language_input.v1` | `blind_text_id:string, text:string(nonempty)` |
| `language_result.v1` | 第 11.3 节七个键；三个分数字段必须是 `integer 1..5` |
| `positive_roster.v1` | `qt_unit_id:string, q_record_id:string, t_record_id:string, proposer_rank:integer>=1` |
| `rtech_slot_commitment.v1` | `reserve_slot_id:string, reserve_slot_type:qt_unit\|h_node, replaces_formal_slot_id:string, allowed_activation_reasons:array[enum]`；数组只能含四种预注册技术原因 |
| `rtech_activation_receipt.v1` | `reserve_slot_id:string, activated:boolean, actual_reason:file_corrupt\|withdrawn_pre_score\|duplicate_pre_score\|manifest_invalid\|null, evidence_sha256:string\|null, decided_at:string, target_pack_sha256:string, any_semantic_result_read:false` |
| `pair_review_input.v1` | `blind_item_id:string, story_1:string(nonempty), story_2:string(nonempty)` |
| `pair_review_result.v1` | 第 16.2–16.3 节的完整 review item；四个 axis key 必须恰好各出现一次 |
| `blind_position_key.v1`（私有） | `canonical_item_id:string, stage:positive\|single_axis, track:model_a\|model_b\|human, blind_item_id:string, story_1_role:Q\|T\|H, story_2_role:Q\|T\|H, candidate_role:T\|H, assigned_axis:string\|null`；每个 canonical item×track 恰一行 |
| `surface_edge_allowlist.v1` | `surface_edge_id:string, qt_unit_id:string, h_record_id:string`；不得含分数或理由 |
| `h_proposer_rank.v1` | `qt_unit_id:string, h_record_id:string, rank:integer>=1`；每个 T 的 allowlist 必须无遗漏、rank 连续且无并列 |
| `committed_edge.v1` | `canonical_edge_id:string, qt_unit_id:string, h_record_id:string, allocator_order:integer>=1`；blind ID 只能来自私有 position key |
| `matching_result.v1` | `solver_status:exact\|error, max_cardinality:integer>=0, axis_counts:object, selected_edge_ids:array[string], solver_version:string, tie_domain:string`；`axis_counts` 只能含四个冻结轴且值为非负整数 |

positive proposer、语言 edge reconciler、H allocator、上界 solver 和 final matcher 的输入／输出 JSON Schema 文件仍必须作为 execution amendment 的独立、机器可校验文件冻结并记录 SHA-256。本表不是允许边运行边补 schema 的例外；机器 schema 任一缺失即 `STOP_HNF1_PROTOCOL_NOT_EXECUTABLE`。

评分前冻结：

- protocol 与 amendments；
- source lock、denylist 和匿名 roster；
- P0/M1/Rtech 分层与轴 key；
- 每轨私有 blind position key schema、实际 key commitment 与角色解码器；
- reviewer instructions；
- language/positive/edge pack builders；
- language edge reconciler、strict reconcilers、H allocator、upper-bound solver 和 final matcher；
- blind ID、order、batching 和 tie-break domains。

每阶段使用：

- 内部 registry；
- 不含语义角色的 blind packs；
- 私有 position key；
- pre-score commitment 和 root anchor；
- 两模型家族与人工原始结果；
- 在任何人读取语义结果前冻结的 reconciler root anchor；
- 只含 eligible IDs/edges、不含分数与理由的下游 whitelist；
- 原子新建、拒绝覆盖的结果目录。

脚本必须从同一字节快照做 hash 与解析，并在原子发布前重新核验全部输入。实现或盲法失败时，保留旧文件和哈希、写失效记录、升版本修复；不得静默覆盖。

## 21. 报告规则

主要报告原始计数和约束匹配：

- 来源等级、贡献者和事件簇数量；
- Q/T/H singleton 合格数；
- 严格正例数；
- H 候选边数和语言合格边数；
- positive proposer、语言评审、严格正例、H proposer、单轴评审各阶段的模型／人工调用数与技术重试数；
- 唯一 Q/T/H 数；
- 每轴有效匹配数；
- 最大配额匹配总数；
- 排除原因分布；
- 模型家族、人工轨和不同来源 cohort 分层。

可选 95% Wilson 区间只能作描述，不能控制 reserve、判定通过或充当显著性确证。来源或贡献者聚类时必须说明普通二项区间会低估依赖性。

禁止报告：

- Spark 或 CMI 有效／无效；
- 自动筛选准确率；
- p 值显著性；
- 下游帮助度；
- natural_observed 总体发生率；
- 多 seed 等于独立复制；
- human-reviewed 等于 human gold。

## 22. 执行前仍需项目所有者填写并批准

本协议在以下字段冻结前不可执行：

1. HN-F1-N 的具体来源、许可、收集时间窗、纳入全集，以及 `cross_donor` 或 `within_donor_ecological` 贡献者模式；
2. 私密原文的受限研究目录与数据管理员；
3. 是否允许把去标识文本发送给哪些具体服务商／endpoint／模型版本，以及训练使用、日志／保留期、处理地域和转包商限制；
4. 模型家族 A、模型家族 B 的具体版本，以及人工评审人数、reviewer-group assignment 和跨阶段隔离；
5. token 计数器、near-duplicate 阈值、事件簇手册版本，以及 `source_medium/register_bin/length_bin` compatibility matrix；
6. positive proposer 与 H proposer 各采用人工 oracle 还是冻结模型，以及各自的 instructions、排序规则、最坏规模预算和 allocator 版本；
7. P0/M1/Rtech 的实际来源 roster、donor 重复上限和 exact 技术替补 slot map；
8. 所有机器 JSON Schema、builder/reconciler/solver 版本、精确求解设置和 tie-break domain；
9. 审计者、reconciler 执行者和最终解码者的隔离安排；
10. 研究副本保留期限、撤回和销毁规则；
11. 高风险材料许可、PII 复核和外部服务商 allowlist。

这些内容必须在读取第一条语言评分前写入 amendment 和 root anchor。任何一项未填，停止码为 `STOP_HNF1_PROTOCOL_NOT_EXECUTABLE`。

## 22A. Stage -1 执行补充协议（未执行）

第 22 节已进一步展开为 [OB HN-F1 Stage -1 执行补充协议](./OB_HN-F1_Stage-1执行补充协议_2026-08-03.md)。补充协议当前状态为 `DRAFT_FOR_OWNER_COMPLETION`、`execution_authorized=false`，只定义无正文的来源与固定容量预检，不授权收集私密正文、读取真实 OB vault、运行模型、positive proposer、语言／结构／单轴评审或 CMI。

固定容量账本为 P0 `144`、M1 `576`、Rtech `48`，共恰好 `768` 个跨批次不复用的事件节点；这是未来正式 roster 的必要供给量，不是 768 位贡献者，也不是已观察到的来源数量。按每位 donor 最多进入 `floor(0.20M)` 个最终三元组的规则，P0 与 M1 的数学硬下限各为 15 位，Rtech 为 8 位，因此跨批次硬下限是 38 位；少于 38 位必然不可行。P0/M1/Rtech 的 `18/18/8=44` 仅是待批准的保守规划阈值，用于不依赖额外有效匹配放宽 donor cap 的最低通过路径，并非数学必要条件。S1-A 的假名化 metadata-only exact upper-bound solver 只能判断固定设计是否已被元数据证明不可能；即使上界通过，也不能代替 proposer 后的 actual roster 与 Rtech map 验证。

补充协议把 `source_universe_lock`、`allocation_rule_lock` 和 proposer 后的 `actual_roster_commitment` 明确分开；桌面安全投影只允许批级批准、聚合计数、metadata-only receipt 和状态，不保存逐条 record/donor/document/event/time-block 关联。2026-08-03 已按继续推进的方向把 `HN-F1-N / N3 / cross_donor` 记录为 `PROVISIONAL_OWNER_DIRECTION`；`natural_observed` 仍是协议固定定义。该方向保留 38 位数学硬下限，并把 44 位记录为 `PROPOSED_UNAPPROVED` 保守规划阈值；方向可撤回，不等于 owner approval、freeze、数据处理许可或执行授权。

当前没有导入或统计任何真实来源，无正文盘点工作簿也尚未生成或填充。招募截止、收集窗口、受限目录、角色隔离、duplicate 阈值、同意与外部服务范围、保留／撤回、metadata-only solver 实现和不可覆盖存储位置仍未冻结；唯一合法状态是 `STAGE1_NOT_READY_METADATA_INCOMPLETE`，不是 HN-F1 供给通过。

在 38 位硬下限、44 位拟议保守规划阈值与 768 个事件节点均尚无现实可得性证据时，补充协议新增了 `HN-F1-PAS`（Pre-S1-A Source Availability Screen）无正文来源可得性普查。PAS 位于 S1-A 之前，只在另行授权后收 ACL 受限的假名化枚举元数据，以及联合唯一节点与事件／文档／时间块诊断边际的预冻结数量档位；一人一 donor 由独立招募台账机械去重，不收正文、不分配 Q/T/H、不调用模型。它以协议有效乐观上界和单 donor 粗容量 cap 判断硬 no-go，以保守联合下界判断是否达到拟议规划门；38–43 位、招募 frame 不可穷尽、规则未冻结或上下界跨门槛时只能输出 `PAS_INCONCLUSIVE`。

当前文档化工作流记录 `hnf1_pas_status=PAS_NOT_AUTHORIZED`、`pas_execution_authorized=false`、`donors_contacted=0`、`metadata_records_collected=0`；现实全局零状态仍需管理员另行签署 attestation。PAS 不能输出任何 HN-F1 正式停止码、`PASS` 或 `READY`；只有未来取得 `PAS_PLAUSIBLE_NOT_VERIFIED`，才值得继续补齐 A01–A21、A28、A32，即使如此也不等于 S1-A 获准或通过。

补充协议第 20 节现已加入 PAS01–PAS14 可审批冻结包、PAS10 有效计数档位草案、PAS12 exact 粗分区合同、角色／存储隔离、撤回与小单元抑制规则。当前仓库已有未冻结的 PAS10 候选归一化器与 PAS12 候选数学核，但没有完整 PAS 治理状态机、正式 test receipt、PAS13 隐私投影、PAS14 hash-bound authorization、现实执行附件或任何真实数据运行；正式字段仍是 `DRAFT/PROPOSED/REQUIRES_REAL_ADMIN_INPUT/UNSET`。“继续”只授权完善候选实现与文档，不构成 donor 联系或 PAS 执行授权。

## 23. 与后续阶段的关系

HN-F1 通过后，下一步仍不是产品实现，而是另写一个自包含的 CMI 数据门协议，使用新的任务输入、独立 renderer 和 N/T/H 条件验证“相关远记忆是否改善当前回答”。HN-F1 文本、评审理由和角色标签不能成为未来自动 selector 的训练泄漏。

如果 HN-F1 失败，应发布来源／语言／结构供给的负结果并停止；不能用同一批次反复修文本、改阈值或增加样本，直到得到想要的 12 条。

## 24. 一手来源

- [ARN: Analogical Reasoning on Narratives](https://aclanthology.org/2024.tacl-1.59/)
- [ARN v1 Zenodo 数据记录](https://zenodo.org/records/11044026)
- [ePiC: Employing Proverbs in Context](https://aclanthology.org/2022.acl-long.276/)
- [AnaloBench](https://aclanthology.org/2024.emnlp-main.725/)

这些来源只用于说明 HN-F0 的历史与 denylist，不能进入 HN-F1-N 主分析。
