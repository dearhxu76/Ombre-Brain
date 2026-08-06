# OB Spark-DSR-CT：发现—盲封—复制的对照迁移方案（含 SOS-PAR／DSPT／WIT-VS／Spark-CUT）

> 文档状态：研究提案；仅 2026-08-03 的 Spark-Gold 补充盲筛与 Hard-Negative 开发门已获单独批准并执行，完整方法仍待审<br>
> 初稿日期：2026-08-01；本轮前沿检索与重构日期：2026-08-02；开发门实测更新：2026-08-03<br>
> 适用范围：Ombre Brain 的 Spark／灵感候选自动筛选<br>
> 当前结论：DSR-CT 是一个比“同一来源自验证”更严格、也更接近真正灵感发现的可证伪主假设；尚未证明有效、优于基线、可产品化或世界首创<br>
> 实施状态：未修改代码、未增加 MCP 工具、未增加环境变量、未写入任何真实记忆库；仅在独立桌面实验目录生成 `test_data` 研究产物<br>
> 修订说明：主核心升级为 **Discover–Seal–Replicate with Contrastive Transport（DSR-CT，发现—盲封—复制的对照迁移）**；其中 Replicate 仅指事件簇外验证，不等同独立科学复制；SOS-PAR 降为验证记忆的结果防火墙与预序评分协议；新增目标条件化见证透镜、自然四元对照、真实多切点响应签名和 **Dual-Sealed Prequential Transport（DSPT，双盲封预序迁移）** 的 prospective target shadow<br>
> 评价隔离：主效应中的自动选择 \(I\) 与方法盲人工 gold \(Y\) 由隔离流程分别产生；人工标签不得回流候选选择、排序、panel pass、证书或 replay winner<br>
> 规范优先级：本轮 DSR-CT 的“发现证据／验证证据事件级隔离、自然对照、目标 shadow 不回流当前调用、固定覆盖率选择风险”高于旧 SOS-PAR／WIT／CUT 条款；发生冲突时以本轮条款为准<br>
> 当前批准边界：已批准的关系级补批与 Hard-Negative 开发门已在 CMI 前停止；本文不构成代码、比较器、生产接入、真实 shadow 或持久化批准，任何后续新数据批次或实现仍需再次批准

## 1. 执行摘要

现有 60 例开发性报告只支持一个狭窄判断：**人工选中的远类比材料可能优于等长干扰材料，但这没有证明自动选择器有效。** 旧方案已经识别了向量表层捷径和来源结果泄漏，却仍留下两个更根本的问题：

1. 如果在候选出现前就冻结焦点机制，系统只能验证已有想法，发现不了由记忆带来的新机制；这更像审计，不像 Spark。
2. 如果提出机制的发现记忆同时也是验证该机制的唯一来源，即使其结果被盲封，也仍是“同一事件拟合映射、同一事件结果验证”的单事件自证。一个三分类结果最多只有 \(\log_2 3=1.585\) bit，远不足以约束灵活的自然语言解释。

因此，新版不再把“同一条来源记忆的结果盲预测”当作 Spark 的最终基础。新版的首要问题是：

> 一条结果盲的发现记忆能否提出当前查询中原本没有的、带原文见证的候选机制；该机制和映射冻结后，能否在**从未参与发现、事件簇外、结果仍被盲封**的自然验证记忆上，同时找到表层远的机制见证、拒绝表层近的假朋友、识别关键边界破坏，并优于最不利竞争机制、强查询无关预测器以及“可见 query／Need 但看不到 discovery memory”的对照？随后，它在目标侧历史 rolling-origin 与完全不可见的未来 shadow 中是否仍有剩余预测迁移技能？

这一定义命名为 **Spark-DSR-CT**：

- **Discover**：发现 seed 可以真正提出一个新候选机制 \(H_C\)，但 seed 永远不能为自己提供确认性证据；
- **Seal**：机制卡、角色映射、方向轴、边界条件、最不利 rival 集、验证检索规则和概率预测全部在验证结果不可见时冻结；
- **Replicate**：只有 discovery event 之外的自然验证记忆能为候选提供来源侧事件外验证证据；该命名不把同一系统内验证夸大为独立科学复制；
- **Contrastive Transport**：验证对象不是“像不像”，而是一个由 `analogue / bridge / foil / null` 构成的自然对照集合，以及同一冻结机制在其中应出现的差异—不变—反转响应签名。

对论文与实验主线，最窄且可检验的定位是 **non-self-confirming, replication-conditioned memory selection**：发现记忆可以提出机制，但不能自证，自动选择必须由事件簇外自然结果验证。DSR-CT 是这一核心选择协议；DSPT 只是来源侧通过后的外部迁移验证扩展，不能用尚未实际完成的 future shadow 为 DSR-CT 的筛选效果或首创性背书。自然四格、hash、proper score、rolling-origin 与 shadow 都是验证设计组件，不是单独原创点。

### 1.1 新 Spark 的基础是什么

新版的基本对象不再是两段文本间的距离，而是一个**能否在与 discovery seed 不相交的事件簇上经受验证、同时能被自然反例推翻的迁移假设**：

\[
\mathcal H_C=(M_C,\tau_C,\omega_C,B_C,\mathbf p_C,\Pi_C)
\]

- \(M_C\)：发现 seed 提出的候选机制；
- \(\tau_C\)：来源角色到目标角色的冻结映射；
- \(\omega_C\)：行动／条件变化与观察轴的冻结变换；
- \(B_C\)：适用边界、必要条件与明确阻断条件；
- \(\mathbf p_C\)：对事件簇外 validation panel 中各真实封存结果的完整概率预测；
- \(\Pi_C\)：每个节点、关系、条件和方向到原文 span、时间戳、ACL 与 hash 的谱系。

类比“近”不再表示 cosine 大，而表示：在表层、主题、时间和实体被改变后，同一机制卡仍在事件簇外自然材料上获得可重复的预测优势；类比“远”也不再是值得奖励的标签，而只是**低表层重叠条件下仍通过事件外验证门**的观察属性。越远不自动越好，新颖性和多样性只能在已通过真实性与适切性门的集合内排序。

### 1.2 发现与验证必须分离

规范流程为：

~~~text
显式 inspiration=true
→ 冻结 Need Frame 与非行动性的 informational-need probes
→ 结果盲宽召回发现 seed
→ seed 产生新的、带原文见证的 MechanismCard
→ 冻结机制卡、轴、边界、最不利 rival 集和验证检索策略
→ 从不同 event_cluster 检索结果盲验证记忆
→ 组成自然 analogue／bridge／foil／null 对照集
→ 一次揭封验证结果并计算竞争机制的预序 regret
→ 通过历史 rolling-origin 的目标侧迁移重放与选择后风险校准
├─ 可见研究臂：最多输出一个可忽略、可核查的诊断问题；其未来结果不算自然 shadow
└─ shadow 臂：本次完全不输出；未来自然结果只更新后续研究证据
~~~

以下隔离是硬约束：

- discovery seed、验证记忆、校准集和锁定确认集按 `owner / event_cluster / time` 隔离；
- seed 的结果即使存在也不能进入该机制的确认性统计量，只能在预注册的探索性附录报告；
- 缺少与 discovery seed event-cluster 不相交的自然验证事件时，状态只能是 `seed_only_unverified`，不得注入当前模型上下文；
- LLM 生成的换皮故事、反事实结局或机制破坏文本只能是 `metamorphic_only` 测试，永远不能充当真实正证据；
- 多个时间切点属于同一事件簇，联合计一个相关响应签名，不能伪装成多个独立样本。

### 1.3 双视图：同时保住结果防火墙和目标条件化性能

单一查询无关 CEF 有利于防泄漏，却可能丢失真正与当前目标有关的可迁移细节；直接读取完整记忆做目标条件化抽象又会重新打开结果泄漏。新版将二者拆成两个权限不同的视图：

~~~text
BlindSourceLedger（查询无关、不可变、结果盲）
- source_id / event_cluster_id / cutoff
- pre-outcome spans / actors / context / action / timing
- provenance / ACL / forbidden-field hash

WitnessedTCA(Q, BlindSourceLedger)（查询条件化、临时、结果盲）
- transferable_principle / role_bindings / conditions
- predicted mechanism / strongest rivals / boundary breakers
- every node and edge -> exact source spans
~~~

`WitnessedTCA` 只能从 `BlindSourceLedger` 生成，不能读取来源结果、标题中的结果代理、全文摘要、后续文本、结果可见 embedding 或缓存；它在揭封前 hash 冻结，不写回记忆真源，并必须通过 target-swap、span-shuffle、结果 canary 和独立生成器一致性检查。这一双视图吸收目标条件化抽象的性能优势，但不把后见信息带回检索器。

### 1.4 召回增强只能推演“信息需要”，不能替模型规划

为减少低语义重叠的真类比漏召回，可以从 Need Frame 临时生成不超过预冻结预算 \(K_{probe}\) 的 **Need-Path probes**，例如“什么约束可能使当前机制失效”“什么观察能区分两个 rival”“当前缺的是边界条件、时序还是反馈证据”。\(K_{probe}\) 必须由独立 calibration 上的 recall—噪声—成本消融选择，不把 PGR 数据集中的数字误写成通用 probe 常数。这些 probe：

- 只描述可能需要查找的证据类型，不预测用户下一步，不生成行动计划、拒绝理由或 permit；
- 只用于结果盲 BM25／dense／稀疏检索的高召回并在本次调用后销毁；
- 不能成为记忆、共激活边的关系类型或模型必须遵守的认知步骤；
- 必须与原查询直召回并行，单独做增量消融，防止“推演得越多、无关记忆越多”。

向量、BM25、RRF 和多样性采样仍只负责不漏掉候选。它们不能决定机制、正反、真假朋友或发布资格。

### 1.5 正／反如何确定：用真实响应签名，不用向量方向

每个真实验证事件按结果不可见时冻结的行动轴与结果轴形成：

\[
r_j=(\operatorname{sign}\Delta A_j,\operatorname{sign}\Delta Y_j,
\text{window}_j,\text{boundary}_j,\text{provenance}_j)
\]

经 \(\tau_C,\omega_C\) 映射后，事件只允许得到 `aligned / opposed / zero / mixed / unknown`。多个真实切点形成联合签名 \(\mathbf r=(r_1,\ldots,r_m)\)：

- **同向**：所有可比较且非零的冻结轴在允许误差带内同向，且没有关键轴反转；
- **逆向**：至少一个预注册关键轴稳定反转，其他关键轴不与该结论冲突；
- **mixed**：不同关键轴或不同切点给出实质冲突；
- **unknown**：缺轴、缺边界、观察窗不成立、来源污染或 rival 无法区分。

`opposed` 只描述冻结坐标上的观察方向，不表示价值上的反对，更不表示“应该反着做”。签名只能由真实、有时间戳的原文观察构成；LLM 插值、合成结果和挑选命中的切点无效。自然变化只是 `intervention_like_change`，不能冒充 `do()` 因果干预。

### 1.6 自然四元对照与竞争机制 tournament

对每个冻结机制，验证检索器尝试形成变量长度的自然对照集合：

- `analogue`：机制和必要条件应成立，但表层距离未达到冻结的 `far` 阈值；
- `bridge`：表层远、机制见证应保持，是“真远类比”的直接检验；
- `foil`：表层近，但关键机制操作或必要条件在自然事件中被破坏；
- `null`：机制无关、零变化或结果基率匹配的控制；必须在揭封前冻结 `null_subtype=no_relation|zero_change|base_rate_matched` 并分层报告，三者不能用一个容易的平均值互相遮蔽。

每个候选同时冻结 \(H_C\) 与有限最不利 rival 集：`surface association / temporal trend or base rate / nearest alternative mechanism / no stable relation`。另冻结两个信息权限不同的 outcome controls：强 query-free predictor，以及可见 query／Need 与 validation 前因、但看不到 discovery memory／MechanismCard 的 `query-aware-no-discovery` predictor。令 \(\mathcal K=\mathcal R\cup\{qfree,qaware\text{-}no\text{-}discovery\}\)。所有假设在同一 panel 上提交概率，不再把竞争机制混成一个容易被基率权重救活的 `p_SOS`。来源结果预测技能与“这个机制是否适用于该事件”必须拆开检验。正例 cell 的核心结果证据是事件簇级 paired proper-loss regret：

\[
G_+=\min_{k\in\mathcal K}\;\overline{L(p_k,Y)-L(p_C,Y)}_{analogue+bridge},
\qquad
G_B=\min_{k\in\mathcal K}\;\overline{L(p_k,Y)-L(p_C,Y)}_{bridge}
\]

\[
\widehat z_C(v)=\mathbf 1\{a_C(v)\ge \tau_a\},
\qquad
S_{contrast}
=\operatorname{TPR}\!\left(\widehat z_C;V_A\cup V_B\right)
-\operatorname{FPR}\!\left(\widehat z_C;V_F\cup V_N\right)
\]

这里 \(L\) 越小越好，因此 \(L(p_k,Y)-L(p_C,Y)>0\) 才表示焦点机制优于相应 rival／control；\(G_B\) 是不可被较容易的近 analogue 补偿的 bridge-only 门，只能在 confirmation 的 query／event blocks 聚合判断，单候选值仅作审计特征。\(a_C(v)\) 是**结果盲、揭封前冻结**的机制适用概率，\(\tau_a\) 是只由 discovery／calibration 冻结、不得在 confirmation 或 null panel 内重选的二值发布阈值，独立 gold cell 对模型隐藏。除阈值化的 \(S_{contrast}\) 外，还必须报告 \(a_C\) 对 gold cell 的 Brier／log loss 与校准，避免用阈值掩盖概率失真。foil／null 上不要求 \(H_C\) 的 outcome loss 击败本来就可能正确的 \(H_{null}\)，而要求低适用率、低假发布和边界校准。规范门要求 \(G_+\) 与 \(G_B\) 均超过各自冻结实质界、\(S_{contrast}\) 超过冻结实质界，并在 outcome-swap、target-swap、leave-one-event-cluster-out 与独立实现上稳定。不得用一个加权总分让 novelty、表层距离、近 analogue 或 LLM 自报置信度补偿 bridge 失败与关键反例失败。

### 1.7 DSPT：历史时间外重放与 prospective 第二封存

来源侧事件外验证仍不能证明该机制会迁移到当前目标。新版增加 **Dual-Sealed Prequential Transport（DSPT）**，但严格区分历史重放和真正第二封存：

1. 第一封是 validation memories 的真实来源结果 ACL；
2. 历史 rolling-origin 是在已有历史上恢复伪时点的信息边界，只能称时间外重放，不能称第二封存；
3. 第二封只指真实 prospective shadow：提交时目标结果尚未发生、候选从未显示，未来自然结果才首次揭封。

离线先对历史时间线做 rolling-origin 重放：在时点 \(t\) 只能读取 \(t\) 以前的材料，冻结 `TargetObservationContract`，再揭示 \(t\) 之后真实记录。只有历史重放显示超越 base／rival 的剩余 proper-score 技能，才允许单独审批真实 shadow。真实 shadow 必须满足：

- 候选、预测及其诊断在评估窗内对当前模型和用户完全不可见，避免 Spark 展示改变后续行为；
- 只观察自然出现、预先定义、可审计的结果；不实施现实干预，不为获取标签诱导用户行动；
- 目标缺失、失访或观察窗改变记为 `target_unscoreable/censored`，不当作零或错误；
- 当前调用不等待未来结果；未来揭封只能更新**后续**研究中的 transport reliability，不能反向决定本次输出；
- shadow 只为 prospective 预测迁移提供前瞻证据；它既不证明因果或独立科学复制，也不证明把 Spark 显示给模型后有帮助。

目标侧真正的帮助度必须另用人工盲评或获批的随机化 `no-memory / equal-length distractor / DSR candidate` 比较。预测迁移、灵感适切性和下游帮助是三个不同终点，不能互相替代。

### 1.8 输出、校准与主终点

允许的输出最多是一条可忽略诊断，例如：

> “独立历史对照更支持该机制而非冻结 rival；可自行检查：当前情境是否具备边界条件 B？”

它不能写成事实结论、建议、计划、拒绝、permit、信念、情绪或记忆可信度裁决。`inspiration=false` 或缺省必须完全旁路；不新增 MCP 工具。

确认性自动筛选的**唯一主效应**改为：在预冻结最小覆盖率 \(c^*\) 上，完整端到端候选的 useful-far precision 相对最强同预算基线的配对改善 \(\Delta_{PF}(c^*)\)。DSR 自身的绝对 precision 下界是不可补偿的安全 floor，不是第二个可择优“主终点”。`useful-far` 由方法盲人工 gold 定义，不能包含“通过 DSR 证书”这一方法自身条件。默认研究值可设 \(c^*=10\%\)，但锁定确认样本量必须由预注册功效／精度模拟决定；30 个发布单元只能作 feasibility floor，不能支撑 5% 风险或 0.80 precision 下界声明。现有 60 例只能做可行性和探索性风险—覆盖曲线。空结果不受惩罚为最大损失，也不允许通过全空赢得主效应；precision、coverage、AURC、空结果率和所有失败必须同时报告。

选择后 conformal／FDR 只在事件簇级 exchangeability、有效 nonconformity score 和独立 calibration 条件可辩护时使用。否则只能报告经验 risk–coverage 与 rolling shadow 风险，不能宣称“95% 有限样本保证”；LLM softmax、自报概率或随意构造的分数不能冒充 p-value、Bayes factor 或 e-value。

### 1.9 新颖性边界与现实预期

本轮检索已经找到大量组成项先例：两阶段召回、结构映射、目标条件化抽象、机制对齐、多类比确认、未来步骤引导的记忆召回、个人跨域 schema、结果盲预序评分、因果抽象、hard-negative、主动信息增益、选择性预测和 conformal 风险控制都不是新发明。

截至 2026-08-02，在本轮**非系统检索所覆盖的一手来源与关键词级专利初筛**中，尚未定位到一个同时明确具备以下三项的个人自然语言长期记忆协议：

1. 提出机制的 discovery event 永不用于确认该机制；
2. 在任何验证结果首次可见前，全 run 冻结机制、material rivals、事件簇外自然验证 panel、适用概率和结果概率，再由真实结果 ACL 统一揭封；
3. 来源事件外验证和历史 rolling-origin 之后，另以 never-shown prospective target outcome 计分，且该结果只影响后续版本。

其中第 1–2 项才是 DSR-CT 的核心方法贡献假设；第 3 项是 DSPT 外部迁移验证扩展，不参与 \(\Delta_{PF}\) 的算法新颖性或本次筛选效果归因。上述组合只是一项**较强、可证伪的两层系统协议假设**。四元命名、hash、proper score、目标条件化抽象、历史重放和 shadow 本身都不承担首创性；该判断也不是系统综述结论、世界首创证明、专利新颖性意见或自由实施分析。正式论文、宣传或知识产权动作前，仍需补做向前／向后引用链、非英文数据库、专利分类检索和独立专业复核。

近期执行顺序改为：

1. 阶段 0D-A：只读审计 60 个 query 是否能形成发现 seed、与 seed event-cluster 不相交的验证 panel、真实多切点签名和历史 rolling-origin 目标；不训练、不揭真实结果、不改生产；
2. 忠实复现或适配 TCA-SIR、PGR、CANA、CMI、case-based prediction、结果盲 BM25／dense 与当前 SOS-PAR，先确认简单方法是否已足够；
3. 阶段 0D-B：只在另行批准后建立一次性结果防火墙 harness，验证结果、代理字段、embedding、缓存、跨桶副本和参数记忆均不可泄漏；
4. 阶段 1D：执行“同源自验证／事件簇外验证 × singleton／自然 contrast set”的最小 2×2，并将 `BlindCEF / BlindSourceLedger+WitnessedTCA`、直召回／Need-Path probes 作为两个局部消融；
5. 只有事件簇外验证在真实全库召回、固定覆盖率 precision、matched-null 假发布、机制见证召回和表层 foil FPR 上通过，才进入历史 rolling-origin DSPT；
6. rolling-origin 仍有目标侧剩余技能后，先在新 confirmation block 做一次训练无关确认；通过后再分成两个互不污染、分别审批的证据分支：完全不可见的真实 future shadow 检验 prospective transport，可见三臂人工盲评检验产品帮助度；同一目标单元不得同时进入两臂；
7. 任一更简单基线在同预算、同覆盖下达到等效表现，或事件外验证／目标迁移效应消失，就删除无增量组件并停止产品化，不继续用复杂度制造新颖性。

在所有阶段，Spark 只在调用方显式设置 `inspiration=true` 时运行；`inspiration=false` 必须保持原路径语义与输出不变。结果只能是可选、待核查的诊断材料，不能写回记忆真源、不能给共激活边增加关系类型、不能直接触发拒绝／permit／计划执行，也不能替当前模型决定是否采纳。

## 2. 问题定义

### 2.1 已经获得的证据

现有 60 例开发性报告只提示以下待复核信号：

- 人工挑选的类比记忆可能提高灵感质量；
- 人工筛选条件相对部分基线可能存在正向差异；
- 这足以支持继续研究“人工 oracle 为什么有用”，不足以确认显著性，更不支持当前自动 Spark 有效。

这些结果不能推出：

- 当前自动选择器已经理解远类比；
- 下游回答变好一定来自正确类比；
- embedding 的中间距离就是远亲；
- 自动选择效果可以从 60 例直接推广到真实长期记忆库。

尤其需要警惕随机上下文或单纯增加推理提示也可能改善下游输出。Relevant or Random 的实验发现，随机自生成示例有时可达到或超过相关示例，因此“最终回答更好”不能单独证明检索到了真正类比。

### 2.2 当前自动筛选的根本错误

目前需要分开的其实是三个问题：

1. 候选能否从大型记忆池被召回；
2. 候选与当前张力是否存在可迁移结构；
3. 结构成立后，它在表面上属于近类比还是远类比。

如果把三者压成一个相似度分数，会产生四类混淆：

| 结构相似 | 表层相似 | 正确解释 |
|---|---|---|
| 高 | 高 | 近类比 |
| 高 | 低 | 远类比 |
| 低 | 高 | 表面假朋友 |
| 低 | 低 | 无关记忆 |

结构成立后还存在一个独立维度：对应响应是同向、逆向还是按预声明条件分块混合。两个事件可以表层很远、结构可比，但结果方向相反；也可以情绪价值相反却使用同一个机制方向。这个维度不能从上表的表层近远或一个 cosine 数值推出。

因此近与远不能成为记忆的永久属性。它们必须相对于本轮张力、所采用的抽象层和可迁移推断，在查询时派生。

### 2.3 设计目标

DSR-CT 主门与条件性的 SOS-PAR 防火墙组件、WIT／CUT 审计层共同目标是：

- 在允许空结果的前提下，提高自动候选的结构精度；
- 保留对表面很远、结构相关记忆的召回能力；
- 明确识别同主题但机制错误的假朋友；
- 给每项判断附原文证据和失效边界；
- 让当前模型保留接受、修订、忽略或拒绝候选的自主权；
- 在未显式请求灵感时完全不运行。

### 2.4 非目标

本方案不做以下事情：

- 不给记忆永久标注近亲、远亲或关系类型；
- 不把自动抽取的因果结构写成记忆事实；
- 不修改、合并、覆盖或删除原始记忆；
- 不新增独立 MCP 工具；
- 不让 Spark 自动触发拒绝、permit、计划执行或行为控制；
- 不实现“模型主动拒绝与反思性自主”方向；
- 不把记忆可信度与本方案的类比置信度混为一谈；
- 不追求每次调用都返回灵感；
- 不以更复杂的图数据库替代当前记忆真源。

## 3. 与 OB 哲学边界的对齐

### 3.1 模型自主性

Spark-WIT 只返回候选材料，不返回规范性结论。它最多说明：

- 哪个局部结构可能可迁移；
- 哪些原文支持这项映射；
- 在什么条件下映射会失效；
- 哪些部分仍然未知。

是否采用候选、如何理解候选、是否形成新的态度或行动，仍由当前模型决定。记忆可以影响思考，但不能替代思考。

### 3.2 记忆真源

原始 Markdown、Ledger、SQLite 或现有记忆真源不因 Spark-WIT 改变。

派生事件草图、抽象视图、反事实探针和响应矩阵必须满足：

- 可从原文重建；
- 带来源指针；
- 带抽取模型版本；
- 带原文内容哈希；
- 原文变化后自动失效；
- 可以安全删除缓存而不丢失真实记忆；
- 不被当作新的记忆真源。

### 3.3 共激活边

共激活边继续只记录同一认知窗口中共同出现的事实。边表保持简单，不增加关系类型或语义标签。

共激活边在 Spark-WIT 中只能作为候选召回先验：

- 图上接近不代表类比成立；
- 高频共激活不代表机制相同；
- 边权重不能越过结构验证门禁；
- 边不能生成行为权重或规范性权重。

### 3.4 触发方式

Spark 不新增 MCP 工具。未来如果进入实现，应在现有合适调用上增加显式参数，例如：

~~~text
inspiration: false
tension: null
~~~

约束如下：

- inspiration 默认必须为 false；
- 只有调用方明确需要灵感时才运行；
- tension 可以由调用方提供，也可以从当前上下文派生；
- 自动派生 tension 时必须附上下文证据和不确定项；
- 没有足够具体的 tension 时返回空结果；
- Spark 关闭时，其他检索、grow、dream 或记忆能力必须正常工作。

具体应挂载到哪个现有入口，必须在实施阶段审计代码后另行确认，本文不提前决定文件职责。

### 3.5 DSR／DSPT 不是认知层或行为预测系统

MechanismCard、transport reliability 与 TargetObservationContract 都是局部、可丢弃的研究派生对象，不是对人或模型建立持续认知模型。它们只能回答“这一冻结类比假设在已授权观察上是否曾复制”，不能推断人格、意图、情绪、信念、服从倾向或未来行动。

目标 future shadow 只有在单独授权、完全不可见、endpoint 明确且不需诱导现实行为时才允许研究；否则只做历史 rolling-origin。即使 shadow 预测正确，也不能据此让模型拒绝、permit、采取行动或相信某段记忆。记忆提供材料，当前模型始终保留独立思考与不采纳权。

## 4. 先行研究与原创性边界

### 4.1 已有研究已经解决的部分

经典与前沿研究分别支持以下组件：

- Gentner 等人的研究区分了长期记忆的可召回性与类比推断的可靠性：表层线索更影响召回，高阶关系更影响推断可靠性。
- Lee 与 Holyoak 的实验进一步表明，因果模型会引导类比推断。
- MAC/FAC 给出了先粗召回、再结构映射的两阶段框架。
- SME、ARCS、LISA 等模型说明角色绑定、高阶连通结构和当前目的约束的重要性。
- Reviving Inert Knowledge 说明比较两个案例形成关系 schema 可以复活原本难以召回的知识。
- StoryAnalogy、ARN 和 AnaloBench 说明长文本、大候选池和远类比对当前 LLM 仍然困难。
- A3E（Automated Analogy Annotation Expert）说明多阶段结构映射提示优于直接判断。
- SemEval-2026 叙事相似度任务及 COGNAC 表明，行动过程视图和困难样本路由有价值。
- YARN 使用多层抽象改善叙事类比，并已明确用“最终事件相反”区分近表层假类比与真远类比；因此“识别相反结果／opposed”本身不是 Spark 的新颖点。YARN 仍受到抽象层级与隐含因果的限制，WIT 的差异只能落在预提交证据、版本空间与选择性发布协议。
- Sultan 与 Shahaf 已经从自然语言过程文本抽取实体／关系并自动寻找跨域映射；“从文本建关系图再匹配”不是新贡献。
- 2026 年的 Teaching Through Analogies 表明 sub-concept grounding 能提高封闭池检索精度与解释质量，但对开放生成帮助有限；这直接支持“先把目标需要拆成可比较子义务”，也说明拆分本身不够。
- CANA 使用机制分解、机制对齐和跨类比确认改善历史类比研究，并明确将表层观察无法识别隐藏机制作为核心障碍；“机制而非表层”以及“多案例确认”均不能再作为 Spark 的独占原创点。
- Past Meets Present 已研究 LLM 的历史类比检索／生成与自反思；Context-Gated Associative Retrieval 又从理论和 transformer 实验讨论上下文如何重塑召回。因此“历史类比 + 自反思”或“上下文门控召回”也不是独立原创点。
- Analogy making as amortised model construction、Graph Similarity Description 等工作已经从模型构造、部分结构映射与可解释相似描述讨论类比；“把类比当模型构造”或“给出图相似解释”不能单独支撑首创性。
- 1998 年 adaptation-guided retrieval 已经论证最相似案例不一定最可复用，并用适配需求指导检索；“按可迁移／适配成本而非相似度选案例”不是新贡献。
- Akifuji 与 Tsuji 早在 1995 年已把 version-space 用于 case retrieval／indexing，以“可直接复用／可能可解”的两级条件定位查询并随成功失败更新索引；Fanizzi、d’Amato 与 Esposito 2007 年又明确以 disjunctive version space 与 semantic difference 做 *Instance-based Retrieval by Analogy*。因此“把 version space 用于案例／类比检索”“disjunctive version space”和“按 semantic difference 做类比推断”都不是新贡献。
- graph edit distance、图匹配、Gromov–Wasserstein 与 Graph Optimal Transport 已经以最小编辑或关系结构保持比较图；“把类比定义成最小图变换”不是新贡献，且精确 GED 通常难以扩展。
- TIMETRAVEL 以受控反事实事件和最小故事改写研究叙事因果链与反事实不变量。
- causal abstraction 已形式化高低层系统之间的干预保持与机制变换；这些工作拥有 Spark-WIT 当前并不具备的 SCM 或更严格形式对象。
- CEGIS/程序综合、主动干预选择和反例驱动验证已经在其他领域形成成熟方法。
- Causal Memory Intervention 已直接比较 `no-memory / with-candidate / perturbed-same-candidate` 三种条件下的回答变化来选择长期记忆，是与本方案原创性主张高度相邻的 2026 预印本；其论文实现依赖 gold target scorer，并把已有 useful/irrelevant/harmful 角色标签作为 risk filter 参与筛除和最终选择，不能不加区分地当作 OB 的无标签在线选择器。
- SparseCL 等 contradiction retrieval 已专门学习保留细微矛盾的检索表示；成熟 NLI／否定感知方法也能识别部分正反关系。因此 CUT 必须与这些专门基线比较，不能只胜过通用 cosine。
- METAL 等 LLM metamorphic testing 已用预定义 metamorphic relations 与文本扰动系统检查模型性质；反事实 RAG 风险控制也已通过改变检索质量／使用方式观察答案稳定性，2026 年 RAG 稳健性工作则测试保持语义内容时的风格、来源、顺序、格式和元数据扰动。因此“做表层保持／机制改变扰动并观察回答”也不是新贡献。
- selective classification、AURC 和 conformal risk control 已经形式化错误—覆盖权衡；“允许 abstain／unknown 并画风险—覆盖曲线”是必要工程纪律，不是原创核心。
- 分布向量混合同义、反义、蕴含和主题相关关系，以及词向量算术只在部分关系上可靠，均已有充分先行工作；“反义词可能很近”“负 cosine 不等于语义相反”和“用向量差表示变化”都不是本方案的原创点。

因此以下单独声明都不能构成原创贡献：

- 首次使用多向量；
- 首次发现通用 cosine 不能可靠区分同义与反义；
- 首次用向量差、关系向量或专门编码器预测关系方向；
- 首次使用两阶段召回；
- 首次使用事件链；
- 首次用 LLM 做因果类比；
- 首次用反事实检查类比；
- 首次使用循环一致性；
- 首次让系统寻找反例；
- 首次动态选择抽象层；
- 首次按图编辑／传输成本判断结构远近；
- 首次按案例适配难度指导检索；
- 首次让候选携带证据位置或验证记录；
- 首次用表层保持与机制破坏对照分析 RAG 输出。

### 4.2 WIT-VS 原方案可能成立的原创核心（现为条件审计层）

截至 2026-08-02，本轮扩展检索没有发现与下列**完整联合对象**同构的公开方法，但其每个单独部件都有强先行工作：

> 将开放个人情节记忆检索定义为一种类内可证伪的选择协议：候选 CEF 在查询出现前独立提交，Need Frame 在读取候选前提交；随后只在预注册有限语言中综合逐操作带语义匹配原文 witness／范围证书的迁移程序，并保留 \(\epsilon\)-近优预测等价类。结构主张只有在该冻结假设类的全部合法完成和近优程序上通过盲封留出且不存在另一结构类别程序时才发布；方向主张再加一层独立全体一致门，否则保留无方向结构候选或返回 unknown，并随结果交付竞争程序、带搜索边界的反例审计记录和类内 artifacts。

潜在原创性最窄地落在五个条件必须同时成立：

1. **对象不同**：检索对象不是相似文本、静态图映射或单条记忆 utility，而是“满足当前 Need Frame 的证据见证传输程序”。
2. **不挑单一解释**：结构和方向分别对近优映射的预测等价类判定可识别性，而非让一个 LLM 选一套最有利映射；该结论明确只相对于冻结 DSL／CEF 假设类。
3. **证据与变换绑定**：每个操作都按 canonical enum 提交语义匹配的可机检 witness：`bind_role / bind_axis / bind_condition / align_phase` 使用双侧 positive spans，`coarsen_type / coarsen_phase` 另需 taxonomy certificate，`drop_optional / restrict_scope / open_boundary_port` 使用冻结的 optional／scope／absence certificate；无证据操作不会因整体解释流畅而被接受。
4. **当前需要不可事后改写**：候选出现前冻结未决槽位；结构成立但不能提供非冗余增量的候选被拒绝，避免把“任何漂亮类比”伪装成当前灵感。
5. **输出可复核且非权威**：输出同时携带最低成本竞争程序、最小机制破坏、已搜索范围、类外证据、未搜索空间和 unknown，不产生永久关系标签或行为指令。

2×2 表层 × 机制干预、整流水线 matched-null、自然后续见证和选择性风险—覆盖评估属于**验证协议**，不是算法原创主张，也不因与 WIT 同时使用就变成新统计方法。它们的职责是尝试推翻上述方法假设并识别收益来源。

更简洁的暂定论文贡献表述可以是：

> We formulate analogical episodic-memory retrieval as claim-selective, evidence-witnessed program synthesis over a preregistered finite transport language: candidate evidence forms and query needs are committed independently, and a structural or signed claim is released only when every admissible completion in the near-optimal predictive version space agrees on blinded witnesses and no in-class rival claim remains.

该表述仍只能称“具备原创潜力”。Transport DSL、关系版本空间、version-space case／analogy retrieval、disjunctive version space、CEGIS／rival、semantic-difference retrieval、evidence witness、Need gating、claim-selective／set-level evidence certification、proof-carrying checker、结构化情节工作区和因子干预中的任何一项单独拿出来都不能宣称首创；Akifuji–Tsuji、Fanizzi 等、Relish、Claim-Selective Certification、SURE-RAG、MedRAGChecker、Proof-Carrying Certificates 与 GSW／Panini 已分别覆盖这些骨架。真正待检验的研究单位只剩：pre-query 的个人情节 actual-payload 承诺、候选盲 Need／claim 承诺、逐操作双侧文本 witness 的 codebook×completion×\(\epsilon_{max}\) 并集 transport、盲封全体一致的结构／方向嵌套候选级发布，以及它们相对强基线的**联合非冗余增量**。术语 `claim-selective`、`certificate`、`proof-carrying` 和 `version space` 本身均不是贡献。有符号方向分类器仍是待证伪子假设，`opposed` 的产品输出仍需独立门禁、锁定确认和再次批准。

该原创性判断只能写成“具备原创潜力”或“本轮非系统检索覆盖的一手来源中尚未定位到所述两层完整协议”。在完成以下工作前不得使用“世界首创”：

- 系统综述；
- 关键词扩展与引用链回溯；
- 相关专利检索；
- 对 causal abstraction、bisimulation、CEGIS、case-based reasoning 和 active system identification 的专门先行技术审查；
- 对文本关系预测、NLI／否定与矛盾检索、逆关系表示、关系专门化向量和 metamorphic testing 的专门先行技术审查；
- 外部研究者的新颖性复核。

### 4.3 最危险的相邻工作

本方案最容易被认为是以下工作的组合应用：

- causal abstraction：已经使用干预保持与交换图；
- bisimulation：已经以行为响应等价定义系统关系；
- CEGIS：已经通过反例迭代修正候选；
- Relish／Relational Program Synthesis：已经把领域 DSL、relational version-space learning 与 CEGIS 反例组合成可按领域实例化的关系程序综合框架；“有限 DSL + 关系版本空间 + rival／反例”这一骨架本身不是 WIT 的原创；
- Version-space case／analogy retrieval：Akifuji–Tsuji 已用 version-space 做事例检索与增量索引；Fanizzi 等已用 disjunctive version space＋semantic difference 做实例式类比检索和类成员推断。WIT 不能把“version space 进入检索”写成差异；至少要有一个 DiVS-style 自然文本适配／reconstruction arm；
- Claim-Selective Certification for High-Risk Medical RAG：已经明确提出 claim-selective certification、证据链接证书、partial／conflict／abstain 动作与 shortcut／novelty slice；WIT 不能把“逐 claim 发布”和“证据证书”写成原创，只能比较其 pre-query 双重承诺与版本空间 transport 是否有额外价值；
- SURE-RAG：已经把 evidence sufficiency 视为 set-level 属性，聚合 claim–passage 的支持／反驳／不足、missing hop、冲突与不确定性，并以选择性风险—覆盖发布；它必须进入强语义证据 verifier 候选池；
- MedRAGChecker：已经用原子 claim、证据 NLI 与生物医学 KG 一致性做支持／矛盾诊断；它否定“原子 claim＋多源验证”本身的新颖性，但其 KG、蒸馏监督和领域资源若无法忠实迁移到 OB，只能记作资源不匹配的 N/A／上界，不能算基线失败；
- Proof-Carrying Certificates for LLM Pipelines：已经把 LLM 输出与可检查证书、独立信任边界和 emission gate 结合；`wit-check` 是必要工程约束但不是可单独主张的新思想，也不能借 Lean／proof-carrying 术语暗示语义已形式证明；
- active causal discovery：已经按信息增益主动选择干预；
- adaptation-guided retrieval：已经按适配需求而非表面相似度选案例；
- GED、Gromov–Wasserstein 与 Graph Optimal Transport：已经把结构比较写成编辑路径或关系传输；
- Causal Memory Intervention：已经直接用记忆干预后的回答效果选择长期记忆；
- TIMETRAVEL：已经用受控反事实重写检验叙事因果链；
- cycle-consistent objectives：已在搜索代理中用问题可重建性作为代理奖励，但不是类比记忆检索的直接先行方法；
- CANA：已经使用机制对齐、结构反馈和跨类比确认；
- YARN：已经使用多层抽象和角色映射；
- RootMem：直接针对个性化长对话中“低语义重叠但逻辑关键”的记忆，构造结构化 root memories 并用 LLM router 激活；它比普通 dense／reranker 更接近 OB 当前难题，必须作为危险直接基线；
- Generative Semantic Workspace（GSW）：已经用 Operator 把观察转为结构化局部语义，并由 Reconciler 维护时间、空间与逻辑一致的情节工作区；WIT 不能主张“结构化、可解释的情节事件表示”本身原创，且公平比较应包含只读、查询无关、不得成为新记忆真源的 GSW-style sidecar；
- Panini：已经在写入时把新文档／用户数据整合为持续更新的 GSW 外部语义记忆，并在查询时只遍历其中的 reasoning-grounded inference chains；WIT 不能主张“写入时结构化外部记忆”“查询时检索推理链”或降低无支持回答本身原创。对 OB 的适配只能读取 source-hash 可追溯的临时 sidecar，不能用持续重写的工作区替代原文、成为第二真源或获得未来查询信息；
- SynthoMinds：已经把 retrieval、analogy 和 reasoning／program synthesis 组合，并从检索到的程序记忆中抽取结构性 revelation；虽任务是代码生成而非个人情节记忆，它直接否定“检索 + 类比 + 程序综合”这一宽组合的新颖性；
- RA-RFT：按可验证推理收益训练类比感知 retriever 和回答策略；它是训练型相邻方法，但训练监督与在线只读 OB 的资源条件不同，不能未经适配直接做同预算结论；
- RAG 反事实风险／稳健性方法：已经使用保持语义或改变检索因素的扰动观察输出；
- 约束程序综合与生成器—验证器系统：已经用有限语言、候选程序、反例和验证器减少生成幻觉；
- proof-carrying／witness-carrying 思想：已有直接 LLM pipeline 实例要求生产者附带可检查对象；Spark-WIT 的 witness 不能借用 formal proof 的保证。

因此研究贡献不能写成“我们组合了这些模块”，而必须以新的检索对象、统一判据和可反驳的审计输出来成立。“反例审计”不表示搜索完备，也不构成严格形式证明；输出必须记录算子集合、预算、已覆盖范围和未搜索空间。

### 4.4 先行方法差异矩阵

| 方法族 | 主要检索或学习对象 | “干预”的对象 | 主要判据 | 典型输出 | 与 Spark-WIT 的关键差异 |
|---|---|---|---|---|---|
| MAC/FAC、SME | 结构化案例与关系映射 | 通常无主动干预 | 表层召回后结构匹配 | 匹配分数或映射 | 没有查询前盲封的正负响应审计 |
| 自动过程类比、YARN、CANA | 文本关系图、多层叙事抽象或历史机制 | 结构提示、反馈或跨案例确认 | 静态／迭代机制对齐 | 类比候选、映射与研究解释 | 不要求双时序承诺、逐操作 witness 与全 completion／codebook／ε 的盲封一致发布同时成立；结构归因实验只是验证包，不是算法差异 |
| adaptation-guided retrieval | 可复用／可适配的历史案例 | 对旧方案的适配操作 | 适配知识与适配成本 | 可复用案例 | 已经否定“最相似就是最好”；通常不处理开放自然语言 witness、方向可识别性和结构归因 |
| GED／GW／Graph OT | 图之间的编辑路径或结构耦合 | 节点／边编辑、质量传输 | 最小编辑或关系结构成本 | 距离、匹配或传输矩阵 | 可作为传输搜索基线；图来自文本时的证据忠实、当前需要和下游结构效应不由距离自动保证 |
| Causal Memory Intervention | 长期记忆对最终回答的影响 | `no-memory / with-candidate / perturbed-same-candidate` | 目标答案评分与稳定性变化 | 有用／无关／有害记忆选择 | 直接测试“记忆是否帮助回答”，不是源—目标机制在共同探针下是否对应；论文忠实／oracle 版与 OB label-free reranker 必须分开 |
| RootMem | 个性化历史中低语义重叠但逻辑关键的结构化 root memory | 从历史蒸馏决策保持逻辑并路由激活 | 逻辑相关性与下游个性化回答 | root memory 与路由结果 | 与 OB 问题高度重合；须做查询无关、只读 sidecar、label-free 适配并控制蒸馏信息预算，WIT 不能只和普通向量基线比较 |
| 反事实 RAG 风险／稳健性 | 检索质量、使用方式或 grounding 表层因素 | 文档、顺序、风格、来源、格式等 | 回答风险或不变性 | 风险分数、拒答或稳健回答 | 已有表层保持扰动；不综合个人情节类比的 evidence-witnessed transport，也不隔离映射结构的特定贡献 |
| causal abstraction | 高低层 SCM 或机制模型 | 严格定义的机制替换／变换 | 干预保持、交换图或近似忠实度 | 模型间抽象映射 | 形式基础更强；Spark-WIT 只有文本证据约束响应假设，不能借用其因果保证 |
| CEGIS | 规格空间中的程序候选 | 反例驱动验证 | 形式验证器产生反例 | 满足规格的程序 | Spark-WIT 的验证器不完备、有限 DSL 也不是自然文本的完备规格 |
| Relish／Relational Program Synthesis | 满足关系规格的一组程序及关系版本空间 | CEGIS 反例与 DSL 约束 | 关系规格上的联合满足 | 关系程序 | 已覆盖 DSL + version-space + counterexample 骨架；WIT 的潜力仅在查询无关个人记忆证据承诺、claim-selective 盲封发布与非权威 Witness Bundle 的领域联合协议及实证增量 |
| Version-space case／analogy retrieval（1995／2007） | 属性化案例或 DL 知识库实例 | 成功／失败索引更新，或类成员查询 | 查询在版本空间中的位置、disjunctive VS 与 semantic difference | 排序案例、类成员／新断言建议 | 已直接覆盖 version-space 检索和 analogy retrieval；WIT 的差异只能是双承诺、逐操作原文 witness、全 completion／codebook／ε 的 heldout-unanimous claim release 与只读非权威输出 |
| Claim-Selective／SURE-RAG／MedRAGChecker 语义证据验证 | 固定检索证据上的原子 claim 或 claim–passage 集合 | 支持、反驳、证据不足、missing hop、冲突或不确定性 | claim／set 级 sufficiency、partial／abstain 与风险—覆盖；MedRAGChecker 另用 NLI＋KG | 证据链接判断、诊断和选择性动作 | 已覆盖 claim 分解、语义证据验证、集合充分性与 auditable selective gate；WIT 的差异只能在检索前双重承诺、transport version space、嵌套结构／方向 claim 与候选级 look-elsewhere 控制；KG／蒸馏资源必须匹配后才公平比较 |
| Proof-Carrying LLM Pipelines | LLM pipeline 的后处理产物与机器证书 | 证书验证失败或 emission gate | 独立 checker／形式信任边界 | 可验证证书与放行／拒绝 | 已覆盖 proof-carrying checker；WIT 的 checker 只验证有限假设类 artifacts，不能验证自然语言机制真值 |
| GSW／Panini／ARTEM 等结构化情节记忆 | 时空锚定事件、角色、状态、QA 网络与持久工作区 | Operator／Reconciler、写时整合或事件更新 | 时空逻辑一致、inference-chain／partial-cue retrieval 与长叙事 QA | 结构化 episodic workspace／推理链／事件记忆 | 已覆盖结构化情节表示和写时整合；WIT 必须以预注册实验检验双时序承诺、逐操作跨事件 transport 和不写回真源是否有非冗余价值，不能让 workspace 成为第二真源 |
| SynthoMinds | 代码语料中的历史程序与结构 revelation | 类比提取和生成推理 | 检索记忆是否改善程序合成 | insight／revelation 与生成程序 | 已覆盖 retrieval + analogy + program synthesis 的宽组合；领域、发布证书和个人记忆安全边界不同 |
| Spark-CUT（子层，待证） | 当前张力与记忆的有符号局部响应对应 | 预冻结文本变形和机制探针 | 正负响应差异、部分双向映射与反例审计 | 方向与反例记录 | 只负责 WIT 中的方向／验证，不再单独构成完整选择器 |
| Spark-WIT（条件审计分支，待证） | 满足 Need Frame 的证据见证传输程序及其版本空间 | 映射探针，以及锁定评估中的表层 × 机制因子 | 程序可行性、方向可识别性、结构归因与风险—覆盖 | 非权威 Witness Bundle | 仅审计 SOS-PAR 已通过且有效定位的候选；原创潜力只在该条件对象，优越性和淘汰判断只对信息对称、忠实实现的冻结基线成立 |

这张表也给出一个重要否决条件：如果适配后的 OB label-free CMI、RootMem／GSW／Panini-style 结构情节 sidecar、DiVS-style 类比检索、CANA 风格机制分解、AGR、GED／GW、冻结最强语义证据 verifier 加独立 PCC-style deterministic checker，或静态结构映射在相同信息权限和预算下达到同等或更好表现，Spark-WIT 不能仅凭设计更复杂主张新方法更优；论文忠实／oracle CMI 只作复现与上界诊断，不直接参与该淘汰判断。

CMI 必须在 WIT 最小核心之前完成复现与任务适配，但必须把两个对象分开：

1. **论文忠实／oracle CMI**：保留论文任务所需的 gold target scorer 和 label-aware risk filter；协议必须分别列明哪些是评估标签、哪些会进入 selector 可见输入。该条件只用于检查复现是否正确、估计方法上界与识别适配损失，不参与 OB 方法淘汰；
2. **OB label-free CMI**：不能读取 gold answer、useful/irrelevant/harmful 标签、gold 候选角色标签或外部挑战标签，只能使用在独立适配集或预注册外层 cross-fitting 中确定、随后冻结的代理评分；只有这一版可参与阶段 0D-B／1D 的公平比较。

共享候选池上的 OB 条件严格说是 **CMI intervention reranker**，不能写成完整端到端忠实 CMI。它主要测候选记忆对最终回答的边际效用，WIT 主要测源—目标的局部可传输结构及其对当前需要的特定贡献；因此阶段 0C 的 CMI 结果最多改变资源优先级，不能在尚未运行 WIT-core 时从理论上单独证伪 WIT。然而，在当前产品目标下，如果经过 Spark-Gold 与硬负例质量门的真结构材料本身仍没有跨案例一致的下游帮助，就必须按资源门停止自动比较器实现，直到新的前瞻证据改变该判断，不能以“理论上尚未证伪”为理由继续堆叠复杂度。

完成 CMI 忠实复现和 OB 任务适配后，阶段 0D 才冻结正式共同协议。条件阶段 1W 必须在相同候选池、answer model、冻结 renderer、候选数、token、延迟和调用预算下比较：

~~~text
静态结构映射
CANA 风格机制分解
RootMem／GSW／Panini-style 只读结构情节 sidecar
Akifuji–Tsuji／Fanizzi DiVS-style version-space 类比检索
Relish-style 关系版本空间自然文本适配
AGR／GED 传输成本
Claim-Selective／SURE-RAG／可适配 MedRAGChecker 语义证据 verifier
PCC-style 独立 deterministic checker
GSW／Panini + DiVS／Relish + 最强语义 verifier + PCC-style checker 最近邻复合对手
OB label-free CMI reranker
Spark-CUT 验证子层
Spark-WIT 最小核心
OB label-free CMI reranker + Spark-WIT 最小核心
~~~

结果按以下规则解释：

| 结果 | 结论与动作 |
|---|---|
| OB label-free CMI 下游更强，WIT 结构区分更强 | 不淘汰 WIT；检验其是否能作为 CMI 的前置或后置审计层 |
| WIT 下游更强，但选择主终点或 2×2 结构归因不成立 | 不得主张机制有效；优先排查额外提示、renderer 或结构改写痕迹 |
| CMI+WIT 相对 CMI 的置信区间达到预注册的最小增量门 | WIT 具有非冗余增量价值，可申请进入下一阶段 |
| 任一简单基线同时满足结构与下游非劣、安全非劣和成本优势门，且 WIT 增量区间上界低于最小有意义增益 | 停止当前 WIT 版本，优先采用更简单方案 |

CMI 当前仍是预印本，且原任务与个人情节远类比并不相同。因此应提前比较，但不能未经适配就把它视为既定金标准。`CMI+WIT-core` 的组合算子必须在阶段 0D 冻结为一种尺度无关的确定规则；本文默认用预注册 tie 规则的 Reciprocal Rank Fusion（RRF），不允许看到结果后在串联门禁、rerank、RRF 和分数融合之间挑选。若研究问题确实要求比较两个组合顺序，必须把它们作为预注册的独立实验臂并纳入检验层级，而不是事后择优。

### 4.5 本次检索范围与限制

本提案在 2026-08-01 至 2026-08-02 针对以下关键词簇检查了 ACL Anthology、PMLR、JMLR、OpenReview、arXiv、论文 DOI 页面和一轮 Google Patents 关键词结果：

- analogical memory retrieval、far analogy、narrative analogy；
- counterfactual narrative、perturbation-response correspondence；
- causal abstraction、interchange intervention、bisimulation；
- causal memory intervention、episodic memory selection；
- active intervention targeting、information gain；
- counterexample-guided synthesis、cycle consistency、version-space case retrieval、disjunctive version-space analogy、semantic-difference retrieval；
- adaptation-guided retrieval、graph edit distance、Gromov–Wasserstein、graph optimal transport；
- partial graph matching、fused partial GW、program-generated GED、version-space unanimity；
- minimum-description-length analogy、witnessed mapping、rival hypothesis search；
- proof-carrying analogy、witness-carrying analogy、analogy certificate；
- claim-selective certification、SURE-RAG、MedRAGChecker、set-level evidence sufficiency、proof-carrying LLM pipeline、evidence-linked certificate；
- generative semantic workspace、Panini structured memory、inference-chain retrieval、structured episodic memory、SynthoMinds、retrieval-analogy-program synthesis；
- counterfactual RAG risk、surface-preserving perturbation、selective prediction；
- predictive utility in case-based retrieval、validated retrieval、case-based prediction、case difference heuristic；
- outcome-blind design、blind analysis、prequential scoring、proper scoring rule、source outcome masking；
- longitudinal self-matched design、personal event outcome prediction、experience-following memory；
- analogical memory、episodic memory、graph matching、counterfactual retrieval 等专利组合词。

引用优先选择作者论文页、正式论文集或预印本原页。专利搜索只是一轮关键词排查，不构成法律意义上的 freedom-to-operate 或完整 prior-art opinion。本次工作不是符合 PRISMA 等规范的系统综述，尚未完成数据库全量去重、完整向前／向后引用链、CPC／IPC 分类专利检索、权利要求逐项比对和非英文文献检索，也没有外部研究者或专利专业人士复核。因此“未检索到完整联合对象”只是受检索范围约束的暂定判断，不能转写为全世界不存在。

### 4.6 SOS-PAR 的直接先例、非冗余点与禁止主张（保留为组件边界）

新版必须正面承认下列强先例：

- Johnson 与 Seifert（1990）研究了包含结果预测线索、但不含结果的部分情节模式能否触发案例召回；这说明“用结果前部分线索召回案例”不是新思想，但不等于对已存储来源结果及其代理字段实施访问隔离；
- Simoudis 与 Miller（1990）的 Validated Retrieval 已提出“先检索、再以领域测试验证候选”，所以“检索后设门”也不是新思想；
- case-based prediction 已研究由历史情境—结果案例预测新情境结果；Case Difference Heuristic 已研究从问题差异到方案／解差异的适配；
- Lee 与 Holyoak 已说明跨域类比可以在角色映射后运行因果模型；
- CANA 已把前提、时间链、机制与结果分解后做跨域历史类比和多类比确认，但会读取完整来源结果，没有同一来源结果 ACL 盲封与揭封前竞争概率预测；
- Dawid 的 prequential 框架、Rubin 的 outcome-blind design 和 blind analysis 已确立“设计／预测先冻结，结果后揭示”的统计原则；
- One Run Is Not an Idea 已在自动化研究中使用冻结候选卡、结果盲忠实度审查与保存产物重跑，说明“冻结规范＋盲审忠实度”本身也不是新意；
- CMI、DCPM、Experience-Following 等工作已分别涉及记忆干预选择、个人跨域模式发现和以后续任务结果评价历史记忆。

因此，SOS-PAR 组件不能主张发明了“用不含结果的部分线索召回案例”、validated CBR、causal analogy、prequential evaluation、proper scoring 或纵向自对照。它过去拟议的非冗余边界是：

> 面向个人自然语言长期记忆，在来源结果不可访问时，以结果前文本完成候选检索，冻结目标侧焦点机制和最近 rival、跨域角色映射、结果轴以及各机制对来源结果的概率分布；随后只揭封一次同一来源情节的原文结果，以相对 rival 和强结果盲来源预测器的预序 proper score 作为候选能否进入条件诊断灵感输出的必要门禁，并保留所有失败分母与开放世界弃权。

这项门禁仍提供有用的**反事后合理化来源结果一致性材料**，但本轮红队审查确认：同一来源事件的 \((X,A)\) 用于提出／拟合映射、同一事件的 \(Y\) 又用于验证，即使 \(Y\) 真正盲封，也仍是单事件自验证；它不能单独成为新版 Spark 的确认性基础。它回答“该映射是否能预序解释这一来源观察”，不回答“机制能否跨独立事件复制”，也不回答“目标一定由相同机制生成”或“模型应采取什么行动”。

与当前 WIT-VS 的判重规则冻结如下：

- 如果 WIT 的检索器、索引、映射器和机制解释器在全部预测冻结前已经严格看不到**同一个真实来源结果及其所有代理字段**，则 outcome sealing 只是 WIT 的完整性条款，不构成新方法；
- 如果当前 WIT 只隐藏合成探针、目标响应或候选响应 span，却允许检索器看全文向量、结果摘要、后续标题或来源结局，那么 SOS-PAR 新增了一个独立 holdout 轴；
- SOS-PAR 的组件价值只在独立验证记忆的共同面板上以纯 Brier／log loss、校准和 rival regret 报告；旧 query 级 \(\Delta_{selection}\) 与固定 top-8 空槽最大损失不再是规范性主终点。总体 DSR-CT 是否有增量，改由固定覆盖率选择风险、自然对照特异性和目标侧复制共同判断。

对外最强可用措辞仅为：

> SOS-PAR 的来源结果防火墙、竞争预序评分和开放世界弃权继续作为 DSR-CT 的验证协议组件；不再把“同一来源盲封结果验证自身映射”作为新版整体的首创新颖性主张。

### 4.7 2026-08-02 前沿增量检索与 DSR-CT 新边界

本轮在 ACL Anthology、PMLR／JMLR、arXiv、AAAI／作者项目页和关键词级专利初筛上继续检索，并新增以下危险近邻。它们直接改变了方法设计，而不只是补充参考文献：

| 相邻工作 | 已覆盖的能力 | 对新版的直接约束 |
|---|---|---|
| Creative Analogy Machine／早期 analogy phase models | 已明确采用 retrieval → mapping → inference validation，并以背景记忆中的 predicate 检查新推断 | “类比发现后再验证”不是新意；DSR 的窄差异只能是 discovery event 不得确认自身、事件簇外结果盲自然 panel 与全 run 竞争概率冻结；target shadow 只是外部迁移测试 |
| Green–Armstrong Structured Analogies（2007） | 专家列多条类比、评相似度、把来源 outcomes 映射到目标 outcomes 后机械导出预测；8 个冲突情境为 46% vs 无辅助判断 32%，能给出至少两条类比且亲历最相近案例的子组为 60%（23 个 forecasts） | “多自然类比 + 来源结果 + 目标预测”已有直接实证；必须作为明确基线，DSPT 不能靠多类比预测主张新颖性 |
| Sen–Workiewicz–Puranam（Strategy Science 2026） | 多来源匹配实验中，LLM 呈高 recall、低 precision，常被表层相似误导；人类呈低 recall、高 precision和更强 causal matching | 这几乎就是 OB 当前失败画像；宽召回生成器与独立适切性审核必须分开，不能把一个 LLM judge 当成自动 gold |
| TCA-SIR（2026-07-30 预印本） | 对每个目标抽取候选来源中的可迁移抽象原则，并学习 graded transferability；在 ResearchBench 报告相对 MOOSE-Chem 的 `HitRate@top4%` 提升超过 10 个百分点 | “目标条件化抽象”不是新意；静态 query-independent CEF 若显著更差，应采用结果盲账本 + 临时见证透镜，而不是为了形式纯度牺牲性能 |
| Prospection-Guided Retrieval（PGR） | 先生成可能的下一步，再把它们用作个人记忆检索探针；在 MemoryQuest 报告 recall `0.723`，最强基线为 `0.256` | 低重叠召回可由多探针显著改善；OB 只能借用“信息需要探针”，不能生成行动计划或把 prospection 变成认知层 |
| DeferMem／MGRetrieval | 高召回后做 query-conditioned evidence distillation，或让已取回记忆迭代指导下一轮检索 | 查询条件化证据蒸馏和迭代检索已有；新版必须靠盲态权限、发现／验证隔离和可证伪复制区分 |
| CANA | 机制分解、结构对齐和两条以上 cross-analogy confirmation | “机制对齐”和“多类比确认”不是新意；自然验证 panel 必须全部结果盲、提交联合概率并能定位反例 |
| Analogous Process Structure Induction | 利用相似过程预测缺失子事件 | “遮住一个过渡再预测”不是新意；多切点只能作为真实相关响应签名的一部分，不能单独构成贡献 |
| CausalNeg／hard-negative retrieval | 逐信息条件制造难负例，也揭示生成负例的 shortcut 与 generative–discriminative gap | 合成 foil 不能当真实验证；必须优先检索自然 foil／bridge，并由独立盲审确认事件身份 |
| Recommendation FDR／Selection by Prediction／mCS／CAP | 对候选选择、排序或在线自适应挑选提供 FDR／FCR 或多变量 conformal 工具 | 选择后风险控制已有；只有满足对应交换性和分数有效性才可借用保证，否则只报告经验风险—覆盖 |
| CMI／Experience-Following | 用候选记忆加入／移除或后续任务结果评价记忆价值 | 目标侧效用与未来结果门已有强邻居；DSPT 只能检验“来源事件外验证后，当前完全 shadow 且不影响被观察结果的 prospective transport”是否有额外证据价值 |
| Remember When It Matters（Proactive Memory Agent） | 独立 memory agent 在长程任务中选择注入 grounded reminder 或沉默；预印本在 Terminal-Bench 2.0／\(\tau^2\)-Bench 报告 pass@1 分别提升 8.3／6.8 个百分点 | “选择性注入优于 always-on／普通检索”不是新意；只能作为获批 sandbox 中的 abstention／可见效用强基线，OB 不采用其行动代理、持续写库或行为控制语义 |
| Decision-Aware Memory Cards／CICL | 按预期 next-action effect、outcome uplift 与 negative-transfer risk 排序／压缩 context；预印本在 50 个 SWE-bench Verified 检索样本上把 hit@1 从 0.58 提升到 0.78 | 反事实启发的决策相关 context selection 已有直接近邻；Spark 不得靠“影响行动”主张新意，也不能把 action shift 变成规范性权重，只能比较其方法中立选择信号 |
| Hindsight／Amory／Memory-R1 | 已覆盖结构化多通道召回、叙事一致性检索以及学习式 memory 管理 | 它们是强记忆系统对手而非 DSR 同构；只允许在独立测试 vault 做 native 复现和不写回真源的只读适配，不能把 opinion／behavior profile、删除或持续改写真源带进 OB |
| TACL 2026 类比综述、ARN、YARN、MIR、MUSE | 系统总结关系映射、叙事类比、方法灵感检索、功能／机制图与跨域抽象路径 | 表层远、结构图、方法灵感检索和功能映射均不能单独宣传为首创 |

#### 4.7.1 当前最危险的逻辑反例

旧 SOS-PAR 可能出现以下结果：一条发现 seed 提出机制 \(H_C\)，系统利用 seed 的行动前材料构造非常贴合该事件的映射，随后成功预测该 seed 的封存结果。这个结果确实排除了直接看结局，却没有排除“对一条事件的高自由度拟合”。更严重的是，若 \(H_C\) 必须在候选出现前定义，真正由候选触发的新联想又被协议禁止。

DSR-CT 用 discovery–validation separation 为两端建立可审计约束；它**不能声称已经解决**选择偏差、隐藏混杂、目标迁移或模型共享偏差：

- 发现阶段允许 seed 提出原本不存在的 \(H_C\)，保留灵感的生成性；
- 验证阶段禁止 seed 及其同事件切片进入候选的事件外验证统计，只允许不同事件簇；这仍是同一研究系统内的外推验证，不叫独立复制；
- 验证检索针对 \(H_C\) 与最不利 rivals 的分歧寻找自然证据，而不是寻找更多“看起来支持”它的故事；
- 历史 rolling-origin 只叫“历史时间外重放”；只有另行批准、候选从未显示且结局尚未发生的 prospective future shadow 才叫目标侧第二封存。

#### 4.7.2 可防守的新颖性陈述

新版只允许以下窄陈述：

> 在本轮**非系统检索所覆盖的一手来源**中，尚未识别到一个同时明确具备以下三项的个人自然语言长期记忆协议：（a）提出机制的 discovery event 永不计入该机制的确认；（b）在任何结果首次可见前，全 run 冻结机制、rivals、事件外自然验证 panel 与概率，并由真实结果 ACL 一次揭封；（c）在来源侧事件外验证和历史 rolling-origin 通过后，使用 never-shown 的 prospective target outcome 做后验计分，且该结果只影响后续版本。四元命名、hash、proper score、目标条件化抽象、历史重放和 shadow 本身都不承担首创性。

这一陈述的每个组成件都有先例。论文的核心方法增量只允许落在（a）+（b）的 **non-self-confirming, replication-conditioned memory selection** 及其非冗余实证收益；（c）作为 DSPT 外部验证扩展单列，不能反向增加主选择器的原创性。若后续发现同构工作，或 structured analogies、TCA-SIR／PGR／CANA／CMI／SOS-PAR 及便宜复合基线在同预算、同覆盖下达到等效效果，必须缩窄或撤回主张。

#### 4.7.3 不能使用的宣传措辞

除旧版禁止项外，新增禁止：

- “首次目标条件化类比抽象”；
- “首次把类比拆成检索、映射和验证”或“首次区分灵感发现与验证”；
- “首次用多条类比确认机制”；
- “首次通过未来步骤提高个人记忆召回”；
- “首次用对照四元组／hard negative 验证类比”；
- “首次用 shadow／prequential 验证迁移”；
- “已证明因果机制跨域传输”；
- “已控制 5% 假灵感率”，除非统计假设、有效 p-value／nonconformity score 和锁定确认均真正满足；
- “世界首创”“世界上没有同类方法”或任何专利可授权／自由实施结论。

#### 4.7.4 关键词级专利初筛的新约束

本轮 Google Patents 关键词初筛至少发现以下相邻披露：

- [US9501469B2](https://patents.google.com/patent/US9501469B2/en)（Google Patents 列为 Active）：`subject–predicate–object` 查询经同／反义扩展检索跨域类似解法，直接邻近远类比发现；
- [US20080091727A1](https://patents.google.com/patent/US20080091727A1/en)（Abandoned，但仍是公开披露）：把具体问题泛化成领域无关问题、检索已有解法并映射／适配，直接覆盖“innovation by analogy”流程；
- [US10872699B2](https://patents.google.com/patent/US10872699B2/en)（Active）：检索多个相似案例并聚合其附着 outcomes 预测新案例 outcome；
- [US10169703B2](https://patents.google.com/patent/US10169703B2/en)（Active）：自然语言 QA 中的 analogy detection、interpretation generation 与 scoring；
- [US20250259042A1](https://patents.google.com/patent/US20250259042A1/en)（Pending）：说明书披露 self-supervised analogical learning、经验证解的 reasoning fingerprint 与后续适配复用；公开权利要求侧重点不能未经 claims chart 扩写；
- [US20230376801A1](https://patents.google.com/patent/US20230376801A1/en)（已授权为 US12572829B2）：在 live／validation data 上比较 incumbent 与 candidate；它没有披露 DSPT 所说的未来结果延迟揭封；
- [US11573882B2](https://patents.google.com/patent/US11573882B2/en)（Active）：同一披露包含历史 backtesting 与不影响 live workflow 的 live shadow，说明二者组合本身也不是新意；

本轮非系统关键词初筛中未识别到完整同构披露；但没有做穷尽式 claims chart、assignee／family 展开、CPC／IPC 分类、非英文同族、失效／审查历史或自由实施分析，因此不能据此断言相关权利要求不存在。说明书披露、公开权利要求和法律状态必须分开记录；Google Patents 的状态也只是数据库标示而非法律意见。专利层面最多写“关键词初筛未识别到完整同构”，不得写“可申请”“不侵权”或“没有相关专利”。

## 5. 核心理论：发现与验证分离的证据约束响应假设族

### 5.1 局部情节系统

把当前张力 Q 和候选记忆 M 都表示为不完整的局部情节系统。关键隔离要求是：候选记忆的证据形必须在不知道本轮查询的条件下生成；用于 SOS-PAR 严格路径的 evidence form 还必须在不知道来源结果、且不读取结果代理字段时生成。查询只允许选择检查哪些已经存在的证据轴，不能为了当前问题重写候选事实：

\[
E=(R,X,A,U,C,O,\mathcal{H},F,\Xi)
\]

其中：

- R：功能角色，而非人物名称；
- X：关键状态及状态轴；
- A：文本中已经观察到的行动；
- U：允许被探针改变的局部变量或文本变形位置；
- C：约束、反馈、时序、资源、控制权与环境调节项；
- O：结果维度；
- H：与现有文本兼容的一个或多个局部机制假设；
- F：由 H 诱导的不完整、可含 unknown 的状态转移假设；
- Xi：每个结构项对应的原文证据、证据强度和未知状态，并区分“支持基线事实”与“支持响应方向”的证据。

该表示是临时推理视图，不是对记忆事实的改写。候选侧可以使用按原文 hash 绑定、可丢弃重建的派生 sidecar，但它不是记忆真源、不能覆盖原文，也不能给共激活边增加关系类型。

### 5.1A DSR-CT 的规范对象：seed 提议，事件簇外记忆验证

#### 5.1A.1 发现 seed 不是验证证据

设查询为 \(Q\)，结果盲宽召回得到发现候选 \(S_d\)。发现模型可以从 \(S_d\) 与 \(Q\) 的 WitnessedTCA 中提出新机制：

\[
H_C = \operatorname{propose}(Q,\operatorname{WitnessedTCA}(Q,S_d))
\]

这是新版相对旧“候选前冻结 \(H_F\)”的关键改变：Spark 必须允许记忆改变当前可考虑的解释空间，否则它只是在给已有假设找证据。但为了防止 seed 自证，一旦 \(H_C\) 生成，必须立即冻结以下 `MechanismCard`：

~~~text
MechanismCard
- mechanism_id / canonical_statement
- discovery_seed_ids / seed_event_cluster_ids
- role_mapping τ / change_mapping ω
- necessary_conditions / blockers / scope
- action_axis / outcome_axis / zero_bands / time_windows
- strongest_rivals {H_surface, H_trend, H_nearest, H_null}
- expected signature for analogue / bridge / foil / null
- source span witnesses and unsupported claims
- model / prompt / seed / implementation / code / input hashes
- created_at / sealed_at / ACL receipt
~~~

`canonical_statement` 只描述一个局部、可推翻的条件关系，不得扩写成人格、信念、欲望、意图或全局认知模型。发现 seed 的结果、后续段落、结果可见摘要与 embedding 不能参与 `MechanismCard`；seed 本身及同一 event cluster 的所有切片都标记 `discovery_only`。

#### 5.1A.2 验证检索是“找能区分机制的自然证据”

机制卡冻结后，第二个检索器在不知道验证结果的情况下寻找 \(V(H_C)\)。它的目标不是最大化与 seed 的相似度，而是找到对 \(H_C\) 和 rival 具有区分力的自然事件：

\[
V^*=\arg\max_{V\subset\mathcal M_{blind}}
\frac{\min_{\pi\in\mathfrak A}\operatorname{EPIG}
(V;H_C,\mathcal R,\text{release error})}{\operatorname{cost}(V)}
\]

该式只定义第二版可研究的 robust-EPIG 上界，\(\mathfrak A\) 是在 calibration 前冻结的概率歧义集。**首版／阶段 1D 强制使用预冻结的分层抽样与确定性 cell-coverage 规则，不运行 EPIG，不根据已揭结果主动 reveal，也不回圈找更有利证据。** 即使第二版获批，EPIG 也只能选择已经存在、结果仍封存的历史证据；不得对现实世界实施干预，也不得向用户提问以制造标签。

每个验证事件必须满足：

- `event_cluster_id` 不属于任何 discovery seed cluster；
- 候选构建器、映射器和审核者均看不到结果与算法最终胜负；
- 至少一个真实原文 span 支持方法提出的 analogue／bridge／foil／null 前因分配；独立 benchmark gold cell 由另一组结果盲审核者冻结并对方法隐藏；
- 结果轴、观察窗、零变化带和 censoring rule 在揭封前冻结；
- 同一事件的多个 prefix-cut 只能组成一个联合观测，不增加独立样本数；
- 若自然 foil、bridge 或 null 不存在，明确记录 missing cell，不能用 LLM 生成故事补位。

#### 5.1A.3 Sealed Contrast Panel

变量长度的自然对照 panel 写为：

\[
\mathcal P_C=
\{V_A,V_B,V_F,V_N\}
\]

其中每一格可以包含零个或多个事件簇。四格按互斥的 `primary_cell` 计数：bridge 是满足冻结低表层阈值的机制兼容事件；analogue 是其余机制兼容事件，因此同一事件不能同时进入二者。

- \(V_A\)：机制与边界条件都应成立的 analogue；
- \(V_B\)：表层远但同一机制响应应保持的 bridge；
- \(V_F\)：表层近但关键操作、路径或必要条件自然破坏的 foil；
- \(V_N\)：零变化、无关机制或结果基率匹配的 null。

panel 分两级，不能把缺格的研究材料称为完整四元对照：

- `minimal_three_cell_panel`：一个 discovery seed cluster；`analogue+bridge` 至少两个 validation event clusters 且包含 bridge；`foil` 或 `null` 至少一格存在；合计至少三个事件簇。它只产生候选审计特征，不能单候选宣称统计确认；
- `full_four_cell_panel`：analogue、bridge、foil、null 各至少一个、至少四个 validation event clusters，并有至少四个真实可评分结果 probe。只有它可进入产品候选校准；
- 任一 cell 缺失时，对应 gate=`not_estimable`，不能让其他 cell 的表现补偿；
- 若以后启用主动选择，untouched holdout 必须是上述最低事件数之外的额外事件簇；首版不启用主动选择。

不足 `minimal_three_cell_panel` 时可以进入 `research_partial_panel`，但不得作为产品候选。这个密度必须在阶段 0D-A 用真实文本检验：至少 20／60 query 能形成 minimal panel，同时至少 6／60 能形成 full four-cell panel；full 比例低于 10% 时，DSR-CT full 立即判定不适合当前数据形态。

每个 validation event 还必须在结果盲时冻结独立的 `ValidationBindingCard`：

~~~text
ValidationBindingCard
- event_cluster_id / sealed_outcome_ref
- role_mapping τ_{v→C} / change_mapping ω_{v→C}
- necessary_condition_status / blocker_status
- proposed_primary_cell / applicability_probability a_C(v)
- null_subtype: no_relation|zero_change|base_rate_matched|null_not_applicable
- source span witnesses / provenance / ACL / hash
~~~

这里的 `proposed_primary_cell` 是方法输出；若为 null，`null_subtype` 也必须同时冻结。方法盲 gold cell／gold null subtype 由 benchmark 持有方在候选进入 scorer 前、仍看不到结果且看不到方法名／proposed cell／分数时另行封存。候选池依赖方法这一事实必须记录，但两套标签不可合并，否则 `surface-foil FPR`、`mechanism-bridge recall` 与 subtype FPR 会退化为自我打分。

#### 5.1A.4 多切点真实响应签名

一个事件内部可以包含多个在时间上真实存在的 prefix-cut：

\[
Z_v=\{(t_{v1},X_{v1},A_{v1},sealed\_outcome\_ref_{v1}),\ldots,
(t_{vm},X_{vm},A_{vm},sealed\_outcome\_ref_{vm})\}
\]

所有 \(t_{vj}\)、轴、窗口、汇总函数和预期差异／不变／反转模式必须在任何 \(Y_{vj}\) 可见前提交。一个机制的响应签名不是对每个点分别挑最佳解释，而是对完整联合向量提交概率：

\[
p_H(\mathbf Y_v\mid\mathbf X_v,\mathbf A_v,
\tau,\omega,B)
\]

如果无法可靠建模点间相关性，采用事件簇级 block score 或先把签名压缩成一个预注册统计量；绝不能把 \(m\) 个相关切点当作 \(m\) 个独立样本。非单调、方向翻转、并行变化、记录频率改变或证据稀疏时，保留 `mixed/unknown`。

#### 5.1A.5 竞争机制预序 tournament

每个 \(H\in\{H_C\}\cup\mathcal R\) 对同一封存 panel 提交：

- 每个真实结果类别的完整概率；
- 对响应顺序、差异／不变／反转的联合预测；
- 适用边界与预期失败 cell；
- 对当前事件的 `out_of_scope/applicability` 概率；`not_scoreable`／censoring 规则只能由独立 outcome holder 在模型运行前统一冻结，对全部机制共同适用；
- prompt、模型家族、seed、实现、输入与代码 hash。

此外必须提交两个信息权限明确的结果对照，不能把 `temporal trend or base rate` rival 改名后重复计算。强 **query-free** predictor 只能读取每个 validation event 在结果前、由共同 allowlist 允许的局部字段，不能读取当前 query、Need Frame、discovery seed、MechanismCard、rivals、方法提出的 cell 或 gold cell；`query-aware-no-discovery` predictor 可以读取共同 query／Need 与 validation 前因，但仍禁止 discovery seed、BlindSourceLedger、WitnessedTCA、MechanismCard、rivals、cell 与结果。两个对照的模型／cross-fit fold、训练 split、owner／event／time 排除表、特征 schema、概率校准与 hash 都在 confirmation 前冻结，训练只用 discovery／calibration 且与正式 event clusters 外层隔离，并对相同 outcome codebook、窗口和 scoreability rule 提交完整概率，随全 run barrier 一起封存。前者排除一般基率／常识可预测性，后者隔离 query 本身的信息；只有 \(H_C\) 对二者都有增量，才支持“发现记忆提出的机制提供了额外信息”。

同一 query 的**全部** discovery seeds、候选数、MechanismCard、ValidationBindingCard、panel、rival、概率、签名、排序和实现回执必须在任何 validation outcome 首次揭封前形成一个 run-level manifest 并全局冻结；随后才对去重的 outcome 并集揭封一次。要求每个 `sealed_at < outcome.first_reveal_at`，先前已揭封的 outcome 永远不能确认其后提出的新机制。确定性 scorer 只根据该次统一揭封的真实 outcome spans 计算 log loss／Brier 和事件簇级 paired regret。禁止：

- 把 \(H_C\)、rival 和 base 混成一个 `p_SOS` 后用混合分布的良好表现代表焦点机制成立；
- 揭封后在 rivals 中挑最弱者，或用 \(\min_H L_H\) 作为方法损失；
- 从多个结果轴、窗口或切点中事后挑命中者；
- 把 LLM 的自然语言信心当作校准概率；
- 用同一模型的多 seed 共识冒充独立真值。

首版采用非补偿式词典序门：

~~~text
ACL / sealing / provenance valid
→ discovery-validation cluster separation valid
→ natural contrast support sufficient
→ H_C beats every frozen material rival on analogue+bridge
→ outcome-blind applicability 在 analogue+bridge 保持、在 foil+null 拒绝
→ H_C beats both query-free and query-aware-no-discovery outcome controls
→ bridge-only proper-loss advantage 通过，不能由近 analogue 补偿
→ leave-one-event-cluster-out and independent-implementation stable
→ 独立 calibration／confirmation 上的 query-level release risk 通过
→ historical target transport reliability above frozen threshold（产品可见路径必需）
→ appropriateness / relevance floor
→ only then optimize novelty / diversity / cost
~~~

任一前层失败，后面的“很新颖”“很远”“LLM 很有把握”都不能救活候选。

#### 5.1A.6 目标侧 DSPT 账本

`TargetObservationContract` 只在历史 rolling-origin 或另行批准的完全 shadow 研究中建立：

~~~text
TargetObservationContract
- target_id / origin_time / eligibility_rule
- observable_measure / baseline / positive_endpoint
- time_window / censoring_rule / missingness_reason
- frozen H_C and rival prediction distributions
- exposure_status = never_shown
- model / prompt / seed / implementation / hashes
- reveal_time / outcome_provenance / score receipt
~~~

`exposure_status` 不是普通日志字段，而是有效性门。只要预测、候选或诊断在观察窗内进入当前模型、用户界面、记忆真源、共激活边或其他行为路径，目标结果就是可能受 Spark 影响的 post-treatment observation；该 receipt 只能用于产品效用实验，不能再作为“自然 target shadow 复制”。

历史 rolling-origin 先于真实未来 shadow。个人 transport reliability 必须按时间顺序 cross-fit，并向总体／机制族层级先验收缩；不得用同一目标事件既选择阈值又评分。真实 shadow 的缺失和 censoring 必须单独建模与报告，不能删掉不便评分的失败者。

#### 5.1A.7 两个正交状态机

当前候选／发布资格状态只允许：

~~~text
disabled
need_unspecified
seed_not_found
seed_only_unverified
validation_panel_insufficient
sealing_invalid
rival_indistinguishable
source_audit_failed
source_audit_passed_unsigned
historical_transport_uncalibrated
historical_transport_valid
eligible_optional_diagnostic
abstain
~~~

前瞻研究状态单独保存，不位于本次候选的升级链上：

~~~text
target_shadow_not_approved
target_shadow_pending
target_unscoreable
target_shadow_failed
target_shadow_replicated
~~~

`source_audit_passed_unsigned` 只表示该候选满足校准器所需的来源侧审计门，不表示三个或四个事件已让单候选达到统计显著；query-level 可靠性来自独立 calibration／confirmation。它的正／反签名仍不稳定时，不能被 CUT 强制二分。`target_shadow_replicated` 只能更新严格晚于揭封时间的后续方法版本，不等于当前目标因果成立，也不能回改已显示或已拒绝的本次候选。产品输出资格至少需要来源审计、历史 rolling-origin transport、共同选择风险校准、当前查询相关性／非冗余性门和产品授权；future shadow 只在主张 prospective transport 时另加。是否还要求 WIT/CUT，必须由增量消融决定。

### 5.2 SOS-PAR 验证协议与 WIT-VS 审计层（不再是总核心）

本节保留 SOS-PAR 的来源单元、防火墙、预提交与 proper-score 细节，供 DSR-CT 的**事件簇外验证记忆**复用。凡本节仍写有“候选出现前冻结唯一 \(H_F\)”“固定 `top_k=8`”“使用混合 \(p_{SOS}\)”“空 slot 记最大损失”或“同一来源结果决定整体发布”的旧条款，均视为历史基线／局部实验定义，不能覆盖第 1 节与第 5.1A 节的新规范。

本节不是 DSR-CT 的发布状态机。`5.2.A–C` 的纵向来源单元、方向坐标和结果防火墙可由 DSR 直接复用；`5.2.E–F` 只复用“全 run 承诺后统一揭封”和逐机制 proper-score 原则；`5.2.D` 以及 `5.2.G–I` 明确属于旧 SOS-PAR `H_F/H_R → candidate localization → post-SOS WIT` 路径；`5.2.J` 只提供证据等级定义。后续 `5.2.1–5.2.13` 是条件 WIT-VS 审计层。DSR 候选是否进入 WIT 只由第 5.1A 节的 DSR→WIT adapter 决定，不依赖旧 `localized_HF/HR`。若旧条款允许在承诺前读取来源结果、结果代理字段，或允许揭封后改候选／映射／机制，新版条款无条件优先。

#### 5.2.A 严格来源单元：先分开前因与结果

每个可用于严格门禁的来源记忆必须被构造成：

\[
b_i=(X_i,A_i,Y_i,\Xi_i)
\]

其中：

- \(X_i\)：同一主体在 \(t_0\) 前的情境、功能角色、约束、资源、已知状态和结果测量轴；
- \(A_i\)：在 \(t_0\) 发生变化的行动、条件或暴露，必须包含行动轴、行动前基线、行动后水平和正向端点；若只能定性判断，则保存独立盲标的行动方向；
- \(Y_i\)：在预冻结观察窗 \([t_1,t_2]\) 内观察或报告的结果方向；
- \(\Xi_i\)：主体标识的不可逆去标识化键、时间戳、原文 span、source hash、并行变化、缺失、模态、证据等级和 provenance。

严格单元的 pairing、主体一致性、\(t_0\)、结果轴和观察窗必须只由 \(t_0\) 前信息或与结果隔离的规则确定。不能看见 \(Y_i\) 后再挑“最像干预”的起点、缩短时间窗、删除零结果或把另一个主体的后续拼上去。

不满足以下任一条件时，该记忆只能用于现有的**非 Spark 普通召回路径**，或进入另行批准、永不注入上下文的 legacy WIT 离线数据审计；它不能成为 DSR 候选／灵感诊断，也不能进入 SOS-PAR 计分或 post-SOS 的在线 WIT 条件审计：

1. 同一主体或稳定分析单位可核验；
2. 行动前材料、行动和后续结果的时间顺序可核验；
3. 行动轴能写成 `{行动量, 行动前基线, 行动后水平, 正向端点, 零变化带}`，结果轴能写成 `{主体, 度量, 比较基线, 时间窗, 正向端点, 零变化带}`；
4. 结果 span 能与前因 span 独立保存和授权；
5. 相同来源事件的重复摘要能够聚类去重；
6. 主要共干预、记录偏差和不确定性被保留，而不是为了形成干净故事被删掉。

#### 5.2.B “正／反”不是情感标签，而是冻结坐标上的观察方向

结果轴 \(O_i\) 必须在揭封前定义：

\[
O_i=(subject,measure,baseline,window,positive\_endpoint)
\]

若文本支持量化差分，先在开发数据上冻结具有领域含义的零变化带 \(\delta_O\ge0\)，再定义：

\[
Y_i=
\begin{cases}
+1,&s_O(o_{after}-o_{baseline})>\delta_O\\
0,&\left|s_O(o_{after}-o_{baseline})\right|\le\delta_O\\
-1,&s_O(o_{after}-o_{baseline})<-\delta_O
\end{cases}
\]

其中 \(s_O\) 由 `positive_endpoint` 冻结。不能为了让预测命中而在测试集调整 \(\delta_O\)。若文本只能支持类别判断，则由只看结果 span、看不到查询和候选映射的独立标注者给出 `-1 / 0 / +1 / unknown` 及证据 span。`+1` 表示沿冻结轴正向变化，不等于“情绪好”“道德正确”或“应当做”；`-1` 也不等于拒绝、危险或错误。

来源行动方向也必须在揭封前冻结。若行动可量化，先在开发数据上冻结具有领域含义的行动零变化带 \(\delta_A\ge0\)，再定义：

\[
U_i=
\begin{cases}
+1,&s_A(a_{after}-a_{baseline})>\delta_A\\
0,&\left|s_A(a_{after}-a_{baseline})\right|\le\delta_A\\
-1,&s_A(a_{after}-a_{baseline})<-\delta_A
\end{cases}
\]

若行动是类别变化，则由只看 \(X_i,A_i\)、看不到结果和查询的标注者给出 `-1 / 0 / +1 / unknown`。\(\delta_A\) 与 \(\delta_O\) 一样不得按测试结果调整；没有可解释零带时，连续行动方向必须为 `unknown`，不能让任意微小噪声强制形成正负。来源内的有符号行动—结果响应是：

\[
R_i=U_iY_i
\]

若 \(U_i=0\)，该单元只能作为 no-change control，不能冒充一个有向干预来源；若 \(Y_i=0\)，则 \(R_i=0\)，不得强制判成 aligned 或 opposed。

跨域映射必须在揭封前同时提交行动轴方向 \(\sigma_A\in\{-1,+1\}\) 与结果轴方向 \(\sigma_O\in\{-1,+1\}\)。\(\sigma_A\) 就是后续 WIT／CUT 旧符号中的 \(\sigma_U\)，必须绑定同一 commitment 字段，不能由两层分别冻结。于是映射到查询坐标的响应方向为：

\[
R_{i\rightarrow Q}=\sigma_A\sigma_OR_i
\]

例如，来源是“减少行动 A 后结果 O 上升”，而目标探针是“增加对应行动”，若忽略 \(\sigma_A\) 就会把正反判错。行动方向、结果方向、基线、时间窗、零变化带或任一轴变换不明确时，方向统一为 `unknown`；禁止在看见结果后重新解释正负。

#### 5.2.C 真实的来源结果防火墙

“在 prompt 里说不要看结果”不构成盲封。最低可信实现需要字段级或服务级隔离：

| 阶段 | 允许读取 | 禁止读取 |
|---|---|---|
| 查询机制冻结 | 当前 query、开发期冻结基率 | 所有测试候选及其结果 |
| 候选召回 | \(X_i,A_i\) 的原文与结果前派生表示 | \(Y_i\)、结果摘要、结果派生标签／标题／向量 |
| transport／映射 | 查询机制、\(X_i,A_i\)、结果前 provenance | 来源结果及能暗示结果的后续元数据 |
| 概率提交 | 冻结映射、机制、结果轴 | 任何真实 \(Y_i\) |
| 一次揭封与评分 | 全部 top-k 的原文结果 span | 修改此前任何提交 |

必须实施以下控制：

- 原始记忆保持不可变；前结果 span 与结果 span 在派生 sidecar 中物理分离或采用独立 ACL；
- 严格路径的 `BlindCEF` 必须由只收到结果前 span 的独立构建器生成：\(BlindCEF_i=Build(X_i,A_i,\Xi_{i,pre})\)。禁止让抽取器先看完整情节再删除 result 节点，因为后见信息可能已经改写角色、条件和摘要；
- 严格检索索引只由结果前原文构建；禁止使用看过全文后生成的摘要、标题、标签、重要性分数、全篇 embedding、关系边或后续时间字段；
- 任何跨桶副本、supersedes 链、对话历史、缓存或模型会话中已经暴露的结果都进入 taint provenance DAG；
- 在结果字段放置不可猜测 canary，预揭封阶段读取计数必须为零；
- 对 \(Y_i\) 做层内任意置换后，候选池、top-k、映射、诊断探针和预测 commitment hash 必须逐字节不变；
- 公开事件可能已存在于模型参数中，确认实验优先使用经授权的私人新情节或前瞻采集材料，并做事件级去重切分；
- 任一 canary 命中、访问日志异常、hash 漂移或结果代理字段进入严格索引，都使本轮实验无效，而不是把该候选简单记为失败。

#### 5.2.D 查询盲的竞争机制与单一诊断探针（SOS-PAR 基线）

本小节只定义旧 SOS-PAR 对照臂。DSR-CT 主臂改为：Need Frame 在候选前冻结；发现 seed 可以提出新的 \(H_C\)；随后才冻结 \(H_C\)、有限最不利 rival 集、轴、边界和验证探针，并用与 seed event-cluster 不相交的结果盲记忆做事件外验证。以下“候选前冻结 \(H_F\)”不得用于 DSR-CT 主臂。

在 SOS-PAR 基线查看任何候选前冻结：

\[
\mathcal H_Q=\{H_F,H_R,H_{other}\}
\]

- \(H_F\)：本轮真正关心的焦点机制；
- \(H_R\)：一个事先命名、语义上最近、并会在**同一个**诊断探针上给出不同结果分布的 rival；
- \(H_{other}\)：开放世界弃权，表示两者都没有足够解释力或存在未建模机制。

SOS-PAR 基线只冻结一个诊断探针，避免在看到来源结果后从大量问题中挑命中的那个。\(H_R\) 不能是明显荒谬的稻草人，也不能在 \(H_F\) 失败后更换。`H_other` 只能触发弃权，不能在揭封后扩写成一个总能解释结果的自由文本机制。DSR-CT 则在发现后冻结多个真实验证 probe 的联合签名，并始终保留 untouched holdout。

每个机制卡至少包含：适用角色、必要条件、预期中介、结果轴、失败条件、对该探针的三分类概率生成规则和版本 hash。旧基线还冻结 \(\pi_F,\pi_R,\pi_{other}\)；它们只用于复现旧混合臂，不能成为 DSR-CT 证据。若焦点机制与 rivals 对冻结 probe 的预测分布没有最低分离度，验证事件没有区分力，状态为 `rival_indistinguishable`。新版不要求用低信息分布填满固定 \(K\) 个槽，也不以最大损失惩罚合理 abstention。

#### 5.2.E 结果盲选择与不可变承诺

SOS-PAR 基线选择器只读取 \(X_i,A_i\)，给出 transport 合法性概率或排序；旧 `top_k=8` 仅为可复现基线。DSR-CT 采用“宽召回预算 + 变量长度验证 panel + 固定最小输出覆盖率”，召回深度由 Stage 0D 真实全库 recall 审计冻结，不能把 top-8 当作普适常数。在解封服务可用前，必须把以下内容写入追加式承诺记录：

- 完整候选池 hash、固定 top-k ID 和揭封前顺序；
- 每个候选的可见字段 hash、taint 检查、主体与时间窗；
- 角色绑定、轴绑定、\(\sigma_A\)、\(\sigma_O\)、\(\delta_A\)、\(\delta_O\)、允许的 transport 和所有 unknown；
- \(H_F,H_R,H_{other}\)、单一探针和结果标签定义；
- 对每个候选提交 \(p_C\)、全部冻结 rivals 的 \(p_r\) 与交叉拟合 \(p_{base}\) 的完整概率分布；旧 \(p_{SOS}\) 只在 legacy baseline arm 中保留；
- 来源域查询无关基率、\(p_{base}\) 模型、先验混合、模型／prompt／tokenizer／seed／解码参数及上述对象各自的规范序列化 hash；
- 失败、超时、缺失和弃权的统一记分规则。

提交后只允许一次性揭封全部 top-k 结果。禁止：删除失败候选、补召回、换顺序、改方向、重跑到成功、只揭封一个“最有希望”候选，或按揭封后的得分截取通过者。若输出数量有限，通过者仍按**揭封前顺序**截断。

#### 5.2.F 用 proper score 比较焦点机制、最不利 rivals 与强基线

每个命名机制必须在揭封前提交：

\[
p_{i,F}(Y),\quad p_{i,R}(Y),\qquad Y\in\{-1,0,+1\}
\]

用于共同 gold-valid、outcome-scoreable 面板上纯 proper-score 评价的，必须是**每个冻结机制各自揭封前已经确定的分布**，不能在看见 \(Y_i\) 后从多个机制中选较好的那个。以下 \(p_{SOS}\) 只定义旧 SOS-PAR 混合基线，不是 DSR-CT 的焦点机制证据：

\[
p_{SOS,i}(Y)=
\pi_Fp_{i,F}(Y)+\pi_Rp_{i,R}(Y)+\pi_{other}p_{base,i}(Y)
\]

旧混合臂的先验权重在候选不可见时冻结。DSR-CT 不用混合分布的得分宣称 \(H_C\) 成立，而是逐一要求 \(H_C\) 超过所有 material rivals，并与不读取当前 query 的强预测器比较。边际来源基率 \(p_0(Y)\) 只作描述；正式绝对技能门使用在 discovery／validation／confirmation 外切分上按来源事件交叉拟合、只读取结果前 \(X_i,A_i,stratum_i\) 的 \(p_{base,i}(Y)\)。测试 query、同一来源事件的其他副本和任何结果字段都不能进入该基线训练。推荐使用归一化 multiclass Brier loss：

\[
BS(p,y)=\frac12\sum_{z\in\{-1,0,+1\}}\left(p_z-\mathbf 1[z=y]\right)^2
\]

定义焦点机制相对 rival 的差异，以及每个机制相对强结果盲基线的绝对技能增益：

\[
D_i=BS(p_{i,R},Y_i)-BS(p_{i,F},Y_i)
\]

\[
G_{i,h}=BS(p_{base,i},Y_i)-BS(p_{i,h},Y_i),
\qquad h\in\{F,R\}
\]

- \(D_i>0\) 倾向 \(H_F\)，\(D_i<0\) 倾向 \(H_R\)；
- \(|D_i|\) 只表示两个命名机制的相对区分，不能证明胜者本身有预测力；
- \(G_{i,h}>0\) 要求指定机制优于一个能利用来源前因常识的强结果盲基线，阻止“压力通常不好”等基础率套话冒充跨域机制。

旧 SOS-PAR 基线必须在运行前二选一并写入协议，不能揭封后切换：

- **单向确认模式**：只检验预指定 \(H_F\)，要求 \(D_i>\tau_D\) 且 \(G_{i,F}>\tau_G\)；\(H_R\) 更优只记作 `counterexample_to_HF`，不成为通过证书；
- **双向诊断模式**：允许输出“更符合 \(H_F\)”或“更符合 \(H_R\)”，但 winner／sign 是选择规则的一部分；null 必须对 `candidate × mechanism/winner-sign × probe` 的完整规则做联合 max-statistic、closed testing 或等价的预注册选择校正。

不能用 \(\min(BS_F,BS_R)\) 选择揭封后的赢家，却只按单机制阈值计显著性。DSR-CT 主臂不采用“揭封后选方向”的双向模式；方向、关键轴和 rivals 在验证 panel 结果可见前冻结，冲突时直接 `mixed/unknown`。

以下七项只定义 **SOS-PAR legacy 候选**进入旧 post-SOS WIT 审计的条件，不适用于 DSR-CT 候选：

1. 结果盲 transport 被独立判为 `valid`，而不是揭封后才解释为 valid；
2. 两个预测分布达到冻结的最低分离度；
3. 相应单向 \(D_i\) 或双向 \(|D_i|\) 越过与完整选择规则一致的阈值；
4. 被检验机制的 \(G_{i,h}\) 越过冻结的绝对技能门；
5. action-shuffle、结果置换、错误主体、时间反转和角色交换中每个适用负控均未被发布为同一机制候选，且预注册负控家族的假通过上界未超限；任何应运行负控缺失都按门禁失败处理；
6. 全部防火墙、provenance 与 commitment 检查通过；
7. `candidate_localization_status == valid`。该状态只在以下三条充分路径至少一条完整成立时为真：
   - 对该候选运行覆盖其召回、排序、机制／winner-sign 与探针选择的有效 selection-aware candidate-specific test；
   - 使用有效联合 maxT，且其 null 重采样分布满足冻结层内交换性、所需的 subset pivotality 和完整选择族覆盖；
   - 使用覆盖该候选及完整选择路径的有效 closed testing 或条件 selective-inference 证书。

   规范布尔式为：

   \[
   valid=T_{candidate}^{selection\text{-}aware}
   \lor\left(T_{maxT}^{valid}\land Exchangeable\land SubsetPivotal\land FamilyCovered\right)
   \lor T_{closed/conditional}^{valid}
   \]

   单独声明 subset pivotality、单独给一个未覆盖选择过程的 candidate p 值，或只有 complete-null pool maxT，均不得把状态置为 `valid`。定位路径、假设、统计量、重采样器和证书 hash 必须随候选承诺。

`candidate_localization_status` 只表示定位**程序／证书**本身有效，不等于候选已经通过。另定义唯一升级集合：

\[
\mathcal L=\{i:\text{第 1--6 项全部通过}\land candidate\_localization\_status_i=valid\}
\]

实现字段为 `localized_release_status=valid` 当且仅当 \(i\in\mathcal L\)。所有 localized precision／coverage、WIT／flat 审计和未来条件输出都只使用 \(\mathcal L\)；不得只按 procedure status 计入。

第 7 项只约束**具体候选的升级、WIT 审计和可见输出**，不把未定位候选从固定 top-k 的方法级损失分母删除。若只有 complete-null pool gate 通过，系统最多报告池级 `pool_signal_detected`；全部具体 bundle 都必须保持 `shadow_unknown(reason="pool_signal_not_candidate_localized")`，不得交给 WIT、CUT 或当前模型上下文。

outcome permutation 只有在预定义层内具有合理交换性时，才可用于正式错误率控制；纵向观察数据通常不天然可交换。无法证明交换性时，置换只作为泄漏／稳健性压力测试，正式阈值应来自独立校准集、有效的分层随机化机制、closed testing 或保守的选择性风险上界，不能宣称 permutation 已控制 FWER。

单个三分类结果的信息量很低。**未取得有效候选定位，或系统尚未通过锁定的独立外部门时**，单 bundle 永远只能是 audit-only 材料。只有具体 SOS-PAR legacy 候选达到 `localized_release_status=valid`，整个方法通过第 16 节锁定门、阶段 3D 独立确认、阶段 5D 可见效用与阶段 6D 产品决策，当前运行持有 `approved_product` 授权且请求显式 `inspiration=true`，它才可降风险升级为 `source_outcome_more_consistent_with_HF/HR` 的条件诊断材料；即使升级，也不能叫因果见证或确认性证书。方法有效性必须在全部预提交候选上以 query 级聚合损失、校准、risk–coverage 和 matched-null 假发布率判断，不能用几个成功故事验证。

`unknown/unadjudicable` 不是三分类 Brier 的第四个隐含标签。benchmark 构建方必须在任何模型运行前，由只看结果 span 的盲持方冻结 `outcome_scoreable`，并把该标志和标签一起封存；选择器看不到该标志。纯 Brier 只在共同的 gold-valid、outcome-scoreable 面板上计算。旧端到端“缺失 slot 最大损失 1”只作为 legacy stress test 保留；DSR-CT 主终点在固定覆盖率上比较 precision／选择风险，同时单独报告 abstention、invalid、unscoreable、重复事件和技术失败，既不奖励用均匀低信息候选填槽，也不允许用全空输出取胜。

#### 5.2.G SOS-PAR legacy 状态机：后层不能救活前层失败

规范状态按顺序单调收窄：

~~~text
source_unit_assessed
├─ ineligible_source_unit (terminal; ordinary retrieval/offline audit only)
└─ eligible_source_unit
→ outcome_firewall_failed | result_blind_candidate
→ transport_invalid | transport_unknown | precommitted_candidate
→ H_other/underdetermined (terminal audit) | prelocalized_HF | prelocalized_HR
→ pool_signal_only | candidate_not_localized | candidate_localized_HF | candidate_localized_HR
→ wit_audit_status=failed | unknown | structurally_supported
  | flat_transport_failed | flat_transport_unknown | flat_transport_supported
→ direction_unknown | aligned | opposed | mixed(P)
→ optional_diagnostic_material
~~~

- `ineligible_source_unit` 是终态分支，不得继续流向结果防火墙、候选门或 post-SOS WIT；
- `outcome_firewall_failed` 是实验无效，不是低分候选；
- `transport_invalid/unknown` 不能因结果预测命中而升级；
- `prelocalized_HF/HR` 只表示分数规则给出的待定位 winner；`pool_signal_only/candidate_not_localized` 只能进入方法级审计分母，不能定位、注入或进入 WIT；
- `H_other/underdetermined` 不能进入有方向输出；
- post-SOS WIT 只能审计既通过 SOS-PAR、又有有效候选级定位的候选，不能用结构复杂度救活分数门或定位门失败；
- CUT 只能对 WIT 已支持的结构标方向，不能产生结构或价值主张；
- flat 分支只允许 `flat_transport_supported → direction_unknown`；它没有资格输出 aligned／opposed／mixed；
- 任一层 unknown 都应保留具体原因，不强迫二分类。

#### 5.2.H SOS-PAR legacy 的 WIT-VS 定位与 DSR adapter

WIT-VS 继续检查三个 SOS-PAR 不直接解决的问题：

1. 从来源前因到查询问题的角色／条件 transport 是否有原文证据；
2. 是否存在同成本或更低成本的竞争映射，使当前结构 claim 不可识别；
3. 该候选是否补充了 candidate-blind Need Frame 的未决槽位，而不只是能预测自己的历史结果。

旧 SOS-PAR 路径不再为所有高召回候选运行完整版本空间、主动信息增益和大量反事实；它只对 `localized_release_status=valid` 的候选运行 `WIT-slim`。DSR 使用单独 adapter：只有 `candidate_audit_receipt.passes_frozen_candidate_audit_gates`、共同固定覆盖率风险校准通过、来源防火墙有效，且 DSR 候选在结果盲时已经冻结 WIT 所需输入，才可条件进入 `WIT-slim`；它不需要、也不得伪造旧 `localized_HF/HR`。两条 adapter 的结果分开报告。只有 `WIT-slim` 相对 flat verifier 有独立增量时，才研究后续完整 WIT。

如果实验发现 DSR-CT 或 SOS-PAR 对应主路径已达到其冻结目标，而复杂 WIT 在 matched-candidate 上没有独立增量，则删除 WIT 在线层，只保留离线审计工具；反之，若 WIT 有增益，也不能把它解释为 outcome sealing 或独立复制的替代。

删除在线 WIT 后只有两种合法动作，必须在锁定数据揭封前写入决策函数：

1. 一个不生成新映射、不运行版本空间、不判断方向的 `flat_transport_integrity_verifier`，仅机械核对来源 span、主体／时间窗、预提交角色绑定、禁止字段和 commitment hash；它通过时仍强制 `direction_class=unknown`；
2. 若该 flat verifier 自身的 transport precision／unsupported-binding 安全门未通过，则该候选永不形成诊断输出，系统返回 `none`。

因此“WIT 判重”不等于绕过结构安全门。旧 SOS 分支最多降级为 **SOS-only + flat integrity + direction unknown**；DSR 分支最多降级为 **DSR source event-external validation + flat integrity + direction unknown**。两者都仍须通过阶段 3D 锁定确认、阶段 5D 可见效用和阶段 6D 产品批准，且当前文档没有授权上线。

“WIT 只审计已经通过各自主路径前层门的候选”是规范：旧 SOS 路径要求有效定位，DSR 路径要求 DSR adapter receipt；二者不可互换。为估计增量，离线 2×2 允许 `WIT-min` 在固定候选全集上作为实验基线运行，但它必须只读 `BlindCEF`、保持来源结果盲封，且所有输出仅用于比较，不能注入模型上下文。

#### 5.2.I SOS-PAR legacy 允许的输出：条件诊断，不是目标结论

通过候选的最强允许措辞是：

> 在已授权的来源侧盲封对照中，\(H_F\) 的预提交预测损失低于冻结 rival \(H_R\)；这不说明当前情境成立。可自行检查：〔冻结边界条件〕。

输出必须同时显示：来源主体／时间窗是否稳定、观察结果证据 span、获胜机制与 rival、Brier 差、相对基率增益、transport 状态、主要混杂、未知项和“来源内见证不证明目标机制”的固定警示。

禁止输出“该记忆证明目标由 \(H_F\) 导致”“因此应拒绝／批准／执行”“该方向是真实关系类型”或任何写回真源的结论。只有阶段 6 已批准的 `approved_product` 运行、且当前请求显式 `inspiration=true` 时，条件材料才可进入当前模型上下文；在此前所有研究、离线与 shadow 阶段，它不能影响当前模型。即使未来获准，采纳、反驳、重构或忽略的权利始终属于当前模型。

#### 5.2.J 纵向证据等级与对照包完整性

为了避免只收集成功故事，所有来源包在揭封前按可见信息冻结 eligibility，并必须保留 `-1 / 0 / +1 / unknown`、失败、零变化和相反变化：

- `T1`：单情节行动前—行动—后续观察，对时间和主体有原文证据；
- `T2`：同一主体、同一冻结轴上至少两次可比的自对照；
- `T3`：同一主体的重复前后观测、较稳定测量和更完整的共干预记录。

等级越高只表示来源内观察证据更强，不自动形成因果效应。所有分析按来源事件聚类，重复摘要不作为独立样本；无法形成严格 bundle 的记忆仍可进入**现有非 Spark 普通召回路径**，但不得以 Spark 降级候选绕过门禁，也不得混入 SOS-PAR 的通过率或 proper-score 分母。

#### 5.2.1 WIT-VS 审计层的新基础不是一种新距离

WIT-VS 的全称是 **Witnessed Interventional Transport over Version Spaces**；它只是 Spark-WIT 条件审计分支内部的规范算法，不是新版 Spark 的总基础。它不声称发明了 transport、图编辑、MDL、反事实或 version space；它改变的是该审计分支的发布规则：

> 不信任单一“最佳映射”。先提交查询无关、原文锚定的候选证据形和查询侧盲封测试，再在冻结的有限迁移语法中保留全部近最优证据相容映射；只有冻结 CEF／DSL 假设类内、与当前主张相关的整个预测版本空间在未参与拟合的见证上给出同一个可识别结论时，才允许发布该主张，否则返回 unknown 和最能区分竞争映射的反例／下一探针。

旧 CUT 的问题不是反事实思想错误，而是仍可能由同一个模型依次完成抽取、挑映射、造探针和评价，形成“内部很一致、现实却错误”的闭环。WIT-VS 首先限制解释自由度，然后用未见证据和竞争映射检查这种闭环。

#### 5.2.2 查询无关的承诺式证据形（Committed Evidence Form, CEF）

在 DSR-CT 中，本小节 CEF 只承担 `BlindSourceLedger`／不可变来源账本职责，不再是唯一候选表示。目标条件化性能由第 1.3 节的临时 `WitnessedTCA` 提供；它必须逐项回链 CEF／ledger span，不能访问结果或写回。若静态 CEF 在对照中更好，可删除 WitnessedTCA；若 WitnessedTCA 更好，其增量也不能放松结果防火墙。

本节保留的传统 CEF 可含结果节点，现记作 `FullCEF`。SOS-PAR 的检索、映射、WIT-min 基线和揭封前 WIT-slim 一律只能读取第 5.2.C 节独立构建的 `BlindCEF`；不能通过对 FullCEF 做字段遮罩来模拟盲态。FullCEF 的结果节点只由 OutcomeStore 在全部承诺冻结后用于结果标注、评分和原文 provenance 展示，不能回流修改映射。

对每条候选记忆 M，先构造：

\[
\mathcal E_M=(G_M,\Omega_M,\Gamma_M,d_M,h_M)
\]

其中：

- \(G_M=(V_M,A_M,\tau_M,\preceq_M)\)：不完整的类型化局部事件图；节点只允许来自冻结的小型 schema，例如角色、行动、状态、资源、约束、结果和阶段；
- \(A_M\)：有证据支持的关系或有向超边；
- \(\preceq_M\)：原文支持的偏序；文本只证明先后时不强造精确时间，也不把共现写成因果；
- \(\Gamma_M(a)\)：每个原子 a 的原文 span、原文 hash、证据等级、模态／时态／主体和允许解释集合；
- \(\Omega_M\)：冻结 CEF schema 与抽取协议内所有仍与证据相容的局部抽取／完成假设；它必须是 schema-bounded 的有限集合，或由 solver 给出完整符号覆盖证书；存在歧义时保留多解，不挑最有利版本，超过事前 cap 或覆盖无法证明时返回 unknown；schema 之外的解释另记为类外风险；
- \(d_M\)（`cef_payload_digest`）：对规范序列化后的**实际 CEF 内容 payload**——\(G_M,\Omega_M,\Gamma_M\) 及其排序／unknown 项、全部证据 span、source hash 与内容 schema——计算的摘要；它排除提交时间戳、run ID、存储位置等非内容运行元数据，用于读取时内容完整性和跨隔离实验臂的字段漂移比较；
- \(h_M\)（`cef_commitment_hash`）：把 \(d_M\) 与 schema／抽取协议、模型／prompt、seed、解码参数、依赖版本、提交时间戳和 provenance 链再次规范承诺得到的完整 hash。只 hash“版本号”而不把实际输出摘要 \(d_M\) 纳入承诺，不能证明预提交；跨独立运行的 \(h_M\) 可因时间戳不同而不同，不能单独解释为内容漂移。

严格版候选 evidence form 必须在本轮查询到达前、`query_access=false` 时生成并提交同时包含 \(d_M\) 的 \(h_M\)，读取时重算 `cef_payload_digest`，再验证完整 commitment、source／extractor／seed／解码参数与时间戳链。新写入或 source hash 变化而尚未重建 CEF 的记忆，本轮只能标记 `cef_not_precommitted` 并异步排队供**未来请求**使用；不能在已经看到当前 query 后做一次“自称看不到 query”的懒重建，再把它当作 pre-query commitment。实现若研究运行时 query-blind 两进程抽取，必须另命名为弱消融，并用进程级输入白名单、固定随机性和先 commit 后重新发起映射的两阶段事务证明隔离。

CEF 允许把原文中明确写出的“如果、因此、随后、尽管、只有在”等结构做临时索引，但禁止：

- 根据当前 tension 补写原文没有的动机、因果边或结果；
- 把未出现的角色自动补成常识角色；
- 因为某个事实不利于映射而删除它；
- 将模型推断写回桶、条目、边表或永久 schema；
- 在 source hash 改变后继续复用旧 sidecar。

对查询 Q 则在 `candidate_access=false` 时冻结 Need Frame：

\[
N_Q=(R_Q,C_Q,U_Q,O_Q,\Omega_Q,\mathcal B_Q,\mathcal P^{fit}_Q,
\mathcal P^{resp}_Q,\mathcal P^{hold}_Q)
\]

- \(R_Q\)：当前真正需要比较的功能角色；
- \(C_Q\)：必要条件、边界与不可违反约束；
- \(U_Q,O_Q\)：干预轴和结果轴及其正向端点；
- \(\Omega_Q\)：在不看候选时仍与当前上下文证据相容的查询侧解释／完成集合；同样必须有限或有完整符号覆盖证书，不能在看到候选后删掉难映射的 query completion；
- \(\mathcal B_Q\)：当前尚未解决的槽位、竞争解释或待突破约束；
- \(\mathcal P^{fit}_Q\)：只含结构义务、允许用于综合映射骨架的探针；
- \(\mathcal P^{resp}_Q\)：映射与坐标提交后才揭封、只允许淘汰既有程序的响应 fit 探针；
- \(\mathcal P^{hold}_Q\)：在任何映射拟合完成前盲封的留出探针。

Need Frame 只决定从 \(\mathcal E_M\) 中检查哪些轴，不能改变 \(\mathcal E_M\) 的原子、证据 span 或允许解释集合。如果当前上下文提取不出具体未决槽位，正确结果是 `none(reason="need_not_identifiable")`，而不是泛化成“寻找任何有趣记忆”。

这里的 `candidate_access=false` 是数据谱系条件，不只是提示词：Need 编译器在当前请求和可追溯的先前工具／对话回合中都不能看见候选 ID、原文 span、CEF 派生字段、召回名次或候选摘要。query commitment 必须保存输入上下文 provenance hash、允许字段清单、进程访问日志和已暴露候选清单。若某候选已在上文被用户或工具显式暴露，应从严格盲态评估排除，或单列为 `candidate_preexposed` 弱条件；不能仍声称候选盲提交。

#### 5.2.3 冻结的有限 Transport DSL

传输程序 \(T\) 只能由阶段 0A 预注册的有限语法组成。第一版建议只允许：

~~~text
bind_role(r_query, r_memory)
bind_axis(x_query, x_memory, orientation)
bind_condition(c_query, c_memory)
align_phase(t_query, t_memory)
coarsen_type(node, frozen_parent)
coarsen_phase(frozen_adjacent_block)
drop_optional(atom)
open_boundary_port(atom)
restrict_scope(frozen_condition)
~~~

每个操作 o 必须携带与其语义匹配的 witness；不能强迫所有一元、缺失或边界操作伪造“双侧肯定 span”：

\[
W(o)=(type,positive\_spans,scope\_certificate,optionality\_certificate,
taxonomy\_certificate,absence\_certificate,hashes,grade,assumptions)
\]

最低要求按操作冻结：

| 操作 | 必需 witness | 不允许的替代 |
|---|---|---|
| `bind_role / bind_axis / bind_condition / align_phase` | 查询侧与记忆侧各自的肯定 span、hash、类型／坐标证据 | 用“未提到”或常识补一侧 |
| `coarsen_type / coarsen_phase` | 被粗化原子的肯定 span + 冻结 taxonomy／相邻阶段路径证书；若形成跨侧绑定，仍需另一侧肯定 span | 结果可用所以事后选父类 |
| `drop_optional` | 该原子的肯定 span + 在查询前 schema 中被标为 optional 的证书 | 因不利于映射临时声明 optional |
| `open_boundary_port` | 指向文本明确未知／未完结的肯定 span；若主张“范围内未找到”，另附检索范围、方法、版本和 hash 的 absence certificate | 把无 span 当作事实不存在 |
| `restrict_scope` | 条件／时间／主体／模态边界的肯定 span | 用未观察范围制造有利子集 |

absence certificate 只证明在冻结检索范围内未找到证据，不证明自然世界中不存在；范围、分词／检索方法、版本或原文 hash 任一变化都使其失效。任何所需 witness 不满足时该操作非法或返回 unknown。

双侧 span 只证明各自局部事实存在，**不证明跨域等价本身为真**；跨域绑定仍是待验证假设。第一版明确禁止：

~~~text
invent_edge
invent_role
drop_required
rewrite_evidence
flip_response
posthoc_axis
posthoc_mixed_partition
probe_specific_orientation
~~~

`orientation=-1` 只能表示有独立证据的坐标端点反向，不能把候选的真实响应翻转“修正”为同向。真实方向差异必须由 CUT 揭封后报告为 `opposed` 或 `mixed`。

Transport DSL 的价值是把 LLM 的自由文本解释压成可重放操作，不是因为“程序化变换”本身新颖。若开发时需要不断为失败样本增加临时操作，说明 DSL 已经退化为事后解释，必须停止当前版本。

#### 5.2.4 传输目标与近最优映射版本空间

第一阶段只在结构 fit 探针和证据骨架上评价**程序—完成对**，响应方向仍遮蔽。先定义相容域：

\[
\mathcal A=
\{(T,\omega_Q,\omega_M):
\omega_Q\in\Omega_Q,\omega_M\in\Omega_M,
T\text{ 在该完成对上类型正确且满足硬证据约束}\}
\]

不同完成不能无条件与所有程序做笛卡尔积；不相容程序—完成组合不进入版本空间，但某个仍属证据相容的完成若没有任何幸存程序，也不能被静默删除。令阶段 0A 冻结的有限合理代码本族为 \(\mathcal K\)。对 \(k\in\mathcal K\) 及 \((T,\omega_Q,\omega_M)\in\mathcal A\)：

\[
J_k(T,\omega)=L_{\mathcal G,k}(T)
+\lambda_k D_{struct\text{-}fit}(T;\omega)
+\mu_k R_{evidence}(T;\omega)
+\nu_k R_{boundary}(T;\omega)
\]

- \(L_{\mathcal G,k}(T)\)：在预先冻结、满足前缀码约束的第 \(k\) 个语法 codebook 下的程序长度；
- \(D_{struct\text{-}fit}\)：对结构 fit 义务的残差，不读取候选响应方向；
- \(R_{evidence}\)：未知、低等级证据、模态／主体／时间错配与无支持操作惩罚；
- \(R_{boundary}\)：未映射必要原子和开放 boundary port 的代价；

不再另加含义模糊的 \(R_{search}\)。程序复杂度只由满足 Kraft／前缀码约束的 \(L_{\mathcal G}\) 及明确证据／边界项承担；候选池和自适应尝试带来的多重性由整流水线 matched-null max statistic 单独处理。两者职责分开，避免对某些程序双重惩罚，也避免用任意“搜索修正权重”假装控制全库误报。

这些成本不是自然常数。全部事前合理 codebook、权重、归一化和 tie 规则必须在确认前冻结；候选池 K 与跨候选多重性不进入 \(J_k\)，而在独立 matched-null 协议中冻结，避免双重修正。规范实现必须在响应和 heldout 揭封前，对所有 codebook 综合程序并提交“codebook × completion × 最大合理 \(\epsilon\)”的**并集版本空间**；发布证书直接在该并集上成立。各 codebook／较小 \(\epsilon\) 子空间只用于画敏感性曲线，不能事后新增程序或用某个有利子空间救活并集中的分歧。Cornuéjols 的 MDL 类比、AGR、GED 和 GW 都是强先行方法，因此最小 \(J(T)\) 本身不构成原创性。

先定义原始证据相容完成宇宙 \(\Omega^{orig}=\Omega_Q\times\Omega_M\)，以及当前 DSL 中至少存在一个合法程序的子集：

\[
\Omega^{T}=\{\omega\in\Omega^{orig}:\exists T,(T,\omega)\in\mathcal A\}
\]

每个 \(\omega\in\Omega^{orig}\) 都必须保留 ID 并被 solver 分类：`transportable`、由预注册普遍必要不变量证明的 `contradicted`，或 `out_of_language/unknown`。不能把 \(\Omega^{orig}\setminus\Omega^T\) 静默改名为“不合法 completion”后删除；任一 unknown／类外 completion 阻止正结构发布，只有所有原始 completion 都获普遍矛盾证书时才允许 `no_supported_correspondence`。为防止全局最低成本完成或某个 codebook 把另一仍证据相容但略难表达的完成挤掉，对每个 \(k\in\mathcal K,\omega\in\Omega^{T}\) 分别定义：

\[
J^*_{k,\omega}=\min_{T:(T,\omega)\in\mathcal A}J_k(T,\omega)
\]

WIT 不只保存一个 \(T^*\)，而先保存响应揭封前、带 codebook membership 的发布并集版本空间：

\[
\mathcal V^0_{release}=
\bigcup_{k\in\mathcal K}
\{(T,\omega,k):
(T,\omega)\in\mathcal A,\ 
J_k(T,\omega)\le J^*_{k,\omega}+\epsilon_{max}\}
\]

随后先提交每个程序的角色、轴坐标和允许 mixed 分区，才揭封 \(\mathcal P^{resp}_Q\)。令事前方向假设族

\[
\mathcal D_0=\{aligned,opposed,H_0^{dir}\}\cup
\{mixed(P,s_P):P\in\mathcal P_{frozen},
s_P\in\mathcal S^{frozen}_P,
aligned\in s_P,opposed\in s_P\}
\]

其中 \(s_P\) 是对 P 每个块的完整 `aligned/opposed` 符号向量，且两类都出现；全文简写 `mixed(P)` 时均指包含 `(partition_id, block_signs)` 的这个完整 canonical key，不是只有分区名的裸标签。\(H_0^{dir}\) 的唯一内部 token 固定为 `H0_dir`；它只是“无稳定方向”的竞争假设，任何 fit∩heldout singleton `H0_dir` 都必须规范化为产品 `direction_class=unknown, direction_hypothesis_key=unknown` 和 reason code `no_stable_direction`，绝不成为第五种方向或第七种最终状态。

对每个版本成员先保留全部内部 fit 合格的方向假设：

\[
\mathcal D^{fit}_{T,\omega}=\{d\in\mathcal D_0:
D_{resp\text{-}fit}(T,\omega,d)\le\tau_{resp}\}
\]

再定义

\[
\mathcal V^{fit}_{release}=
\{(T,\omega,k)\in\mathcal V^0_{release}:
\mathcal D^{fit}_{T,\omega}\ne\varnothing\}
\]

这里的 loss 和淘汰规则必须对方向类**符号对称／等变**：将候选所有已观察响应做真实全局极性翻转时，\((T,\omega,k)\) 的存活成员、结构 claim 和成本完全不变，只允许 \(\mathcal D^{fit}_{T,\omega}\) 中 `aligned ↔ opposed`、`mixed(P)` 的块符号相应翻转、\(H_0^{dir}\)／`H0_dir` 保持。响应 fit 绝不能因为“更同向”而给 aligned 更低成本，也不能在 CUT 之前丢掉 opposed；全部幸存方向假设随版本成员交给后续独立 heldout/CUT 审计。

禁止根据响应新建程序、改 orientation、改 mixed 分区或重新定义 \(J_k\)。若 \(\mathcal V^{fit}_{release}\) 整体为空，状态必须由**证书类型**决定，不能把所有空集都写成一种拒绝：只有 solver 对每个原始 \((T,\omega,k)\) 成员给出同一 scope 内、由原文证据支持的互斥命题不可同时成立之穷尽证书时，才返回 `mapping_status=contradicted` 与 `reason_codes=["logical_contradiction"]`；若不存在这种逻辑矛盾，但 solver 证明每个原始成员都违反一个事前声明、对所有允许映射普遍必要的非逻辑结构不变量，才返回 `mapping_status=no_supported_correspondence` 与 `reason_codes=["no_supported_transport"]`。若空集来自 unknown、证据缺失、普通阈值淘汰、搜索未闭合，或证书无法穷尽全部原始成员，则返回 `mapping_status=unknown` 并记录具体原因。若任一原始证据相容 completion 在任一预注册 codebook 下没有幸存成员，也返回 unknown 并记录 `reason_codes=["completion_coverage_missing"]`，不能通过删除该 completion 或 codebook membership 制造共识。后文 \(\mathcal V_{release}\) 均指这个只经预注册 response-fit 淘汰后的 \((T,\omega,k)\) 版本空间；\(\mathcal H=\operatorname{proj}_T(\mathcal V_{release})\) 仅作为程序投影简写。所有发布量必须在 \(\mathcal V_{release}\) 上计算，heldout 只能评价和发布，不能再淘汰成员后声称剩余者一致。

\(\epsilon\) 不能只冻结一个任意小的点值。阶段 0A 必须依据 cost 标注误差、代码本扰动和“两个程序成本可区分的最小单位”先定义合理区间 \(\mathcal E=[\epsilon_{min},\epsilon_{max}]\)、主值和有限审计网格，不得依据 selector 标签或 heldout 选区间。规范实现直接以所有 \(k\) 的最大包络并集 \(\mathcal V_{release}\) 作发布门；单一 codebook 或较小 \(\epsilon\) 子空间只用于画 `ConsensusVsEpsilon` 曲线和诊断何时开始分裂，不得用来救活并集中的不一致候选。结构与 eligible 方向分别报告 `EpsilonStructuralFlipRate` 和 `EpsilonDirectionFlipRateOnEligible`；任何合理 codebook／\(\epsilon\) 引入另一发布类别时，相关主张退为 unknown。这样不能靠挑 codebook 或把 \(\epsilon\) 缩到几乎只剩单一程序来伪造版本空间共识。

实现不必总是显式枚举全部成员；可以用 CP-SAT／SMT 对每个 \(k\)、证据相容完成和审计 \(\epsilon\) 反复询问“是否还存在成本不超过 \(J^*_{k,\omega}+\epsilon\)、但会产生另一关系类别或另一关键留出预测的相容程序”。如果任一 codebook／完成的求解器在预算内无法关闭最优性 gap，就返回：

~~~text
unknown(reason="solver_unclosed_gap")
~~~

不能把当前 best-so-far 当作已证明的最佳映射。

#### 5.2.5 盲封留出上的版本空间稳健门

Response fit 只用于形成 \(\mathcal V_{release}\)。候选发布必须使用未参与综合、调参和选层的 \(\mathcal P^{hold}_Q\)，并按基础事件／机制组切分，不能把同源改写放到 fit 与 heldout 两侧。

对每个 \(v=(T,\omega,k)\in\mathcal V_{release}\)，记录其 codebook membership、留出结构预测、证据覆盖、\(\mathcal D^{fit}\)、\(\mathcal D^{hold}\)、交集和失败原因。发布必须拆成两个嵌套证书，不能因为当前问题的响应尚未发生，就把一个结构稳定的候选全部丢掉；也不能反过来用结构稳定冒充方向已知。

在读取候选、版本空间与 heldout 前，Need Frame 还必须提交一个有限的 **claim lattice** \(\mathcal L_Q\)：每个节点规定允许发布的结构字段、投影函数 \(\phi_\ell(v)\)、specificity、必须覆盖的 Need obligations 和信息量下限；边只表示事前允许的唯一粗化顺序。主 claim、最多一次或有限次粗化、每层 multiplicity 调整和停止位置全部冻结。不得看到 version-space 分歧后临时删除争议字段、改写“最小共同结构”或选择最容易一致的投影；若唯一冻结退让链只能落到“都有参与者／都有变化”之类低于最低 specificity 或 Need 覆盖的平凡交集，结果必须是 `mapping_status=unknown` 或 `shadow_none`。因此“claim-selective”表示对**预提交的 claim schema**选择性发布，不表示事后选择主张粒度。

**结构候选证书**使用 \(z^{struct}_v=\phi_{\ell^\star}(v)\)，它只包含 `mapping_status`、预提交 claim 的关键结构义务、边界和 heldout 结构预测。默认最严格门是：

\[
\forall v\in\mathcal V_{release},
\quad z^{struct}_v=z^{struct,*}\ne unknown,
\qquad mapping\_status(z^{struct,*})=supported
\]

即冻结 CEF／DSL 假设类内的所有近最优证据相容程序和合法完成都必须支持同一组**事前承诺且达到最低信息量**的结构主张，并在 heldout 上成立。此门通过后只形成 pre-null 的 `mapping_status=supported, direction_class=unknown` 结构候选；只有下述 claim-level null 也以有效 candidate-level 程序定位到该候选，才允许进入候选级发布状态。complete-null pool-only 通过仍只能留审计。

**有符号方向证书**另外使用第 5.2.6 节的 \(z^{dir}\)。只有冻结假设类内每个版本成员的 \(\mathcal D^{fit}\cap\mathcal D^{hold}\) 都是同一个已知完整方向 key、查询侧响应确有观察／独立标签支持，而且方向 claim-level null 也有效定位该候选时，才允许把 `direction_class` 从 unknown 提升为 `aligned / opposed / mixed(P)`。因此“结构争议”否决整个候选，“只有方向争议、方向 null 未定位或目标响应未观察”则保留已通过结构 null 的 unsigned 候选并禁用方向主张。

若计算规模要求使用复杂度加权质量门，只能作为预注册的放宽版本：

先按当前 claim 在全部 heldout 上的完整预测向量定义等价关系 \(v\sim_{claim}v'\)，得到预测等价类集合 \(\mathcal C^{claim}_\epsilon\)，再判：

\[
\sum_{C\in\mathcal C^{claim}_\epsilon:z_C=z^{claim,*}}w(C)\ge 1-\delta,
\qquad claim\in\{struct,dir\}
\]

结构与方向必须各自判门，不能把两者质量混成一个多数票。\(w(C)\) 不能按语法程序或完成副本数量计票，否则同一预测的冗余写法会获得更多权重；它必须来自事前冻结且归一化的完成基准测度与代码先验。其中 \(w\)、\(\delta\)、截断误差和未枚举质量上界必须在校准集冻结；不能事后把少数反对映射称为“噪声”。

发布门还必须胜过匹配难度的 null：

- `shuffled_mapping`：保留节点／边数量，打乱角色绑定；
- `surface_only_decoy`：保留实体、主题词与长度，破坏机制；
- `mechanism_matched_random`：结构复杂度匹配但不服务当前 Need Frame；
- `candidate_permutation`：保持候选集合，随机候选身份；
- `renderer_only`：没有来源记忆，只保留同样的类比提示脚手架。

空模型必须重跑**整条**召回—短名单—综合—版本空间—heldout 流程，并保持候选池规模 K、文本长度、主题密度、CEF 复杂度、模型调用和搜索预算一致；不能只把已经入选候选的最终分数与单个随机文本比较。

为避免 Pareto 输出没有可执行的 max-stat，阶段 0A 先把每个 **pre-null 硬门**写成“正数表示越过门槛”的冻结标准化 margin \(g_j(c)\)，并按发布层拆开。\(\mathcal G^{struct}_{pre}\) 只含 evidence、结构 claim、completion／codebook 覆盖、结构 heldout／rival、Need value 与成本；\(\mathcal G^{dir}_{pre}\) 只含在“结构已支持且 target response 有独立证据”时才适用的方向 heldout／rival、符号一致与方向覆盖。二者都明确排除本轮 null p、后续 calibrator 和 product policy，避免 \(S\rightarrow p\rightarrow S\) 自指。定义：

\[
S_{struct}(c)=\min_{j\in\mathcal G^{struct}_{pre}}g_j(c)
\]

以及只对 signed-eligible 候选定义：

\[
S_{dir}(c)=\min\left(S_{struct}(c),
\min_{j\in\mathcal G^{dir}_{pre}}g_j(c)\right)
\]

方向不 eligible 或方向为 unknown 时，\(S_{dir}\) 记 `not_applicable`，绝不能把它当作 \(S_{struct}\) 的失败。任何适用硬门失败都会令对应 \(S_q(c)\le0\)。对 \(q\in\{struct,dir\}\)，分别在真实 eligible 池和第 b 个同规则 matched-null eligible 池定义：

\[
S^{q,max}_{real}=\max_{c\in\mathcal C^q_{real}}S_q(c),
\qquad
S^{q,max}_{null,b}=\max_{c\in\mathcal C^{q,null}_b}S_q(c)
\]

若真实池与 B 个 null 池的生成和分配在对应 claim level 可交换，则：

\[
p^q_{pool}=\frac{1+\sum_{b=1}^{B}
\mathbf 1[S^{q,max}_{null,b}\ge S^{q,max}_{real}]}{B+1},
\qquad
p^q_c=\frac{1+\sum_{b=1}^{B}
\mathbf 1[S^{q,max}_{null,b}\ge S_q(c)]}{B+1}
\]

\(p^{struct}\) 只能支持 unsigned structural claim，不能为 signed direction 背书；已知方向只有在结构层先通过、方向层也独立通过时才能发布。跨候选和两个 claim level 的 multiplicity 必须在阶段 0A 选择一种冻结方案：对 `candidate × claim_type` 做联合 max-stat，或使用结构先行、方向后行且有明确 alpha 分配／closed-testing 的层级门。不得看结果后在两种方案间择优，也不得把同一个结构 p 值复制到方向字段。

每个候选的对应 pre-null 条件先要求 \(S_q(c)>0\)，而 \(p^q_{pool}\) 只回答相应 claim 池中是否至少有一个信号。这里的 maxT 首先只是 **complete-null pool gate**：若要在真／假候选混合池中把 \(p^q_c\) 称为一般强 FWER 调整 p 值，还必须证明冻结流水线满足 subset pivotality／条件交换性，或使用有效候选级条件重采样、closed testing 等程序。满足时才可定位并发布具体候选。若未证明，模式必须写 `complete_null_pool_only`：即使 \(p^q_{pool}\) 通过，也只能报告“池中存在某种信号”，**不能**因一个按真实分数选出的“预注册 top 规则”而给任何具体 bundle 记 passed、显著或可注入；所有候选保持 audit-only／`shadow_unknown(reason="pool_signal_not_candidate_localized")`。只有候选身份本身在看任何分数前固定，且对该身份运行有效 candidate-specific null，或对完整 top-selection 规则给出真正的 selective-inference／条件随机化证书，才可把该候选视作定位成功。结构层有有效 candidate-level 证书而方向层失败／不适用时，才可保留 `mapping_status=supported, direction_class=unknown`；结构层未定位时，方向层无权救活候选。B 必须达到目标 p 值分辨率；null 不可交换时只报告压力测试。在线顺序扩大候选池时必须固定 K，或使用事前批准的 anytime-valid／alpha-spending 规则，不能搜到过门为止。

若近最优程序在关键结构留出预测上分歧，结果不是“取多数票后发布”，而是：

~~~text
unknown(
  reason="mapping_nonidentifiable",
  rival_programs=[...],
  cheapest_discriminating_probe=...
)
~~~

若结构预测一致、只有方向预测分歧，则 reason 使用 `direction_nonidentifiable`，候选仍可作为无方向的结构材料进入 shadow；它不得被描述为正向、反向或 mixed。

#### 5.2.6 两件事情如何判定同向、逆向或无法判断

正／反不是记忆固有标签，也不是情感正负。它只相对于一组预先声明的功能坐标成立。对每个探针 c：

1. 查询侧先声明“增加哪个变量”是 \(+U_Q\)，以及“哪个结果方向”是 \(+O_Q\)；
2. transport 在响应揭封前给出候选轴与查询轴的坐标方向 \(\sigma_U^T(c),\sigma_O^T(c)\in\{-1,+1\}\)；
3. 查询和候选分别从有证据的 baseline／after 状态得到 \(S_Q(c),S_M(T(c))\in\{-1,0,+1,\bot\}\)；
4. 候选响应搬运到查询坐标：

\[
\widetilde S_M^T(c)=
\sigma_U^T(c)\sigma_O^T(c)S_M(T(c))
\]

5. 在单一 T 下，\(S_Q=\widetilde S_M^T\ne0\) 支持 aligned，符号相反支持 opposed；不同预冻结轴／阶段稳定分块才支持 `mixed(P)`；证据缺失、基线未知、非单调、跨转折点或坐标不唯一都返回 unknown。

这里必须区分**回顾性事件—事件比较**与**前瞻性未决问题**。显式目标“希望 \(+O_Q\)”只定义结果坐标，不等于已经观察到 \(S_Q=+1\)；LLM 猜测某个干预会怎样也不能冒充查询侧响应证据。只有查询上下文确有 baseline／after 观察、受控标签或独立时间留出时，才能机械发布两事件的 `aligned/opposed/mixed`。若当前问题尚未发生，\(S_Q=\bot\)，两事件必须写 `direction_class=unknown` 并记录 `reason_codes=["target_response_unobserved"]`；系统最多另列 `transported_candidate_response_hypothesis`，说明候选过去的响应搬到当前坐标后指向何方，并明确它不是当前事件的预测事实、行动建议或 permit。

response-fit 与盲封 heldout 不能各说各话。对每个版本成员 \(v=(T,\omega,k)\)，令 CUT 在 heldout 上、使用相同冻结坐标和 mixed 库得到方向假设集合 \(\mathcal D^{hold}_v\)，再定义：

\[
\mathcal D^{release}_v=
\mathcal D^{fit}_{T,\omega}\cap\mathcal D^{hold}_v
\]

严格 signed 发布要求存在同一个已知 \(z^\star\)，使：

\[
\forall v\in\mathcal V_{release},
\quad \mathcal D^{release}_v=\{z^\star\},
\qquad z^\star\in\{aligned,opposed,mixed(P)\}
\]

因此任一成员的交集为空表示 fit 与 heldout 冲突；交集含多个方向表示该成员方向未识别；不同成员的 singleton 不同表示版本空间方向分裂。三者都返回 `direction_class=unknown` 并保留已通过的结构候选。只有每成员交集为同一 singleton，KnownCoverage、DirectionalCoverage、MismatchRate、至少两个非冗余探针、方向 rival 和 claim-level null 也均过门时，方向才“可识别”。不能只使用 heldout 方向而忘记 response-fit 假设，也不能只选 \(\mathcal D^{fit}\) 中最有利的一类。若未来研究复杂度加权放宽版，必须另外命名、预注册未枚举质量上界和错误—覆盖门，不能与严格版结果混报。

坐标重命名并同步更新 \(\sigma\) 时类别应保持不变；固定坐标与 \(\sigma\) 而真实翻转候选响应时类别才应 `aligned ↔ opposed`。情绪、评价或最终结果正负均不能直接替代以上过程。

#### 5.2.7 机制近与表层远不可互偿

表层远度 \(q_{surface}(Q,M)\) 使用冻结的 lexical、entity、domain 与通用 embedding 视图，转为同职责候选池中的分位数；不同视图不必平均，冲突时保留区间。

机制传输也不压成一个无条件真值。对冻结假设类内所有证据相容完成和近最优程序，得到归一化传输代价区间：

\[
d^-_{mech}=\min_{v\in\mathcal V_{release}}d_{mech}(v),
\qquad
d^+_{mech}=\max_{v\in\mathcal V_{release}}d_{mech}(v)
\]

只有 \(d^+_{mech}\le\tau_{mech}\) 才算稳定的“机制近”；阈值落在 \([d^-_{mech},d^+_{mech}]\) 内时返回 unknown。这里的距离是查询条件化、非对称的 dissimilarity，不声称满足三角不等式。

解释层才使用四格：

| 机制传输 | 表层 | 状态 |
|---|---|---|
| 强 | 近 | `near_analogy` |
| 强 | 远 | `remote_analogy`／Spark 候选 |
| 弱 | 近 | `surface_false_friend` |
| 弱 | 远 | `irrelevant` |

这张四格表是报告界面，不是原创理论。发布顺序必须是：先过 evidence、版本空间、heldout 和安全硬门，再判断表层近远；任何更低 cosine 都不能补偿机制失败，极端表层远也不继续获得无限奖励。

#### 5.2.8 当前 Need Frame 的增量价值

结构可传输仍不等于当前值得想起。对通过结构门的候选，另外记录：

- `new_supported_slots`：候选映射支持了多少读取候选前标为未决的槽位；
- `contrast_separation`：候选是否对预冻结竞争解释给出不同可核查预测；
- `redundancy`：映射内容是否只是重复查询已经明确知道的事实；
- `boundary_relevance`：失效边界是否刚好命中当前场景；
- `unmatched_required`：仍无法覆盖的必要义务。

没有校准概率时不把这些量伪装成 information gain。第一版采用不可互偿的 Pareto／词典序门：

~~~text
证据与安全通过
→ 版本空间关系可识别
→ heldout 胜过 null
→ 至少一个预冻结未决槽位获得非冗余支持
→ 才按表层远度、多样性和成本排序
~~~

若多个候选在前四层互不支配，允许返回小型 Pareto 集；不能用临时权重把证据不足但“看起来新奇”的候选抬上来。

#### 5.2.9 Witness Bundle

WIT 的输出不是“这两件事就是同一种规律”，而是一份可复核 bundle：

~~~yaml
candidate_id: "..."
query_commitment: "..."
source_hash: "..."
cef_payload_digest: "..."
cef_commitment_hash: "..."
need_obligations: [...]
transport_grammar_version: "..."
codebook_family_hash: "..."
codebook_members: [...]
hypothesis_class_scope: {...}
excluded_operations: [...]
representative_program: [...]
program_cost_components: {...}
claim_release:
  claim_lattice_hash: "..."
  selected_claim_node: "..."
  fixed_attempt_order: [...]
  fallback_rule: "..."
  minimum_specificity: "..."
  minimum_need_coverage: "..."
  multiplicity_adjusted_results: [...]
version_space:
  epsilon_policy:
    min: "..."
    primary: "..."
    max: "..."
    audit_grid: [...]
    policy_hash: "..."
    maximum_envelope_used_for_release: true
  consensus_vs_epsilon: [...]
  triple_count: "..."
  prediction_equivalence_class_count: "..."
  original_codebook_completion_membership_count: "..."
  represented_codebook_completion_membership_count: "..."
  per_member_surviving_direction_hypothesis_hash: "..."
  unexplored_mass_or_gap: "..."
solver_certificate: {...}
certificate_checker:
  check_type: "supported_release|typed_negative_audit"
  checker_version: "..."
  checker_hash: "..."
  input_bundle_hash: "..."
  passed: false
  invariant_failures: [...]
codebook_sensitivity: {...}
out_of_language_evidence: [...]
evidence_witnesses:
  - operation_id: "..."
    type: "bind_role|bind_axis|bind_condition|align_phase|coarsen_type|coarsen_phase|drop_optional|open_boundary_port|restrict_scope"
    positive_spans: {query: [...], memory: [...]}
    scope_certificate: {...}
    optionality_certificate: {...}
    taxonomy_certificate: {...}
    absence_certificate: {...}
    hashes: {...}
    grade: "supported|weak|unknown"
    assumptions: [...]
heldout_results:
  structural: [...]
  direction: {eligible: false, results: [...], consensus_passed: null}
  external_challenge:
    required_for_locked_signed_product_release: true
    split_commitment_hash: "..."
    revealed_once: false
    per_direction_or_block_results: [...]
    all_required_results_passed: null
mapping_status: "supported|no_supported_correspondence|contradicted|unknown"
direction_class: "aligned|opposed|mixed|unknown"
mixed_partition_id: null
mixed_block_signs: null
direction_hypothesis_key: "aligned|opposed|mixed(P_id,block_signs)|unknown"
target_response_observed: false
structural_rivals: [...]
direction_rivals: [...]
full_pipeline_null_gate:
  multiplicity_rule: "joint_candidate_claim_maxT|hierarchical_closed_testing"
  structural:
    control_mode: "strong_candidate_level|complete_null_pool_only|not_applicable_typed_negative_audit"
    pool_size: "..."
    matched_search_budget: true
    pool_complete_null_p: "..."
    candidate_level_p: null
    strong_control_certificate: null
    candidate_localized: false
    pool_signal_only: true
    passed: false
  direction:
    eligible: false
    control_mode: "not_applicable|strong_candidate_level|complete_null_pool_only"
    pool_size: null
    pool_complete_null_p: null
    candidate_level_p: null
    strong_control_certificate: null
    candidate_localized: false
    pool_signal_only: true
    passed: null
surface_band: "near|far|uncertain"
lowest_cost_break: {...}
highest_loss_residual: {...}
unmatched_required: [...]
known_boundaries: [...]
unknowns: [...]
reason_codes: [...]
risk_band: "uncalibrated|..."
status: "shadow_hypothesis|shadow_unknown|shadow_none"
product_release:
  status: "not_evaluated|candidate|none"
  audit_bundle_hash: "..."
  config_tuple_hash: "..."
  run_manifest_tuple_hash: "..."
  calibrated_risk: null
  risk_gate_passed: false
  reason_codes: [...]
  current_scope_injection_allowed: false
  renderer_policy_hash: "..."
~~~

其中 `representative_program` 只用于可读展示，发布判断来自整个 version space，不能让代表程序遮住竞争解释。产品上下文只注入最少必要证据和明确的“可忽略／待核查”提示；完整 bundle 留在隔离审计层，并遵守原文访问权限。

顶层 `status` 始终描述不可注入的研究／shadow 审计状态；它不能被产品策略原地改成 `candidate`。产品评估必须先冻结一份明确排除 `product_release` 字段的 canonical base-audit projection，再另生成不可变的 `product_release` 审计对象，以二者的不可变配对与冻结 renderer 签出最小产品信封；不得向已经 hash 或已经放进返回列表的基础 bundle 原地写字段。只有该信封同时绑定仍可重算一致的 base-audit hash、跨确认／复制保持不变的 `config_tuple_hash`、本次数据与 run 专属的 `run_manifest_tuple_hash`、校准风险、风险门、理由码、本次请求的 `inspiration=true` 和 renderer hash 时才可进入当前上下文；原始 diagnostic、shadow 字段和完整证书不得直接追加到模型上下文。

该证书始终是**相对于冻结假设类**的：版本空间一致只能说明“在当前 CEF、DSL、代码本和已搜索证据补全内没有发现另一发布类别”，不能说明自然世界不存在其他解释。因此 bundle 还必须记录 `hypothesis_class_scope`、`excluded_operations`、`codebook_sensitivity`、`solver_certificate` 和 `out_of_language_evidence`。只要任一事前合理代码本导致类别翻转，或关键原文无法由 schema 表达，就令相关 `mapping_status`／`direction_class=unknown` 并记录 `reason_codes=["model_class_sensitivity"]`，不能把类内共识写成绝对证明。

#### 5.2.10 独立证书检查器与“扩类不增信”约束

Witness Bundle 不能只由生成它的 selector 自己宣布有效。规范实现还包含一个**不调用 LLM、与综合器代码路径分离、输入输出确定**的证书检查器 `wit-check`。它有两个互斥入口：`supported_release` 重放 Need／CEF commitment、实际 payload 与 source hash、Transport DSL／代码本、逐操作 witness、completion 覆盖、逐 codebook 最优性边界、\(\epsilon_{max}\) 并集、claim lattice、heldout、rival／null 与最终正结构状态；`typed_negative_audit` 重放每个原始 completion／codebook／允许 transport 的穷尽负证书、同范围逻辑矛盾或预提交普遍必要不变量。两者都只消费冻结 artifacts，不读取自由文本解释，也不重新选择主张；负状态入口只产生 audit-only 结果。

检查器至少验证：

1. hash 链、提交先后和 candidate-blind provenance 完整；
2. 每个程序只使用冻结 DSL，且每个操作拥有与其语义匹配的 witness／scope／optionality／taxonomy／absence 证书；
3. 所有 evidence-compatible completion、预注册 codebook 与最大 \(\epsilon\) 包络都被纳入，solver gap 已按协议关闭；
4. 发布的 claim 是冻结 claim lattice 中按固定顺序获得的首个合法节点，且全部版本成员在盲封 heldout 上同意；
5. 正结构、方向与 typed-negative／空集状态使用正确且互斥的 checker 入口；response-fit 满足全局极性翻转等变性，每个版本成员按 \(\mathcal D^{fit}\cap\mathcal D^{hold}\) 判方向，结构／方向 null 分层且 multiplicity 闭合；不能把 `unknown`、非逻辑不可映射、方向失败或整池 complete-null 伪装成逻辑矛盾、结构失败、候选级 maxT 或强 FWER，也不能把 audit-only 负状态送入正候选排序；
6. 删除任一 required evidence、破坏任一关键证书或令搜索 gap 未闭合时，检查必须失败并把输出收缩为 `unknown/none`。

再冻结一条可机检的**主张单调性**：在保持既有证据不变时扩大合理 codebook 集、\(\epsilon\) 包络、completion 集或 DSL，只允许最终主张保持、退到 claim lattice 中更弱的祖先或变为 unknown；绝不能因此升级为更具体结构、从 unknown 变成已知方向，或从无符号结构升级为 `aligned/opposed/mixed`。若扩类后反而增信，说明实现遗漏分支、事后挑类或 claim fallback 有 bug，候选直接无效。

`wit-check` 给出的不是现实真理证明，而是**冻结有限假设类内的可重放审计结果**：它能验证 spans、hash、覆盖和 solver 证书之间是否自洽，不能自行判断原文 span 的语义是否真实表达了某个因果关系。后者仍由独立人工或跨模型证据审计承担。论文和产品文案因此使用 `audit certificate`／“类内证书”，不用无条件的 `formal proof`。

#### 5.2.11 表层 × 机制的 2×2 结构归因实验

这不是在线选择器，也不是原创点，而是判断“为什么有效”的锁定评估。对同一目标—候选构造四个等预算版本：

| 条件 | 机制 | 表层 |
|---|---|---|
| \(Y_{11}\) | 完整 | 原样 |
| \(Y_{10}\) | 完整 | 中和／换皮 |
| \(Y_{01}\) | 最小关键机制破坏 | 原样或主题匹配 |
| \(Y_{00}\) | 最小关键机制破坏 | 中和／换皮 |

在冻结 answer model、renderer、token、顺序随机化和盲评器下，定义探索性结构归因信号：

\[
\Psi_{structure}=\frac12[(Y_{11}-Y_{01})+(Y_{10}-Y_{00})]
\]

表层主效应：

\[
\Lambda_{surface}=\frac12[(Y_{11}-Y_{10})+(Y_{01}-Y_{00})]
\]

交互：

\[
\Omega=Y_{11}-Y_{10}-Y_{01}+Y_{00}
\]

若 \(\Psi_{structure}\) 不成立而只有 \(\Lambda_{surface}\) 或提示效应成立，不能说 WIT 找到了结构类比。若 \(\Omega\) 很大，说明结构效果依赖特定表层呈现，需要报告而不是平均掩盖。

四个版本必须由独立盲审确认：事实可读、长度／信息密度／自然度非劣，表层换皮保持预定结构，机制破坏确实破坏目标关系，并把其他属性变化控制在预注册界内；自然语言刺激几乎不能证明“只改变一个关系”，因此估计对象严格限定为**这些通过审计的文本版本分配所产生的 intention-to-treat 效应**，不是纯机制变量的无污染效应。同一个生成器制作全部版本会留下可识别痕迹，因此确认集必须包含人工原生或 generator-disjoint 条件。

这只能称为“在固定 renderer 和随机化提示条件下的结构归因”，不是自然世界因果中介。CMI、反事实 RAG 和稳健性研究均是强相邻工作；WIT 必须用预注册实验检验该因子设计是否相对普通 `no / with / perturbed memory` 提供非冗余诊断。

#### 5.2.12 最强反自证门：自然后续见证

合成 heldout 仍可能由同一模型生成并评价。若获得明确授权的测试／研究数据，最强离线证据应来自**后来真实发生、在映射形成时不可见的后续**：

1. 首选真正前瞻的 prequential 协议：事件仍未发生时，由盲持服务提交“当时可见前缀”、Need Frame、探针、\(\mathcal E\)、transport、\(\mathcal V_{release}\) 和预测的时间戳 hash；到预定窗口后只揭封后续；
2. 若只能回顾历史数据，构建 Need Frame／探针的人必须看不到切点后的文本，后续由独立持有者隔离；该条件只能称 retrospective temporal holdout，不能冒充事前预测；
3. 用实际后续检查事前提交的版本空间对留出关系变化、边界或结果方向的预测，禁止看完结局后重写“当时真正需要解决什么”；
4. 按人物、基础事件、时间段和 vault 分组隔离，禁止同源泄漏；
5. 跨模型家族、跨时间和跨授权测试 vault 复制，并与向量、YARN／CANA 风格结构映射、AGR／GED、CMI、随机和无记忆条件比较。

该设计的价值是验证信号不再完全由解释类比的模型自己制造。只有第 1 种真正前瞻提交可称 prospective witness；第 2 种仍受后见之明与记录选择影响。两者都不能把观察性后续自动升级成真实因果，因为后续包含未观测混杂和环境变化；但它们比同模型自评更接近独立外部见证。

如果没有自然后续、人工 gold 或有效随机化 null，系统结果只能称“留出迁移审计分数”，不能宣称因果识别、错误率保证或统计显著性。

#### 5.2.13 为什么它有机会比旧 CUT 更有效

WIT-VS 预期改善的不是模型“说类比故事”的流畅度，而是自动筛选 precision：

- 查询无关 evidence form 降低看见目标后挑事实的偏差；
- 有限 DSL 降低自由文本映射的解释空间；
- 版本空间防止从多个合理映射中只选有利方向；
- heldout 与 null 防止在 fit 探针上过拟合；
- Need Frame 拒绝结构成立但对当前问题冗余的候选；
- 2×2 和自然后续见证把结构贡献与表层／提示效应分开；
- unknown、求解 gap 和竞争程序使失败可见，而不是被一个置信分数掩盖。

这些是待检验机制，不是效果承诺。若版本空间长期过大导致多数候选 unknown，或自然记忆的证据不足以支撑 CEF，那么 WIT 可能比 CUT 更严谨但产品覆盖过低；风险—覆盖曲线和固定成本下的下游价值必须与 precision 一起决定是否继续。

### 5.3 Spark-CUT 子层：角色映射后的有符号响应对应

Spark 的基础不再是“两个事件在一个向量空间里有多近”，而是：

> 在当前张力下，先把双方的功能角色、可改变因素、结果轴、时间尺度和调节条件映射到同一局部坐标；再检查同一个功能性文本变形在对应结果轴上引起的、有原文证据支持的变化方向，是稳定同向、稳定逆向、分块混合，还是根本无法判断。

可以把它称为 **查询局部、证据约束的有符号扰动—响应对应**。这里的“有符号”不是情绪正负，也不是好坏评价，而是“沿一个预先声明的干预方向变化时，对应结果轴向哪一边变化”。它只在本轮查询中派生，不是记忆永久属性，不写回桶、条目或共激活边。

#### 5.3.1 先冻结可比较坐标，再看响应

对当前张力 Q 和候选事件 M，先提出查询条件化的部分映射：

\[
\Pi_{QM}=
(\pi_R,\pi_U,\pi_O,\pi_T,\pi_C;\sigma_U,\sigma_O)
\]

其中：

- \(\pi_R\)：功能角色映射；
- \(\pi_U\)：探针所改变变量的映射；
- \(\pi_O\)：状态或结果轴映射；
- \(\pi_T\)：时间尺度或阶段映射；
- \(\pi_C\)：约束与调节条件映射；
- \(\sigma_U:U_{ord}\rightarrow\{-1,+1,\bot\}\)：每个有序候选干预轴搬到查询坐标时是否需要反转；
- \(\sigma_O:O_{ord}\rightarrow\{-1,+1,\bot\}\)：每个有序候选结果轴搬到查询坐标时是否需要反转。

每个有序轴都必须在揭封双方响应前声明“正方向”。例如：错误率增加和稳定性增加不是同一正方向；若把“错误率”映射到“稳定性”，就需要显式的 \(\sigma_O=-1\)。没有原文或冻结规范支持轴方向时必须标 `unknown`，不能让比较器在看到结果后选一个最有利的正负号。

只有具有可解释顺序的轴才能编码符号，例如增加／减少、提前／延迟、激活／停用、收紧／放松、获得／失去控制。分叉、合并、角色类别等无天然线性顺序的轴继续按类别匹配和必要不变量审计，不能为了得到正负号强行排序。

对事件 E、探针 u、结果轴 r、时间尺度 t 和条件 cxt 组成的响应单元 \(c=(u,r,t,cxt)\)，定义证据约束的定性有限差分：

\[
g_E(c)=
\operatorname{sgn}_r\!\left(\widehat\Delta_E(r\mid\tau_u,t,cxt)\right)
\in\{-1,0,+1,\bot\}
\]

- \(+1\)：沿冻结干预正方向变化时，结果轴沿冻结正方向变化；
- \(-1\)：结果轴沿冻结反方向变化；
- \(0\)：有证据表明在当前幅度和条件下基本保持；
- \(\bot\)：文本不足、方向不明或前置条件不满足。

这只是文本支持的局部定性有限差分假设，不是真实导数、SCM 参数或 Pearl `do()` 效应。非单调、U 型、强路径依赖或基线区间不明的关系不能被压成一个符号；若探针可能跨越转折点，应直接返回 \(\bot\)。

候选响应搬到查询坐标后：

\[
\widetilde g_M(c)=
\begin{cases}
\sigma_U(c)\sigma_O(c)\,g_M(\Pi_{QM}(c)),
&\sigma_U(c),\sigma_O(c)\in\{-1,+1\},\ g_M(\Pi_{QM}(c))\neq\bot\\
\bot,&\text{其他情况}
\end{cases}
\]

这里的 \(\bot\) 是未定义，不是可以参与乘法的 0。任一必要方向签名未知时，该单元必须进入 `unknown`，比较器不得临时补符号。

\(\sigma_U=-1\) 的代数翻转只在探针算子确有冻结的逆操作、基线与幅度可比且局部单调性有证据时成立；离散操作、阈值效应或不对称路径不能因为“方向相反”就直接乘以 -1。此时应让候选侧直接回答映射后的实际探针，或返回 \(\bot\)。

单元方向关系由冻结规则机械计算：

\[
\chi(c)=
\begin{cases}
aligned,&g_Q(c)=\widetilde g_M(c)\in\{-1,+1\}\\
opposed,&g_Q(c)=-\widetilde g_M(c),\ g_Q(c)\widetilde g_M(c)\neq0\\
neutral,&g_Q(c)=\widetilde g_M(c)=0\\
mismatch,&\text{一边为 0、另一边非零}\\
unknown,&\text{任一边为 }\bot
\end{cases}
\]

`neutral` 可以支持“该稳的稳”，但不能单独证明同向或逆向；`mismatch` 表示该响应单元不一致，不自动等于逆向；`unknown` 不参与正反计票，也不能当作无关证据。

#### 5.3.2 从响应单元聚合为事件方向

先在预冻结且条件可比的候选响应单元集合 \(\mathcal C=\mathcal C_{eligible}\) 上记录全部结果。经预冻结证据门后，角色映射、方向签名或双方响应证据不足的单元统一令 \(\chi(c)=unknown\)；方向计票只会自然使用其余达到中强证据门的单元。定义：

\[
W_+=\sum_{c\in\mathcal C}w_c\mathbf 1[\chi(c)=aligned],\qquad
W_-=\sum_{c\in\mathcal C}w_c\mathbf 1[\chi(c)=opposed]
\]

\[
W_{mis}=\sum_{c\in\mathcal C}w_c\mathbf 1[\chi(c)=mismatch]
\]

\[
W_0=\sum_{c\in\mathcal C}w_c\mathbf 1[\chi(c)=neutral],\qquad
W_{\bot}=\sum_{c\in\mathcal C}w_c\mathbf 1[\chi(c)=unknown]
\]

权重 \(w_c\) 只能来自预冻结的证据等级、探针诊断性、条件兼容性和家族内重复稳定性；不能因为故事稀有、新颖或看起来“有灵感”就临时加权。定义：

\[
A=\frac{W_+}{W_++W_-},\qquad
R=\frac{W_-}{W_++W_-}
\]

同时定义：

\[
W_{known}=W_++W_-+W_{mis}+W_0
\]

\[
KnownCoverage=\frac{W_{known}}{W_{known}+W_{\bot}},\qquad
DirectionalCoverage=\frac{W_++W_-}{W_{known}},\qquad
MismatchRate=\frac{W_{mis}}{W_{known}}
\]

分母为 0 时相应量未定义并返回 `unknown`。只有 A 或 R 通过冻结纯度门、KnownCoverage 与 DirectionalCoverage 达到各自下界、MismatchRate 低于上限，并且至少存在两个非冗余中强证据响应单元时，才能稳定分类。这样“1 个同向、20 个 mismatch”不会得到虚假的纯同向。`neutral` 可以支持应保持的不变量，但不增加同向或逆向票；强证据 mismatch 若击中必要不变量，只能按下述映射状态机审查，不能被方向分母隐藏。至少一个响应单元必须来自映射与轴方向冻结后才揭封的 `internal_audit_heldout` 探针；一个文本中明确记录干预前后、条件和结果的强观察可以形成 `provisional_single_observation`，但不能冒充跨探针稳定。

`internal_audit_heldout` 只是在单个开发运行中未参与映射、分组选择和阈值拟合的内部审计单元，可以参与候选开发判定，但不构成锁定确认。`external_challenge` 由独立标签持有方保管，以基础模板／机制组整体隔离并只揭封一次；方向机制成功以及每个 mixed 块的最终复现只能由它确认。公开的受控案例与隐藏 challenge 的生成 seed、模板和实例必须分离。

从响应量到类别的完整协议参数也必须在阶段 0A 枚举冻结，而不能只写“达到门槛”：

~~~text
tau_pure
known_coverage_min
directional_coverage_min
mismatch_max
n_nonredundant_min
n_internal_holdout_min
mixed_block_tau_pure
mixed_block_coverage_min
mixed_block_n_nonredundant_min
mixed_block_n_internal_holdout_min
cell_weight_normalization
same_source_deduplication_rule
equality_and_tie_rule
zero_denominator_rule = unknown
~~~

这些数值、\(w_c\) 归一化、同源重复去重、等号与 tie 处理只能用开发／校准集确定；`external_challenge` 不参与设门。揭封后任何调整都必须提升研究版本并换用新的未揭封挑战集。

若存在多个仍与证据相容的目标假设 g 或候选假设 h，必须保留 \((a,g,h)\) 维度并使用冻结集合规则。只有允许假设对在规则下支持同一方向类别时才能稳定输出；不得挑选最有利的一对，也不能把尚未消歧的相反假设称为 `mixed`。

#### 5.3.3 不能强迫二分类：两级状态机及其六种最终状态

文档不使用含义混乱的“正关系／负关系”。内部先判 `mapping_status`，仅在映射受支持时再判 `direction_class`；下表是这一两级状态机投影出的六种互斥最终状态，不是可自由拼接的平面标签：

| 状态 | 必要含义 | 不能被误解为 |
|---|---|---|
| `aligned` | 有效映射上，所有达到门槛的预冻结方向块都稳定同向，仅允许冻结的小误差 | 两件事都“好”、模型应照做 |
| `opposed` | 同一可比结构上，所有达到门槛的预冻结方向块在坐标校正后都稳定逆向，仅允许冻结的小误差 | 负 cosine、无关或逻辑矛盾 |
| `mixed` | 至少一个预冻结块稳定同向、至少一个块稳定逆向，各块独立通过证据和 `internal_audit_heldout` 门，且全局纯同向与纯逆向均失败；最终主张还需 `external_challenge` | 模型或 seed 意见不一后的事后解释 |
| `no_supported_correspondence` | 有充分证据说明所有允许映射在必要角色、干预、结果轴或结构不变量上都不能成立 | 单纯没召回、低 cosine、证据缺失或方向不稳定 |
| `contradicted` | 在同一实体、时间、条件和模态范围内，强证据击穿映射所需的必要命题或不变量 | 机制响应逆向、反义词或价值相反 |
| `unknown` | 轴方向、响应证据、coverage、调节条件或机制假设不足以稳定归类 | 应被优化掉的失败项 |

`opposed` 仍可能是有价值的反向类比：它说明双方存在足以比较的局部结构，但同一功能性改变产生系统性相反响应，可能暴露边界条件或调节变量。它不是“应该采取相反行动”。

`mixed` 必须使用响应揭封前冻结的分组 P，例如“短期反馈同向、长期累积结果逆向”。每个符号块都要达到各自最小探针与 `internal_audit_heldout` 门，并在锁定阶段通过独立 `external_challenge`，而且纯 `aligned` 与纯 `opposed` 假设都必须失败。若是在看到符号后才创造分组，只能返回 `unknown`，并记录 reason code `no_stable_direction`。

`no_supported_correspondence` 比“绝对无关”更严谨。有限探针无法证明两个事件在所有可能张力下永远无关；系统只能说当前查询和预算内没有得到有证据支持的对应。没有证据建立映射不等于有证据证明无映射。

六类按唯一优先级产生，不能在两个字段间自由拼接：

1. 所有仍允许的映射假设都被同一范围内的强必要命题或不变量击穿，才是 `contradicted`；若仍有可行假设，保留假设集合或返回 `unknown`。
2. 有充分证据表明所有允许映射都结构不兼容，但不存在上述同范围逻辑冲突，才是 `no_supported_correspondence`。
3. 映射证据不足、允许假设无法消歧或方向签名不稳定，返回 `unknown`。
4. 只有 `mapping_status=supported` 后才判断方向：预冻结 `mixed(P)` 各块分别通过且两个纯类均失败时输出 `mixed`；否则仅 `aligned` 门通过时输出 `aligned`，仅 `opposed` 门通过时输出 `opposed`；其他情况输出 `unknown`，可附 `no_stable_direction`，但不得新造第七类。

#### 5.3.4 两个最小判定例

**坐标重命名不应改变类别。**查询写“增加阻尼会降低振幅”，候选写“增加阻尼会提高稳定性”。若查询结果正轴是“振幅增加”，候选结果正轴是“稳定性增加”，则 \(g_Q=-1\)、候选原始符号为 \(+1\)，但冻结映射给出 \(\sigma_O=-1\)，搬到同一坐标后两者仍是 `aligned`。系统不能把不同指标命名造成的表面正负当成逆向。

**真实响应反转才可能得到逆向。**在冻结“反馈强度增加”为干预正轴、“偏差增加”为结果正轴后，若负反馈情境中增益增加使稳态偏差下降，而正反馈情境中同一功能量增加使偏差上升，则两边为 \(-1\) 与 \(+1\)，角色和坐标校正后仍相反，才形成待验证的 `opposed`。这里的反馈符号是必须报告的调节条件；证据不够时只能是 `unknown`，不能从词面或 cosine 猜测。

#### 5.3.5 决定同向或逆向的隔离顺序

判定顺序必须固定为：

~~~text
目标侧先冻结张力、探针、结果轴正方向和响应假设
→ 映射器只看双方角色、轴定义、条件及允许的 baseline／orientation evidence，响应方向 span 保持遮蔽
→ 冻结 π、σU、σO 以及允许的 mixed 分组
→ 候选回答器独立回答探针，不看目标隐藏响应
→ 比较器机械计算 χ、A、R、coverage 和硬冲突
→ 先输出结构化方向类别
→ 最后才允许生成自然语言解释，解释不得改写类别
~~~

稳定输出 `aligned` 或 `opposed` 至少要求：

1. 查询侧有明确张力、干预轴和结果轴；
2. 双方功能角色、控制位置、时间尺度和必要调节项可比；
3. \(\sigma_U\) 与 \(\sigma_O\) 有独立证据且在揭封响应前冻结；
4. baseline evidence 与 response-direction evidence 分离；
5. 至少两个非冗余中强证据探针，至少一个 `internal_audit_heldout`；锁定结论另需一次性 `external_challenge`；
6. 表层保持对照不改变方向类别；
7. 单纯反向命名某个坐标并同步更新 \(\sigma\) 时类别保持；在 \(\Pi\) 与 \(\sigma\) 不变时真实翻转候选全局响应极性，才使 `aligned ↔ opposed`；单点真实响应翻转使纯类退化为预冻结 `mixed` 或 `unknown`；
8. 删除关键证据后必须退化为 `unknown`，不能继续高置信补全。

少于上述证据、正控制不稳定、负控制不敏感、轴方向需在看见响应后决定、关键调节条件缺失，或置信区间跨越分类门时，必须返回 `unknown`。

#### 5.3.6 为什么通用向量和 cosine 不能承担这个职责

这里需要使用一个准确而不过度的结论：**未经本任务验证的通用单向量 cosine 不能单独判定机制同向、机制逆向、逻辑矛盾或无关。**这不等于“所有向量模型永远不可能学习方向”。经 NLI、反义约束、关系监督或专门对比学习的成对编码器可以学习部分方向信息，但它们仍是需要独立验证的预测模型，不能替代角色、条件、极性和原文证据审计。第 28–30 条参考文献分别说明了通用分布向量需要外部反义约束、分布空间会混合多种关系，以及向量算术只在部分关系类型上可靠。

通用 cosine 的根本问题是：

- cosine 作为对称比较算子本身不输出角色或因果方向标签；编码器是否保留了这些信息、下游能否可靠解码，必须另行验证；
- 负 cosine 只表示相对当前坐标原点的夹角，不具有稳定的“语义相反”解释；
- 同义、反义、蕴含、主题相关和角色共现可能在通用分布空间中混在一起；
- “A 导致 B”“A 不导致 B”、施动者交换或反馈符号反转，可能仍有很高文本相似度；
- 低相似度同时混合了跨领域、无关、表达差异和真正逆向，不能自动提供关系类型；
- 同一事件相对于不同张力、结果轴、时间尺度和基线区间，可以分别表现为同向、逆向或 unknown；
- `Enc(after)-Enc(before)` 可以作为候选召回特征，但没有冻结坐标、调节条件和方向证据时，不能证明局部响应符号。

因此新的职责划分是：

> 向量负责“可能想起谁”；角色映射后的有符号响应负责“它与当前张力在哪些轴上同向、逆向或无法判断”。

向量仍可用于宽召回、候选去重、多样性配额、表层近远分层和困难负例采样。它不得单独成为类比真实性、`opposed`、`contradicted` 或 `no_supported_correspondence` 的决定性门禁，也不得覆盖有证据的方向审计。

#### 5.3.7 哲学与数据边界

- 所有方向类别只对当前 request 有效，不写回真实记忆或共激活边；
- 不新增永久关系类型，不把它变成用户维护的知识图谱；
- `aligned` 不替模型决定接受，`opposed` 不替模型决定反着做；
- `contradicted` 只否定当前局部映射，不宣布原记忆为假；
- 不触发拒绝、permit、计划执行或行为控制；
- 不修改 `plan`、`pinned`、`anchor`、`I` 或原文；
- 只在显式请求 inspiration 时运行；
- 输出必须附双方证据、坐标方向、探针、适用条件、unknown 和失效边界。

### 5.4 张力

张力不是泛泛主题，而是尚未闭合的具体结构。例如：

~~~text
目标：保持系统可用
障碍：输入超过共享瓶颈
当前行动：增加并发处理
观察到的反馈：并发越高，争用越严重
未闭合问题：怎样改变输入或反馈结构才能稳定系统
~~~

“工作压力”“关系问题”“想寻找灵感”都过于宽泛，不能直接生成可靠探针。

张力至少需要：

- 一个可识别目标；
- 一个阻碍或约束；
- 一个正在发生或可能发生的状态变化；
- 一个本轮希望获得的新视角；
- 对关键未知项的明确标注。

### 5.5 文本变形与机制探针

针对张力生成有限个候选文本变形或机制探针：

\[
T_Q=\{\tau_{u_1},\ldots,\tau_{u_k}\}
\]

每个探针原则上只改变一个预先声明的局部因素，并附适用前置条件。例如：

- 删除某个行动；
- 交换两个行动的顺序；
- 改变反馈符号；
- 增加或减少反馈延迟；
- 转移控制权；
- 放松或收紧关键约束；
- 改变资源供给；
- 改变信息可见性；
- 改变可逆性；
- 改变参与者之间的依赖方向。

单段自然语言叙事通常不满足结构因果模型的识别条件，因此本文不使用 Pearl 意义上的 do 记号。每个探针的目标侧冻结响应假设写成：

\[
\widehat D_Q(\tau_{u_j};\mathcal H_Q,\Xi_Q)
\]

它表示模型根据当前文本、显式关系、有限推导和证据范围形成的“结构化文本变形后定性响应假设”，不表示现实世界真实干预分布。若多种机制都与文本兼容，应保留响应假设集合，不得压成一个伪确定答案。

响应不能把“增强、延迟、消失、分叉、未知”混成同一互斥概率空间。建议按轴表示：

~~~text
direction: 增强 | 减弱 | 保持 | 翻转
timing: 提前 | 延迟 | 保持
topology: 消失 | 分叉 | 合并 | 保持
activation: 出现 | 消失 | 保持
missing_mask: 已知 | 未知
assumptions: []
~~~

### 5.6 证据等级

每个响应必须标注来源：

1. 强证据：原文明确陈述动作、条件和结果。
2. 中证据：可由相邻事件和明确关系局部推导。
3. 弱证据：需要模型补全未写出的机制。
4. 未知：文本不足以判断。

纯弱证据不能单独让候选通过。未知必须保留，不能为了完成结构图而自动填满。响应一致性只在双方共同可观察的轴上计算；双方都未知不会贡献一致分，只会降低 coverage。

### 5.7 检索前盲封

本节原本描述的是“查询侧对象在读候选前冻结”，它与新版“来源结果在检索／映射／预测完成前盲封”是两条不同的隔离轴。SOS-PAR 严格路径必须同时满足两者；只做到本节而仍让检索器看见来源结局，不能称为结果盲封。

目标张力的表示、探针池和目标侧响应假设集必须在候选记忆被读取前生成并冻结。

建议流程：

1. 张力编译器只能访问当前上下文；
2. 使用多个预冻结 seed 重复或相互不可见的跨家族模型产生草案；seed 只代表重复测量，不代表独立证据；
3. 只保留有证据且多次一致的核心；
4. 冻结探针池、目标侧响应假设集、证据位置、前置条件和未知项；
5. 将探针预先拆分为抽象层选择集、主动验证池、算法内部审计池和由标签持有方保管的外部挑战池；
6. 候选验证器只能看到公开探针、规范角色和候选映射假设，不能看到目标侧冻结响应；
7. 候选侧独立回答后，才由第三个比较阶段揭封对比。

这些“冻结响应假设”不是 gold answer。盲封只能减少候选泄漏和事后共同点，不能让模型预测自动变真。确认实验中的目标响应必须由受控程序、明确文本证据或独立人工判断提供外部依据。

## 6. 动态相邻抽象层带

### 6.1 抽象梯度

每个张力生成预先规定范围内的离散抽象梯度：

| 层级 | 内容 |
|---|---|
| L0 | 原始文本与具体实体 |
| L1 | 去除专名、日期和偶然对象 |
| L2 | 功能角色、行动顺序和状态变化 |
| L3 | 目标、约束、反馈、控制与结果机制 |
| L4 | 更高阶系统模式 |

L4 不是越抽象越好。像“主体受到约束后作出调整”这样的结构覆盖面过大，几乎不能区分类比与无关记忆。

L0–L4 不是连续实数轴。只有当相邻层之间存在预先定义的抽象变换，并且人工审计确认较高层没有偷偷加入新机制时，才能把若干相邻层称为“层带”。若嵌套关系不成立，系统只能报告“多个可接受层”，不能假装存在连续区间。

### 6.2 两类扰动

对每个抽象层，从预先冻结的算子族生成两类候选对照。它们的类别不是由名称保证，而是由前置条件和独立审计决定。

这里的“表示保持／机制破坏”是**对照生成方式**，不是第 5.3 节的“同向／逆向”关系标签。正控制检验表示在不改变目标机制时是否稳定，负控制检验破坏关键机制后是否仍被误判；同向／逆向则比较两个事件在冻结角色和坐标轴后的响应方向。一个逆向对应可以通过严格对照而得到支持，不能因为它叫“逆向”就被当作负控制、矛盾或错误样本。

候选表示保持变换 P：

- 人物改名；
- 在材料属性、可逆性、时延和控制结构均不变时做领域替换；
- 同义改写；
- 在关键功能属性不变时做具体对象替换；
- 叙述风格变化；
- 在不改变因果顺序的情况下压缩或展开文本。

候选机制破坏变换 B：

- 施动者与受动者交换；
- 因果方向反转；
- 否定极性变化；
- 在顺序已被证据支持为机制相关时改变行动顺序；
- 反馈由负转正或由正转负；
- 在必要性已有外部依据时删除该约束；
- 改变控制权；
- 将可逆条件改成不可逆；
- 保留结果但替换产生结果的机制。

每个变换必须声明适用前提、意图保持的量、意图改变的量和证据。领域或对象替换若同时改变材料属性、可逆性、时延或控制条件，应标为 `unknown` 或重新归入机制破坏，不能强行作为正控制。顺序变化若对该机制无关，也不能被预标为负控制。

机制破坏变换应尽量匹配词汇、长度、主题、自然度和信息量，防止系统通过合成痕迹识别负例。确认实验中的类别必须来自已知生成程序或与算法隔离的人工审计，不能由同一模型同时生成、分类和验证。

### 6.3 对比响应判据

在查询 Q 的抽象层 a 上定义开发期经验目标：

\[
M_Q(a)=
\operatorname{Stability}(P,a)
-
\operatorname{Leakage}(B,a)
-\lambda_q\operatorname{RepresentationComplexity}_Q(a)
-\lambda_g\operatorname{Genericity}(a)
+\lambda_e\operatorname{EvidenceCoverage}(a)
-\lambda_u\operatorname{UnknownRate}(a)
\]

其中：

- Stability 在查询阶段是表示保持变换上查询表示稳定的比例；
- Leakage 是经审计的机制破坏变换上，系统仍错误通过的比例；
- RepresentationComplexity 是查询表示与查询机制假设族的描述复杂度，此时还不存在候选映射；
- Genericity 是该表示在确认前冻结的开发参考分布或固定困难负例统计量上的错误覆盖率，或等价的低区分信息量；它不得读取本轮真实候选池；
- EvidenceCoverage 是可追溯到原文或受控程序的轴占比；
- UnknownRate 是无法由证据判断的轴占比。

这些量的取值域、估计器、算子权重和阈值必须在开发阶段定义，并在打开确认集前冻结。空泛高阶抽象往往很短，因此不能只靠“复杂度惩罚”防止过度抽象；Genericity 和困难负例泄漏才是主要约束。

候选出现后，在相同冻结估计器下另算 \(M_{Q,M}(a)\)：此时用候选局部映射的 `MappingComplexity` 代替查询表示复杂度，Stability 指映射在正控制上的稳定性，Leakage 指候选在负控制上的错误通过率。\(M_Q\) 只决定允许层，\(M_{Q,M}\) 只能在允许层内缩窄；二者不得混用。

### 6.4 冻结查询允许层，再由候选缩窄

先在候选不可见时，根据查询侧证据、表示保持控制和空泛度控制冻结允许的相邻离散层集合：

\[
A_Q=\{L_p,L_{p+1},\ldots,L_q\}
\]

其中相邻性只有在层间抽象变换已定义时成立。查询侧只能判断表示本身是否稳定，不能提前声称候选映射稳定。

候选出现后，对候选 M 得到实际支持带：

\[
A_{Q,M}\subseteq A_Q
\]

候选只能缩窄查询允许层，不能为了通过而扩展到更高抽象。若有多个不相邻层满足条件，应逐层报告，不能将中间失败层补成一个带。

允许层与候选支持层需要同时满足：

- 保持扰动稳定；
- 破坏扰动敏感；
- 在预先冻结、未参与层选择的算法内部审计扰动上仍成立；
- 原文证据覆盖达到要求；
- 不依赖单个 seed；
- 不包含明显空泛的超高抽象。

探针必须在候选出现前分为四个互不重叠的用途：抽象层选择集、主动验证池、算法内部审计池、由标签持有方保管且算法完全不可见的外部挑战池。外部挑战池不参与排名、阈值、校准、早停或任何实现决策。

如果不存在有区分力的允许层或候选支持层，应返回：

~~~text
本轮张力无法形成具有区分力且有证据支持的抽象表示。
~~~

### 6.5 先验证真实文本能否支撑形式化

在实现响应矩阵或主动层析前，先对**恰好 18 个**单一 episode 做一次只读证据充分性审计：行动—反馈—结果较完整、中等完整、稀疏／反思性三层各 6 个。这里的分析单位是人工按预注册规则切分的一次具体事件，不是整个 bucket；同一 bucket 含多个事件时不得把整个 bucket 算作一个样本，也不得把其中多个探针当作多个独立 episode。来自同一 bucket、人物或来源的 episode 在后续统计中必须保留聚类标识。

该审计只回答“当前记忆文本是否可能提供足够证据运行 WIT”，不回答 WIT 是否能正确筛选类比，也不能据 18 条样本宣称整个记忆类型已经适用。

阶段 0A 先对允许读取的只读快照建立**未按轴适用性过滤**的完整抽样框。两名看不到后续响应假设和方法结果的人工只按冻结规则判定完整度；“轴是否适用”在抽样后标注，不能用于排除零适用轴事件。随后用预注册随机种子在每层无放回抽取 6 个。若某层不足 6 个，不得从结果较好的层补齐，必须报告抽样框不足并暂停或重新批准范围。样本分层为：

- 行动、反馈和结果较完整的事件记忆；
- 缺少部分条件或结果的中等完整记忆；
- 稀疏、反思或情绪性记忆。

同时覆盖不同文本长度、时间跨度和来源。当前 18 条最小筛查只针对阶段 0A 预注册的普通情节记忆候选范围；`pinned`、`anchor`、`I`、`plan` 等职责不同的结构既不能混入同一个总体，也不能依据这 18 条被宣布适用。审计必须在只读副本、独立测试 vault 或本地脱敏材料上运行，不向未经项目所有者明确批准的外部 API 发送私人记忆。

人工先把每个轴标成三个互斥状态：

~~~text
supported：该轴适用，且原文有足够证据
unknown：该轴适用，但原文证据不足
not_applicable：该轴对该 episode 不成立
~~~

`not_applicable` 不进入 EvidenceCoverage、UnknownRecall 或 SupportedFieldRecall 的轴级分母，但所有已抽 episode 始终保留。若某个已抽 episode 没有任何适用轴，它在本层 TextEvidenceCoverage 与 UsableEpisodeRate 整数门中按未通过处理，不能在看过轴标签后删样或补抽。若某 episode 没有“适用但缺证据”轴，UnknownRecall 在该 episode 记为未定义，只在确有这类轴的预注册汇总分母上计算，不能把零分母当作满分。SupportedFieldRecall 同理。先在**完整抽样框**报告 `AxisApplicableRate`（至少一个适用轴的 episode／全部抽中 episode），再用人工 gold 衡量文本内在可用性：

\[
TextEvidenceCoverage=
\frac{\text{有原文支持的适用轴}}
{\text{全部适用轴}}
\]

若人工审计显示文本内在证据达到预注册门槛，再用阶段 0A 冻结的至少两个模型家族分别测试自动抽取可行性：

\[
UnknownRecall=
\frac{\text{系统正确标为 unknown 的“适用但缺证据”轴}}
{\text{人工确认“适用但缺证据”的轴}}
\]

\[
SupportedFieldRecall=
\frac{\text{系统找到的有证据轴}}
{\text{人工确认有证据的轴}}
\]

两个模型家族不得先聚合，应分别报告各自重复结果和跨家族共同错误。还必须报告：非 unknown 断言的证据精确率、baseline evidence 被误当成 response-direction evidence 的比例、每个 episode 可形成的中强证据探针数，以及不同预注册分层上的分布。

“两个探针”必须是两个非冗余诊断探针，而不是统计独立探针。它们至少改变不同局部因素或检验不同响应轴，不是同一句话的同义重写，不共享完全相同的推导链，并由看不到模型身份的人工按冻结规则判断冗余。

本文将五种 coverage／可用率分开，禁止混用：

| 名称 | 统计单位与分母 | 门限 |
|---|---|---|
| AxisApplicableRate | 至少一个适用轴的 episode／完整未过滤抽样框；零适用轴保留在分母 | 每层整数门 \(k^{axis}_{min}\)，不得条件化删除 |
| TextEvidenceCoverage | 单个 episode 中 supported 轴／全部适用轴；先按 episode 算，再在每层统计达到 episode 门的绝对个数 | episode 门 \(c^{text}_{min}\) 与每层整数门 \(k^{text}_{min}\) |
| UsableEpisodeRate | 每层 6 个 episode 中能形成至少两个非冗余中强证据探针的比例 | 描述量 \(p^{usable}\) 与预注册整数门 \(k^{usable}_{min}\)；\(p^{usable}=k/6\)，不再称两者为统计等价的 CI 门 |
| PairResponseCoverage | 对协议要求进入结构响应验证的高结构候选，计算查询—候选—探针上双方共同已知响应轴／双方适用响应轴并集；按下文固定次序聚合 | \(c^{pair}_{min}\) |
| RejectionEvidenceCoverage | 低结构候选中，系统对全部原始 completion／codebook／允许 \((T,\omega,k)\) 成员给出穷尽排除证书，或给出协议预提交且由 solver 证明被普遍违反的必要不变量，并由独立 `typed_negative_audit` checker 通过后返回 `mapping_status=no_supported_correspondence` 的候选数／全部低结构候选数；单个局部 mismatch、checker 失败或仅 pool signal 均不能代表普遍不可能 | \(c^{reject}_{min}\) |
| OutputCoverage | 至少产生一个非 reject 候选且形成有效全预序的目标数／全部目标数；全 reject、目标级 abstain 和失败调用不得从分母删除 | \(c^{output}_{min}\) |

PairResponseCoverage 的聚合顺序必须固定。对每个方法、目标、候选和重复，先在同一探针内对预注册且适用的 \((a,g,h)\) 单元等权平均，再对探针等权平均；高结构候选上的任一空分母按 0，不得删除。随后只对协议要求建立响应结构的高结构候选集 \(\mathcal Q_t^+=\{far,near\}\) 取最小值，最后先在同一目标内平均重复，再按全部冻结目标聚类汇总：

\[
C^{pair}_{m,t}=\frac{1}{J}\sum_j\min_{c\in\mathcal Q_t^+}
\left[\frac{1}{|P_t|}\sum_{p\in P_t}
\frac{1}{|A_{t,c,p}|}\sum_{(a,g,h)\in A_{t,c,p}}Coverage^{a,g,h}_{m,t,c,p,j}\right]
\]

这里每个探针权重相同，不能让拥有更多轴或更多假设的探针支配平均。表面假朋友与无关候选属于低结构集合 \(\mathcal Q_t^-\)，正确结果通常正是“不建立共同响应映射”，因此不得因适用共同轴为空而把它们计成 PairResponseCoverage 失败；它们改由 RejectionEvidenceCoverage 检查是否有证据地拒绝，而不是奖励无理由 abstain。只有“穷尽所有合法 \((T,\omega,k)\) 版本成员”或“违反预注册的普遍必要不变量”的 solver 证书才算有证据拒绝；一个操作、一个轴或一个完成上的 mismatch 只足以淘汰对应分支。单独输出 `unknown` 不构成有证据的 `no_supported_correspondence`，不能计入正确拒绝分子。

\(T\)、\(\mathcal Q_t^+\) 和 \(\mathcal Q_t^-\) 在运行前冻结，任何 coverage 都只能作为额外门，不能据此删除目标。PairResponseCoverage 与 RejectionEvidenceCoverage 只约束产生响应轴／结构拒绝证据的静态结构路径、WIT 路径和组合臂中的 CUT 验证组件；不强行套给没有这一构念的 CMI。所有方法共同受到 OutputCoverage 和失败预算约束。

不能把“unknown 越少”当成优化目标。高 unknown 可能表示系统诚实；真正需要否决的是为了提高 coverage 而补全原文不存在的机制。

阶段 0A 必须在看结果前，为每个候选记忆分层分别冻结：\(k^{axis}_{min}\)、\(c^{text}_{min}\) 与 \(k^{text}_{min}\)、\(k^{usable}_{min}\)、最低非 unknown 证据精确率、SupportedFieldRecall 下限、UnknownRecall 下限、response-direction 证据混淆率上限、跨家族共同假阳性率上限，以及每个率指标的最小有效分母。还要冻结每项按 episode 还是按轴聚合和边界等号规则。

由于 18 条只用于明显不可行筛查，阶段 0B 使用上述**绝对计数／点估计门**，置信区间只作描述，不能与整数门并列产生第二套结论。两个模型家族必须分别通过全部自动抽取门；一方失败或有效分母不足，该分层就不通过，不能先聚合或多数表决。未来若用更大独立样本确认正式产品范围，才使用第 16.1 节的 CI 方向门。所有门必须联合通过；不能靠全部输出 unknown 或大量无证据补全通过。

18 条只用于明显不可行筛查和形成开发期适用范围假设，不计算确认性显著性，也不训练校准器。“普遍无法形成探针”的操作定义是：完整组达到两个非冗余中强证据探针的 episode 数低于阶段 0A 冻结的 \(k^{usable}_{min}\)；发生时停止完整 WIT。若只有某些预注册分层通过全部联合证据门，0B 后必须冻结 WIT 的开发期允许范围，0C、0D 和阶段 1W 不得再扩展。未来若要形成正式产品适用范围，仍需更大、独立样本确认。

## 7. 候选生成

### 7.1 召回只负责不漏掉

候选阶段只追求较高 Recall，不负责证明类比成立。DSR-CT 的发现与验证两条严格路径都只能读取各自 cutoff 前、结果盲且 provenance 完整的字段；读取完整记忆的旧 Spark 只能作为泄漏正控，不能进入候选发布路径。发现索引、验证索引与结果可见正控不能共享全文向量、摘要、排名缓存或模型会话状态。

建议候选来源：

1. 只由 \(t_0\) 前原文构建的 BM25／sparse 索引；
2. 只由 \(X_i,A_i\) 构建的 outcome-blind dense embedding；
3. 行动前情境—角色—约束—行动路径视图，明确排除反馈和结果；
4. 从查询无关、结果盲 CEF 派生并删除人物／领域名的前因机制骨架视图；
5. 经结果置换不变性验证的结果前元数据通道；
6. 小比例跨领域多样性配额；
7. 共激活邻居只作为单列消融；若其形成时间晚于结果或可能受结果驱动，则标记 tainted，不进入严格主分析。

严格索引必须保存字段级 provenance、切分器版本和每个向量／倒排项的来源 span。CEF 结构索引只允许由结果前原文锚定的已支持原子派生，source hash 或盲封边界改变即失效。召回查询可以由 Need Frame 选择已存在的角色／约束视图，但不能在召回阶段重新抽取一套更迎合当前问题的候选结构。共激活只表示同一认知窗口共同出现，不表示关系类型或类比真值。

旧方案的状态变化视图：

\[
v_\Delta=Enc(\text{after})-Enc(\text{before})
\]

因为显式使用 `after`，**不得进入 SOS-PAR 严格召回或映射**。它只保留为结果可见的泄漏正控：若该条件显著优于完整盲封条件，说明结果确有可用信息，但不能把该性能记作自动筛选能力。

### 7.2 不平均异质向量

原文语义、行动路径、机制视图和图邻居不是同一种信号，不应未经校准和消融就假定简单加权平均能形成有效、可解释的“统一类比距离”。

严格候选合并只在通过 taint 检查的结果盲通道间使用：

- 每通道配额；
- Reciprocal Rank Fusion；
- 候选并集；
- 通道支持数作为后续特征。

具体策略由开发集消融确定。

### 7.3 表层距离的位置

未经本任务验证的通用文本 cosine 不能单独参与类比真实性、同向／逆向、矛盾或无对应的决定性门禁。专门训练的关系编码器可以作为待消融的辅助模型，但必须与有符号响应证据分开报告，不能把其分数重命名为结构真值。

| 向量适合承担 | 向量不能单独承担 |
|---|---|
| 宽召回主题、实体、场景和粗语义候选 | 决定谁是施动者、受动者或控制者 |
| 候选去重、通道融合和多样性配额 | 决定干预与结果轴的正方向 |
| 表层近远描述和困难负例采样 | 区分 `aligned`、`opposed`、逻辑矛盾与无关 |
| 为专门关系模型提供输入特征 | 把负 cosine 解释成反义或反向机制 |
| 提出 `Enc(before/context/action)` 的结果盲表示并等待验证 | 使用 `Enc(after)-Enc(before)` 或全文向量冒充结果盲证据 |

DSR-CT 的正确顺序是：

1. 只用结果前向量、BM25、结构视图和可选 Need-Path probes 形成发现高召回池，并先做结果置换不变性测试；
2. 允许 discovery seed 生成新的、span-grounded \(H_C\)，立即冻结 MechanismCard、最不利 rivals、轴、边界和验证策略；
3. 从不同 event cluster 的验证盲索引检索自然 analogue／bridge／foil／null，冻结变量长度 panel 与各机制概率；
4. 同一 query 的全部候选先经 run-level barrier 冻结，再对去重 validation outcomes 并集统一揭封；单候选只产生 `candidate_audit_receipt`，按事件簇级 proper-loss regret、query-free 与 query-aware-no-discovery 双对照、bridge-only 门和结果盲对照特异性形成校准特征；
5. 只有在独立 calibration／confirmation 上通过 query-level 来源事件外验证门的方法版本进入历史 rolling-origin；当前 live target 的 future shadow 绝不回流本次调用；
6. 只有在消融证明有独立增量时，少量通过者再运行精简 WIT；CUT 只对 WIT 已支持的轴派生 `aligned / opposed / mixed`，证据不足返回 `unknown`；
7. 最后才按适切性下限、历史 transport reliability、固定覆盖率风险和成本发布；表层距离仅描述近／远，不决定真假。

如果在结构门禁前把表层距离作为独立正向奖励，可能增加系统偏向“看起来新颖但实际无关”材料的风险；该风险及幅度仍需由对照和消融验证。

### 7.4 Need-Path probes：提高远机制召回但不越过自主性边界

每个 query 同时运行两条 discovery recall：

1. 原查询／Need Frame 直接召回；
2. 由只看 Need Frame 的独立生成器产生不超过预冻结 \(K_{probe}\) 个 informational-need probes 后取并集。

允许的 probe 只询问证据形态，例如 `constraint_probe / boundary_probe / temporal_probe / feedback_probe / rival_discriminator_probe`。禁止 `next_action / user_intent / refusal_reason / permit_condition / plan_step`。生成器输出不进入最终 prompt，不持久化，不形成边；probe 数、token 预算、通道配额和 RRF 规则在确认集前冻结。

必须报告 `Recall@64`、新增相关候选数、无关候选增量、独立事件簇覆盖和每次 query 成本。若 Need-Path 仅扩大噪声或其增益不超过直召回的不确定区间，就删除该组件。

### 7.5 discovery recall 与 validation retrieval 是两个不同任务

发现检索的目标是提出可能的新机制，允许高多样性；验证检索的目标是最大化冻结 \(H_C\) 与 rivals 的可区分证据并优先寻找反例。两者必须：

- 使用不同候选集合与 event-cluster exclusion；
- 分别记录 recall、panel coverage 和 selection provenance；
- 不共享由 discovery seed 结果、模型解释或已揭封结果生成的查询扩展；
- 在全尺寸 vault 或等价规模的冻结快照上评估，不能只在 32–64 条人工候选池报告 `Recall@8`；
- 对不完整相关性标注采用池化盲审与未判定项敏感性分析，不能把未标注候选自动当无关。

## 8. DSR-CT 竞争预序裁决与条件性的版本空间主动层析

### 8.0 DSR-CT 的预序裁决先于主动层析

主动层析会根据已经观察到的信息选择下一探针，因此极易在结果揭封后变成适应性找证据。新版规定：

- DSR-CT 第一版使用揭封前冻结的多事件 contrast panel，不做根据已揭结果继续找支持证据的自适应循环；
- discovery seed、机制卡、验证检索规则、rivals、映射、结果轴和全部概率分布提交完成前，不得运行任何依赖真实 \(Y_i\) 的主动选择；
- 揭封后的 paired regret 只能与强基线门、自然 foil／null、负控和共同风险校准一起形成候选审计回执；三到四个事件的单候选不能生成“统计确认”证书。单一 Brier 差不能升级候选，也不得触发补召回、重写机制或重新预测；
- 第二版若研究 robust-EPIG 主动最小揭封，只能在已经存在且结果仍封存的来源 probe 中选择；最大自适应揭封预算 \(B_{reveal}\) 由独立 calibration 的信息增益—偏差—成本消融预冻结，并始终额外保留未参与选择的 final holdout event clusters；
- 主动策略的选择日志、停止规则和 ambiguity set 必须预提交；若无法完成选择后校正，只能作探索性研究；
- 主动层析只能在离线开发期、对已经通过固定预序门的候选研究，并必须使用独立 heldout 评价；
- 若 `WIT-slim` 相对 flat transport verifier 没有预注册增量，本节 8.1–8.6 不进入在线方案。

因此，本节后续内容是**条件性的审计扩展**，不是 DSR-CT 阶段 0D／1D 的必要组件。主动信息增益、WIT 版本空间或 CUT 方向层若没有独立增量，应删除而不是无条件叠栈。

### 8.1 先冻结候选映射版本空间

本节是条件性的 Spark-CUT 验证层。旧 SOS-PAR adapter 必须先按第 5.2 节形成 \(\mathcal V_{release}\)；DSR adapter 则必须在 validation outcome 揭封前，随 run-level candidate package 冻结同构的程序—完成—codebook 并集版本空间，并且只有该候选的来源审计与共同风险校准通过后才可解释其增量。两条 adapter 不共享旧 `localized_HF/HR` 状态。任何地方写到“映射假设族”都指冻结版本空间中的成员；程序投影 \(\mathcal H\) 只用于展示，不能丢掉完成与 codebook membership。不能在响应揭封后新增映射，也不能只挑其中表现最好的一项发布。若本节单映射统计与版本空间稳健门冲突，以稳健门为准并返回 unknown。

响应矩阵不能先于角色映射建立。对每个候选 M，系统先仅根据公开 Need Frame、查询侧结构和候选的查询无关 CEF 遮蔽视图（允许字段见下文），按允许抽象层 a 与候选机制假设 h 综合局部映射假设族：

\[
\Pi_{QM}=\{\pi^{a,h}_{QM}:a\in A_Q,\ h\in\mathcal H_M^a\},
\qquad
\pi^{a,h}_{QM}=(\pi_R,\pi_X,\pi_U,\pi_C,\pi_O,\pi_T)
\]

随后直接沿用第 5.2.4 节的相容域与完成对特定近优界，取得当前候选切片：

\[
\mathcal V_{release,QM}=\{v=(\pi,\omega,k)\in\mathcal V_{release}:
v\text{ 对应候选 }M\},
\qquad
\mathcal H_{release,QM}=\operatorname{proj}_{\pi}(\mathcal V_{release,QM})
\]

后续张量、探针和方向比较必须保留 \(\pi\) 维度；原公式为简洁省略该下标，不表示可以先聚合掉版本空间分歧。

它分别映射功能角色、状态轴、探针可改变变量、约束或调节条件、结果轴和时间尺度。对每个有序映射轴还必须提出方向签名：

\[
\chi^{a,h}_{QM}=(\sigma_U,\sigma_O),
\qquad \sigma_U,\sigma_O:axis_{ord}\rightarrow\{-1,+1,\bot\}
\]

序列化时 \(\bot\) 写作 `unknown`。每一项都是允许未定义的部分映射；任一必要签名为 \(\bot\) 时对应单元直接为 `unknown`，不能参与乘法。映射器不得访问冻结的目标响应假设；完整的“层 → 机制假设 → 部分映射 → 方向签名 → 允许 mixed 分组”集合必须在候选回答探针前冻结，之后只能按预注册规则因证据冲突而淘汰，不能新建、改写、逐探针翻转符号或只保留事后最有利组合。

这需要字段级信息防火墙，而不只是换一个 prompt。映射器与轴定向器只允许读取角色、变量与轴定义、基线、时间尺度、调节条件，以及说明轴哪一端是“增加／减少”的 `orientation_evidence`；候选原文中直接透露“干预后结果向哪边变化”的 span、`response_direction_evidence` 和候选探针答案必须遮蔽。`orientation_evidence` 及其派生字段也不得由被遮蔽的响应 span 生成，每个输入字段都记录 provenance、内容 hash 和生成版本。若不查看这些被遮蔽内容就无法决定 \(\pi\)、\(\sigma\) 或 mixed 分组，必须把相应项标为 \(\bot\)／`unknown`，不能以全量原文泄漏换取可判定性。候选回答器在冻结完成后才读取允许的完整证据并独立回答探针。

允许的 mixed 分组不是每个案例临时生成。阶段 0A 必须冻结一个有限的全局分组库、每个分组的语义定义、最大块数、确定性选择规则或多重比较校正，以及每块的最低证据、`internal_audit_heldout` 门和 `external_challenge` 门。若多个仍允许的 P 给出不同最终类别且冻结规则不能唯一消歧，返回 `unknown`；不得挑选最有利分区。每个被接受的符号块都必须在未参与分组选择的内部审计单元上成立，并在最终主张时由独立持有、一次揭封的外部挑战单元复现。另以随机符号和随机分区 null 测量无结构样本被误报 `mixed` 的比例，并在阶段 0A 冻结 false-mixed 上限。

目标侧多个兼容机制假设 \(g\in\mathcal H_Q^a\) 只有在共享同一个映射相关公共核心时才能留在同一查询分支：功能角色、约束槽位、结果轴和探针语义必须一致。任何目标假设若改变这些映射对象，必须在读取候选前分裂为独立盲封查询，不能在看到候选后挑选分支。因此 \(\pi^{a,h}_{QM}\) 可以建立在公开公共核心上，而不读取目标响应方向。

### 8.2 分轴响应矩阵

对候选池 C 和预冻结主动探针池 U 建立保留层与机制假设索引的响应张量：

\[
\mathcal{R}_{i,j,a,h}=
\widehat D_{M_i}
\left(\tau_{(\pi^{a,h}_{QM_i})^U(u_j)};h,\Xi_{M_i}\right)
\]

候选响应与目标假设的比较另保留目标假设索引：

\[
\mathcal C_{i,j,a,g,h}=
d_{resp}\!\left(
\widehat D_Q(\tau_{u_j};g,\Xi_Q),
\mathcal R_{i,j,a,h}
\right),
\quad g\in\mathcal H_Q^a
\]

这里得到的是证据约束的文本响应假设，不是现实世界的因果干预分布。每个响应至少拆成：

~~~yaml
direction: strengthen | weaken | preserve | reverse | unknown
timing: earlier | later | preserve | unknown
topology: disappear | branch | merge | preserve | unknown
activation: activate | deactivate | preserve | unknown
intervention_positive_pole: ""
outcome_positive_pole: ""
sigma_u: +1 | -1 | unknown
sigma_o: +1 | -1 | unknown
time_scale: ""
missing_mask: []
assumptions: []
baseline_evidence: []
orientation_evidence: []
response_direction_evidence: []
~~~

基线事实证据与响应方向证据必须分开；前者不能冒充后者。候选侧独立回答映射后的探针并引用原文。比较器只通过不可读取内容的句柄取得冻结目标响应假设，候选回答者和映射器均看不到它。

### 8.3 已知共同轴上的相似度

设 \(J^{a,g,h}_{QM}(u)\) 为两侧都已知且被部分映射覆盖的响应轴，预注册轴距离为 \(d_r\in[0,1]\)，则单个探针、抽象层和目标—候选假设对的经验一致度可写为：

\[
S^{a,g,h}_{QM}(u)=
\frac{\sum_{r\in J^{a,g,h}_{QM}(u)}w_r[1-d_r(y^{Q,g}_r,y^{M,a,h}_r)]}
{\sum_{r\in J^{a,g,h}_{QM}(u)}w_r}
\]

并单独报告：

\[
Coverage^{a,g,h}_{QM}(u)=
\frac{|J^{a,g,h}_{QM}(u)|}{|J_Q^g(u)\cup J_M^{a,h}(u)|}
\]

在计算交集前，候选状态轴与结果轴必须先通过 \((\pi_X,\pi_O,\pi_T)\) 投影到查询的冻结规范坐标。`unknown` 永远不贡献一致分，只降低覆盖率。当 \(J^{a,g,h}_{QM}(u)=\varnothing\) 时，定义 `similarity=undefined`、`coverage=0`，该探针不得贡献一致分；若并集也为空，coverage 仍定义为 0。候选只有在其他预冻结探针达到最低已知轴覆盖时才能继续。

这里的 S 只概括多种类别轴的经验兼容性，不能单独区分同向与逆向。尤其不能通过把某种 `opposed` 响应的距离手工设小，就把反向关系伪装成普通相似。

若目标或候选存在多个与证据相容的机制假设，必须按预注册的集合比较规则处理，例如同时报告最好、最坏和范围，或采用保守聚合；不能事后挑选最配对的一组机制当作唯一解释。响应与比较张量在排序、审计和校准中始终保留 a、g、h 维度。

### 8.4 有符号方向比较器

对具有冻结正方向的有序响应轴，比较器先用 \(\chi^{a,h}_{QM}\) 将候选符号搬到查询坐标，再机械产生：

\[
q^{a,g,h}_{j,r}\in
\{aligned,opposed,neutral,mismatch,unknown\}
\]

在 `mapping_status=supported` 的前提下，方向分类只比较四个预冻结竞争假设：

\[
H_+:\text{稳定同向},\qquad
H_-:\text{稳定逆向},\qquad
H_{mix}(P):\text{冻结分组 P 上的混合方向},\qquad
H_0^{dir}:\text{映射成立但无稳定方向}
\]

证据不足时不在四者中强选，直接返回 `unknown`。\(H_{mix}(P)\) 的分组 P 必须在揭封响应前按结果轴、时间尺度、阶段或调节条件定义；没有冻结 P 的正负混杂只能是 `unknown`，并可记录 `no_stable_direction` reason code。\(H_0^{dir}\) 只是否定稳定方向，不能自动把已经成立的映射改成 `no_supported_correspondence`。

至少报告以下独立量，不能压成一个 similarity：

- `aligned_weight` 与 `opposed_weight`；
- `neutral_weight` 与 `mismatch_weight`；
- KnownCoverage、DirectionalCoverage 与 MismatchRate；
- 方向签名证据与分歧；
- `internal_audit_heldout` 是否复现；锁定评估时 `external_challenge` 是否复现；
- 强矛盾、调节条件缺失和未判轴；
- 全部允许 \((a,g,h)\) 假设对的类别范围。

同一映射轴在所有探针上只能使用一个冻结方向签名。若独立映射器对 \(\sigma_U\) 或 \(\sigma_O\) 不一致，需要看响应后才能决定符号，或关系可能跨越非单调转折点，方向比较器必须返回 `unknown`。

最后必须把类别范围扩展到冻结 CEF／DSL 假设类内全部 \(v\in\mathcal V_{release,QM}\)，包括所有 codebook membership。只有每成员 \(\mathcal D^{fit}_v\cap\mathcal D^{hold}_v\) 都是同一个已知完整 canonical key（`mixed` 时含相同 P 与块符号）、没有 unknown 时才允许强方向输出；“最低成本程序是 aligned，但第二低成本程序是 opposed”必须记为 `direction_nonidentifiable`，但若两者结构主张一致，可保留无方向的结构候选。不能用两者的成本差未经校准地强选 aligned。若成员数过多，验证器应优先搜索能产生另一完整 key 或 unknown 的反例成员，而不是只抽样若干看起来一致的程序。

### 8.5 探针区分力与主动选择

令 Z 表示“候选是否形成有效局部对应，以及哪一个冻结映射或机制假设成立”的潜在变量，R 为目前已观察的候选响应。若开发数据足以估计概率，可从冻结主动探针池中选择：

\[
u^*=\arg\max_{u\in U_{active}\setminus U_{used}}
I(Z;R_u\mid R_{used})-\beta\operatorname{Cost}(u)
\]

该分布只能由冻结的机制假设和预测响应估计。若没有可靠概率模型，就只能称为“启发式分区熵”：选择能把仍存活的冻结假设最均衡分开的探针，不能宣称计算了期望信息增益。

单独使用 \(-\log P_{null}\) 会奖励罕见噪声。任何稀有度项都必须乘以证据可靠性与跨重复稳定性，排除 `unknown`，并通过困难负例校验。更稳妥的目标仍是条件互信息或其冻结启发式近似。

主动阶段只能从候选出现前冻结的池中选择探针，不得读取外部挑战池，也不得看到目标侧响应内容。达到区分门槛、没有可区分探针或预算耗尽时停止；预算耗尽而仍歧义时应降级或返回空。

### 8.6 多候选而非只做两两打分

两两评分容易让模型为每一对分别编造不同的抽象理由。响应矩阵要求：

- 同一规范探针对所有候选使用相同定义；
- 候选之间共享响应轴和缺失值规则；
- 映射和机制假设在回答前冻结；
- 可观察候选如何划分冻结假设，而不是把罕见措辞当成高信息；
- 所有选择、成本、停止原因和剩余歧义均可重放。

这是 Spark-CUT 相对普通 LLM reranker 的核心操作差异之一，但是否带来实际收益仍由消融和锁定确认决定。

## 9. 局部迁移验证

### 9.1 部分映射与共同支撑

除正向并集版本空间 \(\mathcal V_{release,QM}\) 外，隔离的反向映射器在看不到任何正向输出的条件下独立提出反向程序族；它必须在同一个 \(\Omega^{orig}\)、\(\mathcal K\)、\(J_k\)、\(\epsilon_{max}\)、证据规则和 pre-response commitment 下形成 \(\mathcal V_{release,MQ}\)，不能另选对反向更有利的 completion、代码本或成本：

\[
\Pi_{MQ}=\{\rho^{a,h}_{MQ}\},
\qquad
\rho^{a,h}_{MQ}=(\rho_R,\rho_X,\rho_U,\rho_C,\rho_O)
\]

正反向映射均按层与冻结机制假设保留 completion／codebook provenance。第三个比较阶段才从两侧独立输出中派生一一对应共同核心 \(B^{a,h}\)，而不是由正向映射机械求逆。共同核心及 round-trip 审计必须覆盖每个证据相容完成、每个代码本和全部合法 \((v_{QM},v_{MQ})\) 版本成员配对；任一 membership 缺失、solver gap 或循环冲突都令该审计 unknown，不能只让代表程序通过。两者都只覆盖本轮张力相关的共同支撑，不要求整段记忆同构，也不要求两边信息量相同。映射至少审计功能角色、因果方向、否定和模态极性、关键时序、控制权、反馈方向与延迟、必要条件、环境调节项、结果轴和证据边界。

一一对应核心 B 上检查 round-trip residual：

\[
\rho^{a,h}_{cycle}(z)=
d\!\left(z,\rho^{a,h}_{MQ}(\pi^{a,h}_{QM}(z))\right),
\quad z\in B^{a,h}
\]

多对一区域没有普通逆映射，只能检查对应关系是否矛盾，不能声称可逆或 cycle-consistent。这里的“双向”只表示两个方向独立提出后的一致性审计，不表示信息对称。

### 9.2 响应对应条件

对探针 \(u_j\)，查询侧和候选侧分别产生集合值响应假设：

\[
Y_Q^{j,a}=\{\widehat D_Q(\tau_{u_j};g,\Xi_Q):g\in\mathcal H_Q^a\}
\]

\[
Y_M^{j,a}=\{\widehat D_M(\tau_{(\pi^{a,h}_{QM})^U(u_j)};h,\Xi_M):h\in\mathcal H_M^a\}
\]

结果、状态和时间轴经相应部分映射后，只在已知共同轴上用预注册的 \(d_{resp}\) 比较，并同时报告覆盖率、目标假设范围、候选假设范围与直接矛盾。确认前冻结的聚合规则必须保留并处理全部 \((g,h,T,\omega,k)\) provenance，不能用最佳配对或最低成本单一 T 覆盖最坏冲突。满足阈值仅表示在该证据范围内存在局部响应对应，不表示识别了真实 SCM，也不批准下游行动。

局部响应对应必须同时给出**结构是否可比**和**可比后的方向**，不能只给一个高低分：

~~~text
mapping_status: supported | no_supported_correspondence | contradicted | unknown
direction_class: aligned | opposed | mixed | unknown
~~~

唯一优先级是：所有允许映射假设均被同范围强必要命题击穿时为 `contradicted`；有充分证据说明所有允许映射结构不兼容时为 `no_supported_correspondence`；证据不足或映射假设不能消歧时为 `unknown`；只有 `mapping_status=supported` 后才定义 `aligned / opposed / mixed / unknown`。`opposed` 需要所有达到门槛的预冻结方向块跨非冗余探针保持系统性逆向；它不是映射失败。`mixed(P)` 需要至少一个同向块与一个逆向块各自通过，且两个纯类失败。`contradicted` 不能由反向响应自动推出；若仍有任一允许映射假设未被击穿，只能保留假设集合或返回 `unknown`。

### 9.3 算法内部审计探针

角色映射、候选排序和主动探针完成后，比较预先冻结且未参与上述步骤的算法内部审计探针。数量、选择规则和通过阈值在开发阶段确定，并在确认前冻结。

它只是算法内部验证，不是最终实验留出。由标签持有方保管的外部挑战探针必须对选择器、校准器和开发人员保持不可见，直到锁定评估。

### 9.4 预算内最低成本破坏测试

系统在预定义算子集合、扰动成本函数和搜索预算内，寻找已发现的最低成本机制破坏，例如反转一条关键关系、交换功能角色、删除有外部依据的必要约束或改变反馈符号。审计记录必须包含：

- 搜索过的算子与前置条件；
- 扰动成本和自然度审计；
- 调用、步数与 token 预算；
- 找到的最低成本项；
- 未搜索或无法判断的空间。

验证器应对经独立确认的破坏版降低支持。但真实系统可能有冗余或补偿路径，因此未下降不能自动等同于假类比。只有证据充分的必要机制冲突才能硬否决；诊断性变化与信息不足必须单独标记。

### 9.5 预算内最高损失残余反例

残余反例与合成破坏测试不是同一个对象。系统在另一套预定义搜索空间和预算内，寻找已发现的最高损失未决反例：

- 环境差异可能使迁移失败；
- 因果关系只有弱方向证据；
- 目标调节变量未知；
- 存在同样解释材料的替代机制；
- 映射只在过窄或空泛层上成立。

输出必须记录损失函数、搜索预算、覆盖算子和未搜索空间。“没有发现”只表示当前模型与预算内未发现，不能写成反例不存在；因此本文使用 `counterexample-audited`，不声称完备证伪界。

## 10. 证据约束的局部迁移假设

### 10.1 输出目标

输出不是“这段记忆就是远亲”，也不是因果证书，而是一份可失效、非权威、待当前模型核查的局部对应假设：

~~~json
{
  "status": "shadow_hypothesis | shadow_unknown | shadow_none",
  "query_scope": {
    "request_id": "",
    "query_commitment_hash": "",
    "need_frame_version": "",
    "valid_for_current_request_only": true,
    "inspiration_explicitly_requested": true
  },
  "derived_non_authoritative": true,
  "query_tension": {
    "goal": "",
    "constraint": "",
    "unresolved_transition": "",
    "evidence": [],
    "derivation": "explicit | temporary_evidence_bounded"
  },
  "source": {
    "bucket_id": "",
    "entry_id": "",
    "source_content_hash": "",
    "cef_schema_version": "",
    "cef_payload_digest": "",
    "cef_commitment_hash": "",
    "cef_built_without_query_access": true,
    "cef_cross_query_stability_audit": {},
    "evidence_spans": [],
    "extractor_and_prompt_version": ""
  },
  "possible_local_correspondence": "",
  "claim_release": {
    "claim_lattice_hash": "",
    "selected_claim_node": "",
    "fixed_attempt_order": [],
    "fallback_rule": "",
    "minimum_specificity": null,
    "minimum_need_coverage": null,
    "multiplicity_adjusted_results": []
  },
  "transport": {
    "grammar_version": "",
    "codebook_family_hash": "",
    "codebook_members": [],
    "hypothesis_class_scope": {},
    "excluded_operations": [],
    "out_of_language_evidence": [],
    "representative_program": [],
    "cost_components": {},
    "optimality_gap_closed": false,
    "solver_certificate": {}
  },
  "operation_witnesses": [
    {
      "operation_id": "",
      "type": "bind_role | bind_axis | bind_condition | align_phase | coarsen_type | coarsen_phase | drop_optional | open_boundary_port | restrict_scope",
      "positive_spans": {"query": [], "memory": []},
      "scope_certificate": {},
      "optionality_certificate": {},
      "taxonomy_certificate": {},
      "absence_certificate": {},
      "hashes": {},
      "grade": "supported | weak | unknown",
      "assumptions": []
    }
  ],
  "version_space": {
    "epsilon_policy": {
      "min": null,
      "primary": null,
      "max": null,
      "audit_grid": [],
      "policy_hash": "",
      "maximum_envelope_used_for_release": true
    },
    "consensus_vs_epsilon": [],
    "triple_count": null,
    "prediction_equivalence_class_count": null,
    "original_codebook_completion_membership_count": null,
    "represented_codebook_completion_membership_count": null,
    "per_member_surviving_direction_hypothesis_hash": "",
    "symbolic_constraints_hash": "",
    "structural_rivals": [],
    "direction_rivals": [],
    "alternative_class_exists": "yes | no | search_incomplete",
    "alternative_direction_class_exists": "yes | no | not_applicable | search_incomplete",
    "class_set": [],
    "unexplored_mass_or_gap": null,
    "every_completion_same_supported_structural_claim": false,
    "direction_eligible": false,
    "every_member_same_known_direction_hypothesis_key": false,
    "codebook_sensitivity": {}
  },
  "heldout": {
    "split_commitment_hash": "",
    "never_used_for_fit_or_layer_selection": true,
    "structural_per_equivalence_class_predictions": [],
    "structural_consensus_passed": false,
    "direction_eligible": false,
    "direction_per_member_predictions": [],
    "direction_consensus_passed": null,
    "external_challenge": {
      "required_for_locked_signed_product_release": true,
      "split_commitment_hash": "",
      "revealed_once": false,
      "per_direction_or_block_results": [],
      "all_required_results_passed": null
    }
  },
  "full_pipeline_null_gate": {
    "multiplicity_rule": "joint_candidate_claim_maxT | hierarchical_closed_testing",
    "structural": {
      "pool_size": null,
      "matched_search_budget": false,
      "null_max_statistic_reference": "",
      "exchangeability_assumption": "supported | unsupported | unknown",
      "control_mode": "strong_candidate_level | complete_null_pool_only | not_applicable_typed_negative_audit",
      "pool_complete_null_p": null,
      "candidate_level_p": null,
      "strong_control_certificate": null,
      "candidate_localized": false,
      "pool_signal_only": true,
      "passed": false
    },
    "direction": {
      "eligible": false,
      "control_mode": "not_applicable | strong_candidate_level | complete_null_pool_only",
      "pool_complete_null_p": null,
      "candidate_level_p": null,
      "strong_control_certificate": null,
      "candidate_localized": false,
      "pool_signal_only": true,
      "passed": null
    }
  },
  "certificate_checker": {
    "check_type": "supported_release | typed_negative_audit",
    "checker_version": "",
    "checker_hash": "",
    "input_bundle_hash": "",
    "passed": false,
    "invariant_failures": []
  },
  "pre_null_claim_audit": {
    "mapping_status": "supported | no_supported_correspondence | contradicted | unknown",
    "direction_class": "aligned | opposed | mixed | unknown",
    "mixed_partition_id": null,
    "mixed_block_signs": null,
    "direction_hypothesis_key": "aligned | opposed | mixed(P_id,block_signs) | unknown",
    "relation_band": "near_analogy | remote_analogy | surface_false_friend | irrelevant | unknown",
    "release_blocked_by": []
  },
  "mapping_status": "supported | no_supported_correspondence | contradicted | unknown",
  "direction_class": "aligned | opposed | mixed | unknown",
  "mixed_partition_id": null,
  "mixed_block_signs": null,
  "direction_hypothesis_key": "aligned | opposed | mixed(P_id,block_signs) | unknown",
  "transported_candidate_response_hypothesis": {
    "direction_in_query_coordinates": "positive | negative | mixed | unknown",
    "target_response_observed": false,
    "non_authoritative": true
  },
  "orientation_transform": "identity | sign_flip(axis) | role_swap | order_reverse | mixed | unknown",
  "reason_codes": [],
  "logical_relation": "compatible | contradicted | unknown",
  "valence_alignment": "same | different | not_applicable | unknown",
  "surface_similarity": null,
  "surface_distance_interval": [],
  "mechanism_distance_interval": [],
  "relation_band": "near_analogy | remote_analogy | surface_false_friend | irrelevant | unknown",
  "need_value": {
    "new_supported_slots": [],
    "contrast_separation": [],
    "redundant_slots": [],
    "unmatched_required": []
  },
  "partial_forward_mapping": [],
  "partial_reverse_mapping": [],
  "intervention_axis_orientation": [],
  "outcome_axis_orientation": [],
  "signed_response_cells": [],
  "aligned_blocks": [],
  "opposed_blocks": [],
  "query_allowed_layers": ["L2", "L3"],
  "candidate_supported_layers": ["L2"],
  "response_evidence_scope": {
    "known_axes": [],
    "unknown_axes": [],
    "baseline_evidence": [],
    "orientation_evidence": [],
    "response_direction_evidence": []
  },
  "passed_probes": [],
  "failed_probes": [],
  "invalidating_conditions": [],
  "lowest_cost_break_test_found": {
    "operator": "",
    "cost": null,
    "operator_set": [],
    "search_budget": {},
    "unsearched_space": []
  },
  "highest_loss_residual_counterexample_found": {
    "hypothesis": "",
    "loss": null,
    "operator_set": [],
    "search_budget": {},
    "unsearched_space": []
  },
  "unknowns": [],
  "confidence_features": {},
  "risk_band": "uncalibrated | calibrated-low-risk | calibrated-medium-risk | calibrated-high-risk",
  "product_release": {
    "status": "not_evaluated | candidate | none",
    "audit_bundle_hash": "",
    "config_tuple_hash": "",
    "run_manifest_tuple_hash": "",
    "calibrated_risk": null,
    "risk_gate_passed": false,
    "reason_codes": [],
    "current_scope_injection_allowed": false,
    "renderer_policy_hash": ""
  },
  "expires_or_rebuild_rule": "request_end | source_hash_change | extractor_version_change",
  "context_injection_allowed": false
}
~~~

字段组合必须通过 schema 级约束，而不是只靠提示词约定。顶层字段有两条互斥路径：`supported_release` 的 `mapping_status`、`direction_class`、`direction_hypothesis_key` 与 `relation_band` 表示经过 claim-level null 和独立 checker 后的最终可发布状态；`typed_negative_audit` 只允许在穷尽负证书经独立 checker 通过后保留 `no_supported_correspondence/contradicted`，且永远 audit-only。null／checker 之前形成的正结构或方向只能复制到 `pre_null_claim_audit`，该审计对象不可被 renderer 当作候选结论：

- `transport.representative_program` 以及 version space 中每个程序的每个 operation 都必须由相同 `operation_id` 的 witness 覆盖；`bind_*` 要求双侧 positive spans，`coarsen_*` 要求 taxonomy certificate 与来源 span，`drop_optional` 必须有独立 optionality certificate，且包含 `schema_hash, atom_id, prequery_optional_flag=true, commitment_timestamp`，`restrict_scope` 要求冻结 scope certificate，`open_boundary_port` 要求范围受限的 absence／boundary certificate。与 operation 类型无关的空对象不得冒充证书；任一 required witness 缺失时该程序非法；
- `mapping_status != supported` 时，`direction_class` 必须为 `unknown`；
- 对 `supported_release`，`transport.optimality_gap_closed != true`、结构类 `version_space.alternative_class_exists != no`、`version_space.every_completion_same_supported_structural_claim != true`、`heldout.structural_consensus_passed != true`、`full_pipeline_null_gate.structural.passed != true`、`full_pipeline_null_gate.structural.candidate_localized != true` 或 `certificate_checker.passed != true` 时，顶层必须原子性降级为 `mapping_status=unknown, direction_class=unknown, direction_hypothesis_key=unknown, relation_band=unknown`，清空 mixed 产品字段，并把任何门禁前判断移入 `pre_null_claim_audit`；产品状态只能是 `none`，研究审计若仍有可复核假设则为 `shadow_unknown`，没有假设则为 `shadow_none`，具体原因写入 `reason_codes`。若结构层与 checker 均通过而只有方向 heldout／`full_pipeline_null_gate.direction` 不适用或失败，只令方向及 mixed 字段退为 unknown，不否决 unsigned 结构候选；
- 对 `typed_negative_audit`，只有 solver 针对每个原始 completion／codebook／允许 transport 成员给出第 5.2.5 节定义的穷尽 `logical_same_scope_contradiction` 或普遍必要 `structural_impossibility`，且独立 checker 以 `check_type=typed_negative_audit` 重放范围、证据、穷尽性和哈希后，才可在研究审计保留 `mapping_status=contradicted` 或 `no_supported_correspondence`；此时 `direction_class/direction_hypothesis_key=unknown`、mixed 字段为 null、`full_pipeline_null_gate.structural.control_mode=not_applicable_typed_negative_audit`、`context_injection_allowed=false`，不得进入正候选排序。checker 失败或证书不穷尽立即降为 unknown；若未来要向产品或用户定位／展示某个负候选，必须另建预注册的 negative-claim family、candidate-level multiplicity／null 与再次批准，不能复用正结构 p 值；
- `direction_class != unknown` 时，另外要求 `version_space.every_member_same_known_direction_hypothesis_key=true`、`version_space.alternative_direction_class_exists=no`、`heldout.direction_consensus_passed=true`、`full_pipeline_null_gate.direction.passed=true`、`full_pipeline_null_gate.direction.candidate_localized=true` 且查询侧 response 有独立证据；这里的 same／alternative 比较完整 canonical key `(direction_class, mixed_partition_id, mixed_block_signs)`，不是只比较泛化 `direction_class`；方向门失败不自动抹掉已经通过的无方向结构候选。shadow 研究阶段允许 `heldout.external_challenge.all_required_results_passed=null`，但任何锁定 signed 结论或产品可见方向还必须满足 `required_for_locked_signed_product_release=true`、独立 split hash 非空、只揭封一次且 `all_required_results_passed=true`；否则 checker／产品策略必须拒绝 signed release，强制方向退为 unknown，而不能拿内部 heldout 代替外部挑战；
- `direction_class=mixed` 时，`mixed_partition_id` 与 `mixed_block_signs` 必填，partition ID 必须属于响应揭封前冻结的库，block signs 必须同时含 aligned 与 opposed 且每块分别通过证据／coverage／`internal_audit_heldout` 门；锁定结论还要求 `heldout.external_challenge.per_direction_or_block_results` 为每个冻结 block 提供独立结果且全部通过。`direction_class` 为 `aligned/opposed/unknown` 时两个 mixed 产品字段必须为 null，诊断候选只能另存审计字段，禁止裸 `mixed`；
- `source.cef_built_without_query_access` 或 `heldout.never_used_for_fit_or_layer_selection` 不是 true 时，候选属于协议无效，不能用较低 risk band 补救；
- 任一事前合理代码本引入另一类别、或 `out_of_language_evidence` 命中 required 项时，必须令相应 `mapping_status` 或 `direction_class` 为 `unknown`，并写入 `reason_codes=["model_class_sensitivity"]`；
- 查询侧没有观察或独立标签支持 response 时，`transported_candidate_response_hypothesis.target_response_observed=false` 且 `direction_class` 必须为 `unknown`；候选侧搬运后的正负方向只能作为单独的非权威假设字段；
- `representative_program` 只用于展示，任何发布判断必须由 `version_space.class_set` 和 heldout 结果派生；
- `relation_band=remote_analogy` 只有在机制传输门、版本空间门、heldout 门和 Need Frame 增量门全部通过后才可出现；表层距离不能单独触发；
- `mapping_status = contradicted` 当且仅当 `logical_relation = contradicted`；
- `mapping_status = supported` 时，当前局部范围内的 `logical_relation` 必须为 `compatible`；
- `mapping_status` 为 `no_supported_correspondence` 或 `unknown` 时，`logical_relation` 为 `unknown`；
- `orientation_transform=sign_flip(axis)` 只记录把候选轴规范到查询轴时采用的坐标变换，本身不表示 `opposed`；只有坐标规范化完成后响应仍稳定相反，才允许 `direction_class=opposed`；
- `no_stable_direction` 只能作为 `mapping_status=supported, direction_class=unknown` 的 `reason_codes`，不能升级为“无支持对应”。

### 10.2 研究与产品状态机必须分开

在没有冻结校准器和确认结果的研究阶段，只允许：

1. `shadow_hypothesis`：形成可审计局部假设，但不注入模型上下文；
2. `shadow_unknown`：存在可审计映射或候选材料，但关键结构、方向、类外证据、solver 或 null 门尚不可识别；具体原因写入 `reason_codes`；
3. `shadow_none`：没有形成满足研究门槛的假设。

只有校准器、风险门槛和产品策略均冻结，阶段 3D 锁定确认、阶段 5D 可见效用与阶段 6D 产品决策全部通过，并取得项目所有者针对当前协议 hash 的明确批准后，才可启用：

1. `candidate`：证据门、内部审计门和预注册低风险门均通过；
2. `speculative`：仅作为旧 WIT／SOS-PAR 的不可见审计标签；单一来源或缺少独立 validation 的 DSR 候选必须保持 `seed_only_unverified`，不得借此升级；
3. `none`：证据不足、存在硬冲突、校准风险过高或预算不足。

`candidate/speculative` 状态本身也不等于允许注入。对 DSR-CT，`speculative` **永久不得**作为灵感诊断进入上下文；项目所有者批准也不能把同一条未验证 seed 重新命名为 DSR 产品候选。若未来确有展示单来源材料的需求，必须另立非 DSR 功能、独立风险协议和名称。本文当前范围内所有 `speculative` 状态一律 `context_injection_allowed: false`；只有另经完整产品门批准的 `candidate` 才可能在当前 scope 取得 true。

`direction_class` 在研究阶段只允许写入 shadow 审计结果。特别是 `opposed`：只有独立的有符号方向机制测试达到冻结门槛、锁定确认复现该结果，并由项目所有者再次批准后，才可讨论是否对产品可见；在此之前不得作为 `candidate` 或 `speculative` 注入上下文。即使未来可见，它也只能说明“在当前查询、当前角色映射和当前证据范围内呈逆向响应”，不能改写为“应采取相反行动”。

### 10.3 不允许的输出

输出不得包含：

- “模型应当拒绝”；
- “必须执行某个行动”；
- “该记忆为真或为假”的未经验证判决；
- 对当前模型态度、情绪或意志的强制要求；
- 将候选提升为长期规则或新的记忆真源；
- 自动修改 plan、pinned、anchor、I 或其他结构。

### 10.4 自动派生张力的边界

若调用方没有显式提供 tension，系统最多从当前请求的可见文字中派生一个临时检索变换。它必须：

- 只在当前请求中有效且不持久化；
- 只引用显式证据，不推断隐藏情绪、意志、人格或长期目标；
- 不等同于 `plan`，也不能创建、修改或终止 plan；
- 不把记忆内容变成当前模型的态度或决定；
- 无法形成具体、可证伪张力时返回空。

## 11. 置信模型

### 11.1 置信度的含义

这里的置信度只表示：

> 当前候选作为本轮局部类比材料的匹配可信程度。

它不表示：

- 原记忆整体真实性；
- 用户陈述可信度；
- 未来事件发生概率；
- 模型是否应该拒绝；
- 行为许可；
- 规范性权重。

### 11.2 可用特征

置信模型可以使用：

- 张力具体程度；
- 查询前盲封的一致性；
- CEF 是否在查询不可见时生成、source hash 是否仍匹配；
- 求解器是否关闭逐 codebook／completion 最优性 gap，以及 \(\mathcal V_{release}\) 的成员数、预测等价类数、原始证据相容 completion／codebook 覆盖和未枚举质量上界；
- 近最优程序的 heldout 类别是否一致、最低成本竞争程序与最小区分探针；
- 查询允许层与候选支持层宽度、内部审计稳定性；
- 结构保持扰动稳定性；
- 结构破坏扰动敏感度；
- 反事实响应一致度；
- 有证据探针覆盖率；
- 原文证据覆盖率；
- 角色映射一致性；
- 因果、时序和极性冲突数；
- 多模型、多 seed 的稳定性；
- 通过全部硬门后的候选间 margin；不得用“最佳单一映射与第二映射”的 margin 取代版本空间可识别性硬门；
- 相对 shuffled、surface-only、mechanism-matched random 与 renderer-only null 的留出优势；
- Need Frame 的新支持槽位、冗余槽位和未匹配必要义务；
- 多召回通道支持数；
- 是否有独立来源交叉确认；
- 未知项数量和位置。

### 11.3 不使用模型自报概率

“我有 90% 把握”不能被当成校准置信度。

在确认评估前，系统只能输出原始特征和 `uncalibrated` shadow 记录。校准流程必须是：

~~~text
现有 60 例开发集
→ 独立校准集，或开发集内严格 cross-fitting
→ 冻结校准映射、风险门槛与覆盖策略
→ 锁定确认集只做一次评估
→ 独立复制集
~~~

确认集不能用于拟合 isotonic、阈值或风险等级。通过上述流程后输出的是“校准概率或风险带”，不是“校准置信区间”；参数和指标的统计置信区间应另行估计。

建议使用：

- risk–coverage 曲线；
- 选择性分类；
- 保序或等距校准；
- Brier score；
- ECE；
- 必要时研究 conformal 或风险控制预测。

任何理论保证都必须说明交换性和分布稳定假设；个人 vault 分布漂移时不能直接宣称覆盖保证。

### 11.4 允许空结果

必须允许大量空结果。

同时不能只看 Precision，因为系统可以通过永不输出作弊。所有精度必须绑定：

- 固定覆盖率；
- 完整 risk–coverage 曲线；
- 正确拒答率；
- 有有效候选时的误拒绝率。

## 12. 算法伪代码

### 12.0 DSR-CT 规范性总流程

以下伪代码定义新版研究主臂。它明确拆成“方法提交”与“跨方法 orchestrator 统一揭封／评分”：DSR、全部预注册基线和消融都只能提交冻结包；orchestrator 在 exact method set、各方法候选数、排序、概率、gold-cell review request 和 outcome 并集全部进入 run manifest 后，才拥有唯一一次 `reveal_union_once` 权限。方法不得自报 scoring callback 或选择 evaluator；由预注册 registry 以 `evaluator_contract_hash` 绑定独立 evaluator。raw outcome、全局 gold 及可自行扩权的 store handle 永不返回方法，只返回按 submission／package scope 封存的只读评估回执。确认研究回执与产品运行回执严格分型：人工 gold 只能评价方法并更新未来 calibration，不能决定同一次 shadow／产品候选；产品运行只能读取在本次 run 开始前已冻结的自动 adjudicator 与交叉拟合风险模型。任何方法内部揭封、某方法先看结果再让另一方法提交、当前 gold 回流产品选择，或临时增删基线都会使整轮无效。实现名、文件职责与现有入口必须等实际代码审计后再决定；本文不授权创建模块或修改生产路径。

~~~python
def prepare_dsr_ct_submission(
    query,
    *,
    inspiration=False,
    run_mode="source_confirmation",
    # source_confirmation | offline_rolling_origin | shadow | approved_visible_product
    blind_discovery_index,
    blind_validation_index,
    frozen_protocol,
    approval_receipt,
):
    # 0. 显式触发与完全旁路
    if inspiration is not True:
        return unchanged_existing_path()

    run_origin_time = freeze_run_origin_time(query.evaluation_cutoff)
    authorization = validate_stage_bound_approval_receipt(
        approval_receipt,
        run_mode=run_mode,
        protocol_hash=frozen_protocol.hash,
        model_hash=frozen_protocol.model_hash,
        renderer_hash=frozen_protocol.renderer_hash,
        vault_scope=frozen_protocol.vault_scope,
    )
    if not authorization.valid:
        return abstain("stage_or_scope_not_approved")
    if run_mode == "offline_rolling_origin":
        assert authorization.stage_1d_source_confirmation_passed
    if run_mode == "shadow":
        assert authorization.future_shadow_opt_in
        assert authorization.historical_rolling_origin_passed
    if run_mode == "approved_visible_product":
        assert authorization.approved_product
        assert authorization.stage_3d_confirmation_passed
        assert authorization.stage_5d_visible_utility_passed
        assert authorization.stage_6d_external_replication_and_product_decision_passed

    # 所有 blind builder 从入口就绑定同一个伪时点；末端检查不能补救未来材料泄漏
    discovery_snapshot = blind_discovery_index.snapshot_strictly_before(
        run_origin_time
    )
    validation_snapshot = blind_validation_index.snapshot_strictly_before(
        run_origin_time
    )
    assert_snapshot_has_no_post_cutoff_or_outcome_proxy(discovery_snapshot)
    assert_snapshot_has_no_post_cutoff_or_outcome_proxy(validation_snapshot)

    # query-free predictor 必须在本轮前训练并冻结，且永远看不到 query／Need／机制／panel role
    query_free_registry = load_frozen_crossfit_predictor_registry(
        registry_ref=frozen_protocol.query_free_predictor_registry_ref,
        registry_manifest_hash=(
            frozen_protocol.query_free_registry_manifest_hash
        ),
        deterministic_fold_rule_hash=frozen_protocol.control_fold_rule_hash,
        allowed_training_splits={"discovery", "calibration"},
        built_strictly_before=run_origin_time,
        forbid_online_retraining=True,
    )
    assert query_free_registry.forbidden_inputs == {
        "query", "need", "discovery_seed", "mechanism_card",
        "rivals", "proposed_cell", "gold_cell", "validation_outcome",
    }

    # 1. Need Frame 在候选可见前冻结；它不包含行动计划或拒绝理由
    need = freeze_need_frame(query, frozen_protocol.need_schema)
    if not need.is_specific:
        return abstain("need_unspecified")

    # 第二个低成本对照可见 query／Need，但看不到 discovery memory 或其机制
    query_aware_no_discovery_registry = (
        load_frozen_crossfit_predictor_registry(
            registry_ref=(
                frozen_protocol.query_aware_no_discovery_registry_ref
            ),
            registry_manifest_hash=(
                frozen_protocol.query_aware_no_discovery_registry_manifest_hash
            ),
            deterministic_fold_rule_hash=frozen_protocol.control_fold_rule_hash,
            allowed_training_splits={"discovery", "calibration"},
            built_strictly_before=run_origin_time,
            forbid_online_retraining=True,
        )
    )
    assert query_aware_no_discovery_registry.forbidden_inputs == {
        "discovery_seed", "blind_source_ledger", "witnessed_tca",
        "mechanism_card", "rivals", "proposed_cell", "gold_cell",
        "validation_outcome",
    }

    direct_queries = [need.result_blind_query]
    need_path_queries = generate_informational_need_probes(
        need,
        allowed_types={
            "constraint", "boundary", "temporal",
            "feedback", "rival_discriminator",
        },
        forbidden_types={
            "next_action", "user_intent", "plan",
            "refusal_reason", "permit_condition",
        },
        max_probes=frozen_protocol.max_need_probes,
    )

    assert_ephemeral(need_path_queries)

    # 2. 发现召回：只读 cutoff 前 BlindSourceLedger；向量只负责 recall
    seed_pool = discovery_snapshot.recall_union(
        direct_queries + need_path_queries,
        channel_budgets=frozen_protocol.discovery_channel_budgets,
        fusion=frozen_protocol.discovery_fusion,
    )
    assert_no_outcome_or_proxy_access(seed_pool)
    assert_outcome_permutation_invariant(seed_pool.commitment_hash)

    seed_pool = event_cluster_deduplicate(seed_pool)
    seed_pool = apply_acl_and_provenance_gate(seed_pool)
    if not seed_pool:
        return abstain("seed_not_found")

    frozen_packages = []

    for discovery_seed in seed_pool:
        # 3. seed 可以提出一个真正的新机制，但只使用结果盲见证透镜
        witnessed_tca = build_witnessed_tca(
            query=query,
            blind_source_ledger=discovery_seed.blind_ledger,
        )
        if not deterministic_span_checker(witnessed_tca):
            continue
        if not passes_target_swap_and_span_shuffle(witnessed_tca):
            continue

        mechanism_card = propose_mechanism_card(
            need=need,
            witnessed_tca=witnessed_tca,
            require_local_falsifiable_claim=True,
            forbid_cognitive_profile=True,
        )
        rival_set = construct_material_rivals(
            mechanism_card,
            required={
                "surface_association",
                "temporal_trend_or_base_rate",
                "nearest_alternative_mechanism",
                "no_stable_relation",
            },
        )
        if not predictions_are_separated(mechanism_card, rival_set):
            continue

        mechanism_commit = append_only_freeze(
            mechanism_card=mechanism_card,
            rivals=rival_set,
            axes=frozen_axes(mechanism_card),
            boundaries=frozen_boundaries(mechanism_card),
            discovery_seed_clusters=all_related_clusters(discovery_seed),
            model_prompt_seed_impl_hashes=current_reproducibility_hashes(),
        )

        # 4. 验证检索：目标是找自然反证／复制，不是找更多相似故事
        panel_blind = retrieve_natural_contrast_panel(
            mechanism_commit,
            index=validation_snapshot,
            exclude_event_clusters=mechanism_commit.discovery_seed_clusters,
            roles={"analogue", "bridge", "foil", "null"},
            policy=frozen_protocol.validation_retrieval_policy,
        )
        assert_no_outcome_or_proxy_access(panel_blind)
        assert_event_cluster_disjoint(
            mechanism_commit.discovery_seed_clusters,
            panel_blind.event_clusters,
        )

        # provenance 过滤／规范排序必须先完成；之后的 binding、gold request 与 panel hash 不得漂移
        panel_blind = deterministic_provenance_check(panel_blind)
        if not panel_blind:
            continue
        canonical_panel_hash = hash_canonical(panel_blind)

        binding_cards = freeze_validation_binding_cards(
            mechanism_commit,
            panel_blind,
            require_event_specific_role_and_change_mapping=True,
            require_outcome_blind_applicability_probability=True,
        )
        gold_cell_request = freeze_hidden_gold_cell_review_request(
            review_view=method_blind_gold_review_view(
                query_or_need=need,
                canonical_mechanism_and_boundaries=mechanism_commit,
                validation_pre_outcome_spans=panel_blind,
            ),
            mutually_exclusive={"analogue", "bridge", "foil", "null"},
            hide_method_name_proposed_cell_and_scores=True,
            review_codebook_hash=frozen_protocol.gold_cell_codebook_hash,
            canonical_panel_hash=canonical_panel_hash,
        )
        assert binding_cards.panel_hash == canonical_panel_hash
        assert gold_cell_request.panel_hash == canonical_panel_hash
        if not meets_proposed_minimal_panel_support(binding_cards):
            record_research_state(
                mechanism_commit,
                "validation_panel_insufficient",
            )
            continue

        # 合成扰动只作 metamorphic test，不进入真实证据 panel
        metamorphic_suite = build_diagnostic_only_perturbations(
            panel_blind,
            types={
                "surface_preserving",
                "mechanism_breaking",
                "role_swap",
                "outcome_swap_canary",
            },
        )
        assert all(x.evidence_role == "metamorphic_only"
                   for x in metamorphic_suite)

        # 只按冻结 ID fold rule 选预构建 cross-fit bundle；每个事件用排除其外层 fold 的模型
        control_fold_key = deterministic_control_fold_key(
            owner_id=query.owner_id,
            discovery_event_clusters=mechanism_commit.discovery_seed_clusters,
            validation_event_clusters=panel_blind.event_clusters,
            time_block=run_origin_time,
            rule_hash=frozen_protocol.control_fold_rule_hash,
        )
        excluded_blocks = canonical_owner_event_time_blocks(
            query.owner_id,
            mechanism_commit.discovery_seed_clusters
            | panel_blind.event_clusters,
            frozen_protocol.confirmation_exclusion_manifest,
        )
        query_free_predictor = query_free_registry.for_prebuilt_fold_bundle(
            control_fold_key,
            excluded_owner_event_time_blocks=excluded_blocks,
            forbid_training=True,
        )
        query_aware_no_discovery_predictor = (
            query_aware_no_discovery_registry.for_prebuilt_fold_bundle(
                control_fold_key,
                excluded_owner_event_time_blocks=excluded_blocks,
                forbid_training=True,
            )
        )
        if (query_free_predictor is None
                or query_aware_no_discovery_predictor is None):
            record_research_state(
                mechanism_commit, "required_prebuilt_control_fold_missing"
            )
            continue
        assert fold_selection_is_deterministic_and_outcome_invariant(
            control_fold_key,
            query_free_predictor,
            query_aware_no_discovery_predictor,
        )
        assert each_event_prediction_uses_model_excluding_its_outer_fold(
            panel_blind,
            query_free_predictor,
            query_aware_no_discovery_predictor,
        )

        # 5. 全部机制对同一 panel 揭封前提交联合概率和响应签名
        commits = []
        for hypothesis in [mechanism_card] + list(rival_set):
            commits.append(
                prequential_commit(
                    hypothesis=hypothesis,
                    panel=panel_blind,
                    probabilities=predict_joint_real_outcomes(
                        hypothesis,
                        panel_blind,
                    ),
                    signature=predict_difference_invariance_reversal(
                        hypothesis,
                        panel_blind,
                    ),
                    applicability=predict_applicability_outcome_blind(
                        hypothesis,
                        binding_cards,
                    ),
                    common_scoreability_rule=(
                        frozen_protocol.outcome_holder_scoreability_rule
                    ),
                    hashes=current_reproducibility_hashes(),
                )
            )

        # 强 query-free outcome baseline 是独立概率提交，不得拿 trend rival 冒充
        query_free_commit = prequential_query_free_commit(
            predictor=query_free_predictor,
            predictor_hash=query_free_predictor.bundle_hash,
            fold_key=control_fold_key,
            panel=panel_blind,
            probabilities=predict_query_free_real_outcomes(
                query_free_predictor,
                panel_blind,
                visible_fields=frozen_protocol.query_free_field_allowlist,
            ),
            training_manifest_hash=query_free_predictor.training_manifest_hash,
            excluded_event_clusters=(
                mechanism_commit.discovery_seed_clusters
                | panel_blind.event_clusters
            ),
            forbidden_inputs={
                "query", "need", "discovery_seed", "mechanism_card",
                "rivals", "proposed_cell", "gold_cell", "outcome",
            },
            common_scoreability_rule=(
                frozen_protocol.outcome_holder_scoreability_rule
            ),
            hashes=current_reproducibility_hashes(),
        )
        assert query_free_commit.sealed_before_any_panel_outcome_reveal
        assert_training_manifest_disjoint_from_owner_event_time_blocks(
            query_free_predictor.training_manifest,
            mechanism_commit.discovery_seed_clusters
            | panel_blind.event_clusters
            | frozen_protocol.confirmation_exclusion_manifest,
        )
        commits.append(query_free_commit)

        # query-aware/no-discovery control 隔离“query 本身的信息”与“发现记忆提出机制”的增量
        query_aware_control_commit = prequential_control_commit(
            role="query_aware_no_discovery",
            predictor=query_aware_no_discovery_predictor,
            predictor_hash=query_aware_no_discovery_predictor.bundle_hash,
            fold_key=control_fold_key,
            query_and_need=(query, need),
            panel=panel_blind,
            probabilities=predict_without_discovery_memory_or_mechanism(
                query_aware_no_discovery_predictor,
                query=query,
                need=need,
                panel_pre_outcome_fields=panel_blind.allowed_pre_outcome_view,
            ),
            training_manifest_hash=(
                query_aware_no_discovery_predictor.training_manifest_hash
            ),
            forbidden_inputs={
                "discovery_seed", "blind_source_ledger", "witnessed_tca",
                "mechanism_card", "rivals", "proposed_cell", "gold_cell",
                "outcome",
            },
            common_scoreability_rule=(
                frozen_protocol.outcome_holder_scoreability_rule
            ),
            hashes=current_reproducibility_hashes(),
        )
        assert query_aware_control_commit.sealed_before_any_panel_outcome_reveal
        assert_training_manifest_disjoint_from_owner_event_time_blocks(
            query_aware_no_discovery_predictor.training_manifest,
            mechanism_commit.discovery_seed_clusters
            | panel_blind.event_clusters
            | frozen_protocol.confirmation_exclusion_manifest,
        )
        commits.append(query_aware_control_commit)

        independent_impl_receipts = cross_family_prequential_commit(
            mechanism_commit,
            panel_blind,
            frozen_protocol.independent_implementations,
            require_full_probabilities_and_signature=True,
            forbid_shared_model_session=True,
        )
        candidate_package = freeze_before_reveal(
            mechanism_commit,
            panel_blind,
            binding_cards,
            gold_cell_request,
            commits,
            metamorphic_suite,
            independent_impl_receipts,
            canonical_panel_hash=canonical_panel_hash,
            required_commit_roles={
                "focus", "all_material_rivals", "query_free_baseline",
                "query_aware_no_discovery",
            },
        )

        # 6. seed 循环内绝不揭封；只追加已冻结候选包
        assert discovery_seed.outcome_ref not in panel_blind.outcome_refs
        frozen_packages.append(candidate_package)

    # 7. DSR 方法只能提交；不得在方法内部揭封
    if not frozen_packages:
        return abstain("no_frozen_candidate_package")

    # raw query、source spans、完整 package 与授权材料留在 method-local sealed store
    local_submission = seal_method_local_state(
        method_id="dsr_ct",
        run_origin_time=run_origin_time,
        run_mode=run_mode,
        query_context=query,
        frozen_need=need,
        authorization_receipt=authorization,
        frozen_packages=frozen_packages,
        exact_method_set=frozen_protocol.exact_method_set,
        forbid_method_local_reveal=True,
        evaluator_contract_hash=(
            frozen_protocol.registry_evaluator_contract_hash(run_mode)
        ),
        forbid_method_selected_evaluator=True,
    )

    # capability 本身不进入 public manifest；它不可转授权、按接收方分域并以内容 hash 回绑
    capability_bundle = seal_nontransferable_capability_bundle(
        evaluator_package_handles=[registry_only_handle(p)
                                   for p in frozen_packages],
        evaluator_query_need_handle=registry_only_handle(
            (query, need)
        ),
        outcome_ref_handles=registry_only_outcome_handles(frozen_packages),
        cell_gold_review_handles=[gold_holder_only_handle(
            p.gold_cell_request.review_view
        ) for p in frozen_packages],
        common_gold_evidence_handles=[common_gold_holder_only_handle(
            query_unit_id=opaque_query_unit_id(query),
            canonical_candidate_id=p.canonical_candidate_id,
            shared_query_need_view=shared_benchmark_query_need_view(query, need),
            fixed_budget_pre_outcome_spans=p.common_gold_source_view,
        ) for p in frozen_packages],
        recipient_allowlist=frozen_protocol.capability_recipient_hashes,
        non_transferable=True,
    )

    public_manifest = freeze_opaque_public_manifest(
        method_id="dsr_ct",
        run_id=frozen_protocol.run_id,
        query_unit_id=opaque_query_unit_id(query),
        run_origin_time=run_origin_time,
        run_mode=run_mode,
        exact_method_set=frozen_protocol.exact_method_set,
        local_state_commitment_hash=local_submission.hash,
        package_commitment_headers=safe_package_commitment_headers(
            frozen_packages
        ),
        canonical_candidate_commitment_headers=(
            safe_canonical_candidate_commitment_headers(frozen_packages)
        ),
        outcome_ref_union_commitment_hash=hash_opaque_outcome_ref_union(
            frozen_packages
        ),
        gold_request_commitment_headers=(
            safe_gold_request_commitment_headers(frozen_packages)
        ),
        probability_commit_headers=safe_probability_commit_headers(
            frozen_packages,
            include={
                "role", "predictor_hash", "training_manifest_hash",
                "fold_key", "probability_hash", "panel_hash", "sealed_at",
            },
        ),
        capability_bundle_commitment_hash=capability_bundle.content_hash,
        automatic_selection_policy_hash=(
            frozen_protocol.automatic_selection_policy_hash
        ),
        evaluator_contract_hash=local_submission.evaluator_contract_hash,
        signed_authorization_claims=sign_non_secret_authorization_claims(
            authorization,
            include={
                "issuer", "subject", "run_id", "run_mode", "scope",
                "protocol_hash", "model_hash", "renderer_hash",
                "vault_scope", "issued_at", "expires_at", "revocation_epoch",
            },
        ),
        config_tuple_hash=frozen_protocol.confirmed_config_tuple_hash,
    )
    assert public_manifest.contains_no_raw_query_or_need
    assert public_manifest.contains_no_source_span_or_outcome
    assert public_manifest.contains_no_authorization_secret_or_bearer_material
    bind_local_state_to_public_manifest(local_submission, public_manifest.hash)
    bind_capability_bundle_to_public_manifest(
        capability_bundle, public_manifest.hash
    )
    register_capability_bundle_with_broker(capability_bundle)
    register_method_local_state(local_submission)
    return public_manifest


def orchestrate_dsr_ct_research_benchmark_run(
    public_manifests,
    *,
    append_only_manifest,
    capability_broker,
    automatic_evaluator,
    gold_evaluator,
    receipt_emitter,
    hidden_cell_gold_holder,
    useful_far_gold_holder,
    outcome_holder,
    frozen_protocol,
):
    # 研究 orchestrator 才要求完整方法集与 human gold；runtime 使用下方独立入口
    exact_methods = tuple(frozen_protocol.exact_method_set)
    assert len(public_manifests) == len(exact_methods)
    assert len({m.method_id for m in public_manifests}) == len(public_manifests)
    assert {m.method_id for m in public_manifests} == set(exact_methods)
    assert all(m.run_mode in {
        "source_confirmation", "offline_rolling_origin",
    } for m in public_manifests)
    assert len({(
        tuple(m.exact_method_set), m.config_tuple_hash,
        m.evaluator_contract_hash, m.run_mode,
    ) for m in public_manifests}) == 1
    assert all(m.run_id == frozen_protocol.run_id for m in public_manifests)
    assert len({(m.query_unit_id, m.run_origin_time)
                for m in public_manifests}) == 1
    assert all(m.contains_no_raw_private_payload for m in public_manifests)

    for manifest in public_manifests:
        validate_signed_authorization_claims(
            manifest.signed_authorization_claims,
            trusted_issuer=frozen_protocol.authorization_issuer,
            expected_run_id=manifest.run_id,
            expected_run_mode=manifest.run_mode,
            expected_scope=frozen_protocol.authorization_scope_for(
                manifest.run_mode
            ),
            expected_vault_scope=frozen_protocol.vault_scope,
            reject_expired=True,
            check_revocation_epoch=True,
        )
        automatic_evaluator.validate_public_manifest_schema(manifest)
        append_only_manifest.append(manifest)
        assert manifest.has_all_native_candidate_and_selection_commits
        if manifest.method_id == "dsr_ct":
            assert manifest.has_commit_roles({
                "focus", "all_material_rivals", "query_free_baseline",
                "query_aware_no_discovery",
            })
            assert all_probability_commit_headers_include(
                manifest,
                fields={
                    "role", "predictor_hash", "training_manifest_hash",
                    "fold_key", "probability_hash", "panel_hash", "sealed_at",
                },
            )

    # 双信封：public 只有 commitments；broker 只给指定进程不可转授权的最小 capability
    automatic_caps = capability_broker.open_scoped_for_recipient(
        public_manifests,
        recipient_hash=automatic_evaluator.identity_hash,
        allowed={"query_need_for_auto_score", "package_for_auto_score",
                 "outcome_ref"},
    )
    cell_gold_caps = capability_broker.open_scoped_for_recipient(
        public_manifests,
        recipient_hash=hidden_cell_gold_holder.identity_hash,
        allowed={"method_blind_cell_review_view"},
    )
    useful_gold_caps = capability_broker.open_scoped_for_recipient(
        public_manifests,
        recipient_hash=useful_far_gold_holder.identity_hash,
        allowed={"shared_query_need_and_fixed_budget_source_view"},
    )
    gold_score_caps = capability_broker.open_scoped_for_recipient(
        public_manifests,
        recipient_hash=gold_evaluator.identity_hash,
        allowed={"package_for_gold_score", "outcome_ref"},
    )
    assert_capability_content_hashes_match_public_commitments(
        public_manifests,
        automatic_caps, cell_gold_caps, useful_gold_caps, gold_score_caps,
    )

    method_barrier = append_only_manifest.close_method_barrier_once()
    assert method_barrier.exact_method_set_frozen
    assert all(
        c.sealed_at <= method_barrier.closed_at
        for m in public_manifests for c in m.probability_commit_headers
    )

    # gold 在盲态封存；automatic process 没有它们的 capability
    hidden_cell_commits = hidden_cell_gold_holder.freeze_method_blind_cells(
        scoped_review_capabilities=cell_gold_caps,
        before_any_outcome_reveal=True,
    )
    useful_far_commit = useful_far_gold_holder.freeze_common_gold_units(
        scoped_evidence_capabilities=useful_gold_caps,
        canonical_candidate_union=union_committed_canonical_candidate_ids(
            public_manifests
        ),
        hide_method_rank_score_certificate=True,
    )
    append_only_manifest.append(hidden_cell_commits)
    append_only_manifest.append(useful_far_commit)
    gold_barrier = append_only_manifest.close_gold_barrier_once()

    # 一份内部 union snapshot，只向两个隔离进程签发不同只读 capability
    assert_all_authorizations_still_valid(
        [m.signed_authorization_claims for m in public_manifests],
        at=current_time(),
        check_expiry=True,
        check_revocation=True,
        check_vault_acl=True,
    )
    reveal_receipt, automatic_outcome_cap, gold_outcome_cap = (
        outcome_holder.create_internal_union_snapshot_once(
            outcome_ref_capabilities=(
                automatic_caps.outcome_refs | gold_score_caps.outcome_refs
            ),
            require_barriers={method_barrier.hash, gold_barrier.hash},
            recipient_hashes={
                automatic_evaluator.identity_hash,
                gold_evaluator.identity_hash,
            },
        )
    )
    assert all(
        c.sealed_at < reveal_receipt.first_reveal_at
        for m in public_manifests for c in m.probability_commit_headers
    )
    assert hidden_cell_commits.sealed_at < reveal_receipt.first_reveal_at
    assert useful_far_commit.sealed_at < reveal_receipt.first_reveal_at

    # automatic envelope 先签出并终止；它从未获得 human gold ACL
    automatic_envelope = automatic_evaluator.score_and_sign_immutable_envelope(
        public_manifests,
        query_need_capabilities=automatic_caps.query_need,
        package_capabilities=automatic_caps.packages,
        outcome_capability=automatic_outcome_cap,
        frozen_automatic_adjudicators=(
            frozen_protocol.automatic_adjudicator_registry
        ),
        forbid_human_gold_capability=True,
    )
    automatic_selection_commit = append_only_manifest.append_and_freeze(
        automatic_envelope.primary_selection_ids_and_release_flags
    )
    automatic_evaluator.terminate_and_zeroize_capabilities()

    # 独立 gold process 只产生 Y／诊断；不能改写已签 automatic bytes
    gold_envelope = gold_evaluator.score_and_sign_gold_envelope(
        public_manifests,
        package_capabilities=gold_score_caps.packages,
        outcome_capability=gold_outcome_cap,
        hidden_cell_commits=hidden_cell_commits,
        useful_far_commit=useful_far_commit,
        bound_automatic_selection_hash=automatic_selection_commit.hash,
    )
    return receipt_emitter.join_signed_envelopes_without_recomputation(
        public_manifests,
        automatic_envelope=automatic_envelope,
        automatic_selection_commit=automatic_selection_commit,
        gold_envelope=gold_envelope,
        assert_automatic_bytes_hash_unchanged=automatic_envelope.hash,
        expose_raw_outcomes=False,
        expose_raw_gold=False,
        receipt_kind="research_benchmark",
    )


def orchestrate_dsr_ct_runtime_run(
    public_manifest,
    *,
    append_only_manifest,
    capability_broker,
    runtime_evaluator,
    outcome_holder,
    frozen_protocol,
):
    # runtime 无完整 benchmark 方法集、无人类 gold holder，也不接受 gold envelope
    assert public_manifest.run_mode in {"shadow", "approved_visible_product"}
    assert public_manifest.config_tuple_hash == (
        frozen_protocol.prior_locked_confirmation_config_tuple_hash
    )
    validate_signed_authorization_claims(
        public_manifest.signed_authorization_claims,
        trusted_issuer=frozen_protocol.authorization_issuer,
        expected_run_id=public_manifest.run_id,
        expected_run_mode=public_manifest.run_mode,
        expected_scope=frozen_protocol.authorization_scope_for(
            public_manifest.run_mode
        ),
        expected_vault_scope=frozen_protocol.vault_scope,
        reject_expired=True,
        check_revocation_epoch=True,
    )
    append_only_manifest.append(public_manifest)
    runtime_barrier = append_only_manifest.close_runtime_barrier_once()
    runtime_caps = capability_broker.open_scoped_for_recipient(
        [public_manifest],
        recipient_hash=runtime_evaluator.identity_hash,
        allowed={"query_need_for_auto_score", "package_for_auto_score",
                 "outcome_ref"},
    )
    assert_capability_content_hashes_match_public_commitments(
        [public_manifest], runtime_caps
    )
    assert_authorization_claims_still_valid(
        public_manifest.signed_authorization_claims,
        at=current_time(),
        check_expiry=True,
        check_revocation=True,
        check_vault_acl=True,
    )
    runtime_outcome_cap = outcome_holder.reveal_union_once_inside(
        runtime_evaluator,
        outcome_ref_capabilities=runtime_caps.outcome_refs,
        require_barriers={runtime_barrier.hash},
    )
    runtime_envelope = runtime_evaluator.score_and_sign_immutable_envelope(
        [public_manifest],
        query_need_capabilities=runtime_caps.query_need,
        package_capabilities=runtime_caps.packages,
        outcome_capability=runtime_outcome_cap,
        frozen_automatic_adjudicators=(
            frozen_protocol.automatic_adjudicator_registry
        ),
        forbid_human_gold_capability=True,
    )
    assert runtime_envelope.config_tuple_hash == (
        frozen_protocol.prior_locked_confirmation_config_tuple_hash
    )
    return emit_runtime_scoped_receipt(
        runtime_envelope,
        receipt_kind="opaque_runtime_audit",
        include_gold_envelope=False,
        expose_per_event_outcomes=False,
    )


def method_local_host_finalize(public_manifest, scoped_receipt, *, local_registry,
                               frozen_protocol, **finalizer_dependencies):
    # 只有 method-local host 能解析 raw state；orchestrator／receipt emitter 无此权限
    submission = local_registry.resolve_by_bound_hashes(
        public_manifest_hash=public_manifest.hash,
        local_state_hash=scoped_receipt.local_state_hash,
    )
    assert submission.public_manifest_hash == public_manifest.hash
    assert submission.hash == public_manifest.local_state_commitment_hash
    if scoped_receipt.receipt_kind == "research_benchmark":
        return finalize_dsr_ct_research_submission(
            submission,
            scoped_benchmark_receipt=scoped_receipt,
            frozen_protocol=frozen_protocol,
            **finalizer_dependencies,
        )
    return finalize_dsr_ct_runtime_submission(
        submission,
        scoped_runtime_receipt=scoped_receipt,
        frozen_protocol=frozen_protocol,
        **finalizer_dependencies,
    )


def validate_registry_scoped_receipt(submission, receipt, frozen_protocol):
    # raw outcome／gold 只存在于独立 evaluator 内；方法与产品 host 均拿不到句柄
    assert receipt.public_manifest_hash == submission.public_manifest_hash
    assert receipt.local_state_hash == submission.hash
    assert receipt.evaluator_contract_hash == submission.evaluator_contract_hash
    assert receipt.evaluator_contract_hash == (
        frozen_protocol.registry_evaluator_contract_hash(submission.run_mode)
    )
    assert receipt.signer_hashes == frozen_protocol.registry_signer_hashes(
        submission.run_mode
    )
    assert receipt.one_union_reveal_only
    assert receipt.no_outcome_was_revealed_before_any_mechanism_seal
    assert receipt.contains_only_submission_scoped_opaque_results
    assert not receipt.exposes_raw_outcome_handle
    assert not receipt.exposes_raw_gold_handle
    if submission.run_mode in {
        "source_confirmation", "offline_rolling_origin",
    }:
        assert receipt.exact_method_set == submission.exact_method_set
        assert receipt.all_method_submissions_frozen_before_reveal
        assert receipt.all_gold_requests_frozen_before_reveal
        assert receipt.has_separately_signed_automatic_and_gold_envelopes
        assert receipt.automatic_envelope_hash_unchanged_after_gold_scoring
    else:
        assert receipt.benchmark_exact_method_set_condition == "not_applicable"
        assert receipt.runtime_barrier_closed_before_reveal
        assert receipt.authorized_method_and_config_hash_match
        assert receipt.human_gold_requested is False
        assert receipt.contains_human_gold is False
        assert receipt.includes_gold_envelope is False
    assert_no_discovery_seed_outcome_in_manifest(
        submission.frozen_packages,
        receipt.run_manifest_digest,
    )


def finalize_dsr_ct_research_submission(
    submission,
    *,
    scoped_benchmark_receipt,
    isolated_historical_evaluator,
    frozen_protocol,
):
    # benchmark gold 只能产生研究评价；不得回流同一轮 shadow／产品选择
    assert submission.run_mode in {
        "source_confirmation", "offline_rolling_origin",
    }
    validate_registry_scoped_receipt(
        submission, scoped_benchmark_receipt, frozen_protocol
    )
    assert scoped_benchmark_receipt.receipt_kind == "research_benchmark"
    assert scoped_benchmark_receipt.context_injection_allowed is False
    assert scoped_benchmark_receipt.gold_used_only_inside_isolated_evaluator

    audit_receipts = scoped_benchmark_receipt.gold_diagnostic_receipts
    automatic_receipts = scoped_benchmark_receipt.automatic_selection_receipts
    assert scoped_benchmark_receipt.automatic_selection_commit.sealed_before(
        scoped_benchmark_receipt.useful_far_gold_first_access_at
    )

    # human gold 只解释／评价，不产生 I_qm，也不决定历史 replay winner
    for receipt in audit_receipts:
        assert receipt.package_hash in {
            p.hash for p in submission.frozen_packages
        }
        assert receipt.grouping_source == "pre_outcome_hidden_gold_cells"
        assert receipt.gold_cell_commit_precedes_union_reveal
        assert receipt.scored_commit_roles == {
            "focus", "all_material_rivals", "query_free_baseline",
            "query_aware_no_discovery",
        }
        assert receipt.applicability_threshold == frozen_protocol.tau_a
        assert receipt.has_metrics({
            "analogue_tpr", "bridge_recall", "positive_cell_tpr",
            "surface_foil_fpr", "null_fpr", "s_contrast",
            "applicability_brier", "applicability_log_loss",
            "outcome_log_loss", "outcome_brier", "paired_regret",
            "query_free_paired_regret",
            "query_aware_no_discovery_paired_regret",
            "bridge_only_paired_regret", "null_fpr_by_subtype",
        })

    automatic_source_passers = []
    for receipt in automatic_receipts:
        assert receipt.used_human_gold is False
        assert receipt.selector_hash == frozen_protocol.automatic_selection_policy_hash
        assert receipt.primary_rank_and_release_flag_frozen_before_gold_access
        if receipt.runtime_equivalent_release_flag:
            automatic_source_passers.append(
                automatically_selected_research_candidate(
                    submission.package(receipt.package_hash), receipt
                )
            )

    # 单候选 receipt 只是研究／校准特征；结论来自独立 query-level confirmation
    if submission.run_mode == "source_confirmation":
        return research_only_receipts(
            automatic_receipts + audit_receipts,
            context_injection_allowed=False,
            forbid_same_run_product_selection=True,
        )

    # 历史 rolling-origin 仍是研究重放；所有 builder 输入已在入口绑定 <t
    replay_winner = select_at_most_one_for_historical_replay(
        automatic_source_passers,
        tie_breaker=frozen_protocol.frozen_shadow_tie_breaker,
    )
    if replay_winner is None:
        return research_only_receipts(
            audit_receipts,
            context_injection_allowed=False,
        )
    replay_contract = freeze_historical_rolling_origin_contract(
        replay_winner,
        origin_time=submission.run_origin_time,
        endpoint=frozen_protocol.target_endpoint,
    )
    assert submission.all_index_snapshot_times_strictly_before(
        replay_contract.origin_time
    )
    replay_receipt = isolated_historical_evaluator.reveal_after_cutoff_once(
        replay_contract,
        require_all_builder_inputs_strictly_before=replay_contract.origin_time,
    )
    return research_only_receipts(
        automatic_receipts + audit_receipts + [replay_receipt],
        context_injection_allowed=False,
    )


def finalize_dsr_ct_runtime_submission(
    submission,
    *,
    scoped_runtime_receipt,
    historical_transport_calibrator,
    frozen_protocol,
):
    # 当前人工 benchmark gold 永不进入 runtime；只使用既往确认过的自动门与风险模型
    assert submission.run_mode in {"shadow", "approved_visible_product"}
    validate_registry_scoped_receipt(
        submission, scoped_runtime_receipt, frozen_protocol
    )
    assert scoped_runtime_receipt.receipt_kind == "opaque_runtime_audit"
    assert scoped_runtime_receipt.contains_human_gold is False
    assert scoped_runtime_receipt.contains_per_event_outcomes is False
    assert scoped_runtime_receipt.automatic_adjudicator_frozen_strictly_before(
        submission.run_origin_time
    )
    assert scoped_runtime_receipt.method_version_has_prior_locked_confirmation

    query = submission.query_context
    need = submission.frozen_need
    run_origin_time = submission.run_origin_time
    authorization = revalidate_stage_bound_approval_receipt(
        submission.authorization_receipt,
        at=current_time(),
        run_mode=submission.run_mode,
        protocol_hash=frozen_protocol.hash,
        model_hash=frozen_protocol.model_hash,
        renderer_hash=frozen_protocol.renderer_hash,
        vault_scope=frozen_protocol.vault_scope,
        check_expiry=True,
        check_revocation=True,
        check_scope=True,
    )
    if not authorization.valid:
        return abstain("authorization_expired_revoked_or_scope_changed")
    if submission.run_mode == "shadow":
        assert authorization.future_shadow_opt_in
        assert authorization.historical_rolling_origin_passed
    else:
        assert authorization.approved_product
        assert authorization.stage_3d_confirmation_passed
        assert authorization.stage_5d_visible_utility_passed
        assert authorization.stage_6d_external_replication_and_product_decision_passed
    release_eligible = []

    for runtime_audit in scoped_runtime_receipt.candidate_runtime_receipts:
        if not (
            runtime_audit.estimated_panel_status == "full_four_cell_panel"
            and runtime_audit.passes_frozen_automatic_source_gates
        ):
            continue
        candidate = runtime_candidate_from_opaque_receipt(
            submission.package(runtime_audit.package_hash), runtime_audit
        )
        transport_risk = historical_transport_calibrator.lookup_cross_fitted(
            mechanism_family=candidate.mechanism_family,
            ontology_hash=frozen_protocol.mechanism_family_ontology_hash,
            query_stratum=need.stratum,
            strictly_before=run_origin_time,
        )
        release_risk = frozen_protocol.release_risk_calibrator.lookup(
            candidate,
            strictly_before=run_origin_time,
        )
        runtime_fit = score_query_relevance_and_nonredundancy(
            query,
            candidate,
            frozen_scorer_hash=frozen_protocol.runtime_fit_scorer_hash,
            forbid={
                "user_intent", "value_judgment", "action",
                "refusal", "permit", "cognitive_profile",
            },
        )
        if not (
            transport_risk.is_calibrated
            and release_risk.is_calibrated
            and runtime_fit.is_calibrated
            and runtime_fit.scorer_hash == frozen_protocol.runtime_fit_scorer_hash
            and runtime_fit.threshold_hash == frozen_protocol.runtime_fit_threshold_hash
        ):
            continue
        if passes_lexicographic_release_gates(
            opaque_runtime_audit=runtime_audit,
            transport_risk=transport_risk,
            release_risk=release_risk,
            runtime_relevance_nonredundancy=runtime_fit,
            risk_threshold=frozen_protocol.selection_risk_threshold,
        ):
            release_eligible.append(
                release_eligible_candidate(
                    candidate, transport_risk, release_risk, runtime_fit
                )
            )

    if submission.run_mode == "shadow":
        shadow_winner = select_at_most_one_for_shadow(
            release_eligible,
            tie_breaker=frozen_protocol.frozen_shadow_tie_breaker,
        )
        if shadow_winner is not None:
            assert_authorization_still_valid_now(
                authorization,
                require_authorization_scope=(
                    frozen_protocol.authorization_scope_for("shadow")
                ),
                require_vault_scope=frozen_protocol.vault_scope,
                require_unrevoked=True,
            )
            register_target_observation_contract(
                shadow_winner,
                origin_time=run_origin_time,
                exposure_status="never_shown",
                evaluation_mode="prospective_future_shadow",
                frozen_endpoint=frozen_protocol.target_endpoint,
                observation_window=frozen_protocol.target_observation_window,
                censoring_rule=frozen_protocol.target_censoring_rule,
                missingness_rule=frozen_protocol.target_missingness_rule,
                outcome_ref=freeze_target_outcome_ref_with_acl(
                    query,
                    endpoint=frozen_protocol.target_endpoint,
                    window=frozen_protocol.target_observation_window,
                ),
                vault_scope=frozen_protocol.vault_scope,
                frozen_predictions={
                    "focus": shadow_winner.focus_prediction,
                    "strong_target_base": shadow_winner.target_base_prediction,
                    "material_rivals": shadow_winner.target_rival_predictions,
                },
                provenance_and_acl_hash=shadow_winner.target_contract_acl_hash,
                confirmed_config_tuple_hash=(
                    frozen_protocol.prior_locked_confirmation_config_tuple_hash
                ),
                authorization_hash=authorization.hash,
            )
        return unchanged_existing_path()

    winner = select_at_most_one_without_reweighting_failed_gates(
        release_eligible,
        tie_breaker=frozen_protocol.frozen_tie_breaker,
    )
    if winner is None:
        return abstain("no_candidate_passed")
    assert_authorization_still_valid_now(
        authorization,
        require_authorization_scope=(
            frozen_protocol.authorization_scope_for("approved_visible_product")
        ),
        require_vault_scope=frozen_protocol.vault_scope,
        require_unrevoked=True,
    )
    # 任一 Spark 内容可见后，当前 query×endpoint 的全部候选均受污染
    mark_query_endpoint_future_as_post_treatment_for_transport(
        query_id=query.id,
        endpoint=frozen_protocol.target_endpoint,
        all_candidate_hashes=[p.hash for p in submission.frozen_packages],
    )
    return render_optional_diagnostic_question(
        winner,
        approved_product_receipt=authorization,
        wording=(
            "来源侧预提交预测优于冻结 rival；"
            "这不说明当前情境成立。可自行检查边界 B。"
        ),
        forbid={
            "action", "plan", "refusal", "permit",
            "fact_assertion", "cognitive_profile", "memory_write",
        },
    )


def settle_target_observation_contract(
    contract,
    *,
    target_outcome_store,
    transport_calibrator,
    authorization_registry,
    acl_registry,
):
    # prospective 延迟闭环：只结算历史校准已通过、另行授权且从未展示的 contract
    assert contract.exposure_status == "never_shown"
    assert contract.evaluation_mode == "prospective_future_shadow"
    assert contract.historical_rolling_origin_passed
    assert contract.future_shadow_authorization_hash_is_valid_at_creation
    assert observation_window_closed(contract)
    assert contract.commitment_time < contract.reveal_time
    # shadow opt-in 可撤销；撤销或 ACL 收紧后绝不读取 outcome
    if not authorization_registry.is_currently_valid(
        contract.authorization_hash,
        require_mode="prospective_future_shadow",
        require_vault_scope=contract.vault_scope,
        require_endpoint=contract.frozen_endpoint,
        check_expiry=True,
        check_revocation=True,
    ):
        return record_target_state(
            contract, "authorization_revoked_no_reveal"
        )
    if not acl_registry.allows_current_read(
        contract.outcome_ref,
        principal=target_outcome_store.identity_hash,
        scope=contract.provenance_and_acl_hash,
    ):
        return record_target_state(contract, "acl_changed_no_reveal")
    if not endpoint_is_naturally_observed(contract):
        return record_target_state(contract, "target_unscoreable")
    if exposure_audit_detects_any_influence(contract):
        return invalidate_contract(contract, "post_treatment_exposure")

    target_y = target_outcome_store.reveal_once(
        contract.outcome_ref,
        receipt_hash=contract.commitment_hash,
    )
    receipt = deterministic_target_scorer(
        frozen_predictions=contract.predictions,
        revealed_outcome=target_y,
        compare_against={"strong_target_base", "material_rivals"},
        metrics={"log_loss", "brier", "calibration"},
    )
    # 只写派生研究账本，并且只影响严格晚于 reveal_time 的后续调用
    transport_calibrator.append_for_future_cross_fit(
        receipt,
        effective_after=contract.reveal_time,
        forbid_memory_truth_write=True,
        forbid_current_call_retroaction=True,
    )
    return receipt
~~~

### 12.0S SOS-PAR 规范性总流程（冻结基线／组件）

下列旧流程必须保留用于忠实基线与来源结果防火墙组件测试，但不再定义新版 Spark 的端到端发布逻辑；发现 seed 与独立验证 panel 的隔离、固定覆盖率主终点和 DSPT shadow 规则以第 12.0 节为准。

以下伪代码优先于旧 WIT 总流程。`OutcomeStore` 与 `BlindIndex` 必须是权限分离的数据面；`commit_log` 是 append-only。任何方法都无权单独调用揭封：benchmark orchestrator 必须等冻结方法集合的 native top-k 与共同面板预测全部提交后，才对候选并集执行同一 run ID 的唯一一次 `reveal_union_once`，并向各方法返回受限视图。

这里严格区分两种 hash：`config_tuple` 只绑定跨锁定确认与独立复制必须保持不变的方法／模型／协议版本；`run_manifest_tuple` 另绑定本次 `benchmark_run_id`、共同面板和冻结方法集合。阶段 4、5、6 比较的是前者，不能要求独立复制复用同一批数据；任何揭封、评分或产品信封同时验证后者，不能把一个 run 的提交或结果拼进另一个 run。

~~~python
def sos_par(
    query,
    blind_index,
    outcome_store,
    frozen_result_blind_baseline,
    frozen_common_panel,
    frozen_method_set,
    benchmark_orchestrator,
    frozen_config,
    execution_authorization,
    inspiration_explicitly_requested=False,
):
    # 灵感是本次请求的显式能力，不是全局配置。未开启时必须在任何冻结、
    # 检索、提交或揭封之前原样旁路，不能用环境变量／部署默认值代替同意。
    if not inspiration_explicitly_requested:
        return unchanged_request_path()
    assert execution_authorization.current_request_inspiration is True
    scope_policy = {
        "sos_research_audit_only": {"none"},
        "approved_stage_1w_core_offline": {"wit_core_offline"},
        "approved_stage_2_component_offline": {
            "wit_slim_locked", "flat_integrity"
        },
        "approved_stage_3_shadow": {"wit_slim_locked", "flat_integrity"},
        "approved_stage_4_locked_confirmation": {
            "wit_slim_locked", "flat_integrity"
        },
        "approved_stage_5_independent_replication": {
            "wit_slim_locked", "flat_integrity"
        },
        "approved_product": {"wit_slim_locked", "flat_integrity"},
    }
    assert execution_authorization.scope in scope_policy
    assert execution_authorization.audit_strategy in (
        scope_policy[execution_authorization.scope]
    )
    assert execution_authorization.permits_post_sos_candidate_audit == (
        execution_authorization.audit_strategy != "none"
    )
    assert execution_authorization.context_injection_allowed == (
        execution_authorization.scope == "approved_product"
    )
    assert execution_authorization.project_owner_signature_is_valid
    stage_gate_evidence = (
        execution_authorization.required_stage_gate_evidence_for(
            execution_authorization.scope
        )
    )
    assert stage_gate_evidence.every_required_evidence_hash_is_present
    assert execution_authorization.gate_flags_match_evidence(
        stage_gate_evidence
    )
    stage_gate_tuple = canonical_hash({
        "scope": execution_authorization.scope,
        "required_gate_evidence": stage_gate_evidence,
    })
    assert frozen_method_set.benchmark_run_id == frozen_config.run_id
    assert frozen_common_panel.benchmark_run_id == frozen_config.run_id
    config_tuple = canonical_hash({
        "audit_strategy": execution_authorization.audit_strategy,
        "sos_protocol": frozen_config.protocol_hash,
        "runtime": frozen_config.runtime_hash,
        "model_prompt": frozen_config.model_prompt_hash,
        "execution": frozen_config.execution_hash,
        "blind_schema": frozen_config.blind_schema_hash,
        "selection_protocol": frozen_config.selection_adjustment_protocol.hash,
        "common_panel_evaluator_protocol": (
            execution_authorization.common_panel_evaluator_protocol_hash
        ),
        "baseline": frozen_result_blind_baseline.commitment_hash,
        "structural_protocol": execution_authorization.structural_protocol_hash,
        "flat_protocol": execution_authorization.flat_verifier_protocol_hash,
        "calibrator": execution_authorization.frozen_calibrator_hash,
        "risk_policy": execution_authorization.product_risk_policy_hash,
        "renderer": execution_authorization.renderer_policy_hash,
    })
    run_manifest_tuple = canonical_hash({
        "config_tuple": config_tuple,
        "benchmark_run_id": frozen_config.run_id,
        "common_panel": frozen_common_panel.commitment_hash,
        "frozen_method_set": frozen_method_set.commitment_hash,
    })
    assert run_manifest_tuple == (
        execution_authorization.authorized_run_manifest_tuple_hash
    )
    authorization_tuple = canonical_hash({
        "config_tuple": config_tuple,
        "run_manifest_tuple": run_manifest_tuple,
        "stage_gate_tuple": stage_gate_tuple,
        "scope": execution_authorization.scope,
        "authorized_run_id": frozen_config.run_id,
        "current_request_inspiration": inspiration_explicitly_requested,
    })
    assert authorization_tuple == (
        execution_authorization.authorized_scope_tuple_hash
    )
    assert execution_authorization.owner_signature_covers(
        config_tuple_hash=config_tuple,
        run_manifest_tuple_hash=run_manifest_tuple,
        stage_gate_tuple_hash=stage_gate_tuple,
        authorization_tuple_hash=authorization_tuple,
        authorized_run_id=frozen_config.run_id,
        scope=execution_authorization.scope,
        audit_strategy=execution_authorization.audit_strategy,
        current_request_inspiration=inspiration_explicitly_requested,
    )
    actual_runtime_hash = current_runtime_hash()
    actual_model_prompt_hash = current_model_prompt_hash()
    actual_execution_hash = current_seed_decoder_and_budget_hash()
    assert actual_runtime_hash == frozen_config.runtime_hash
    assert actual_model_prompt_hash == frozen_config.model_prompt_hash
    assert actual_execution_hash == frozen_config.execution_hash
    if execution_authorization.scope == "sos_research_audit_only":
        assert execution_authorization.stage_0S_A_data_gate_passed
        assert execution_authorization.stage_0S_B_firewall_gate_passed
        assert execution_authorization.stage_1S_approved
    if execution_authorization.audit_strategy != "none":
        assert execution_authorization.global_sos_1s_locked_gates_passed
    if execution_authorization.audit_strategy == "wit_core_offline":
        assert execution_authorization.wit_min_independent_increment_passed
        assert execution_authorization.wit_protocol_0A_0D_complete
        assert execution_authorization.stage_1W_core_approved
        assert execution_authorization.context_injection_allowed is False
        assert execution_authorization.structural_protocol_hash == (
            execution_authorization.stage_1W_core_protocol_hash
        )
    if execution_authorization.audit_strategy == "wit_slim_locked":
        assert execution_authorization.stage_1W_core_locked_gates_passed
        assert execution_authorization.stage_2_wit_slim_protocol_approved
        assert execution_authorization.structural_protocol_hash == (
            execution_authorization.stage_2_wit_slim_protocol_hash
        )
    if execution_authorization.audit_strategy == "flat_integrity":
        assert execution_authorization.wit_online_redundancy_decision_frozen
        assert execution_authorization.flat_verifier_locked_safety_gates_passed
        assert execution_authorization.structural_protocol_hash == (
            execution_authorization.flat_verifier_protocol_hash
        )
    if execution_authorization.scope in {
        "approved_stage_2_component_offline",
        "approved_stage_3_shadow",
        "approved_stage_4_locked_confirmation",
        "approved_stage_5_independent_replication",
        "approved_product",
    }:
        assert execution_authorization.stage_2_component_protocol_approved
    if execution_authorization.scope in {
        "approved_stage_3_shadow",
        "approved_stage_4_locked_confirmation",
        "approved_stage_5_independent_replication",
        "approved_product",
    }:
        assert execution_authorization.stage_2_component_locked_gates_passed
        assert execution_authorization.stage_2_component_cost_acceptable
    if execution_authorization.scope == "approved_stage_3_shadow":
        assert execution_authorization.stage_3_shadow_approved
        assert execution_authorization.context_injection_allowed is False
    if execution_authorization.scope == "approved_stage_4_locked_confirmation":
        assert execution_authorization.stage_3_shadow_locked_gates_passed
        assert execution_authorization.stage_4_locked_confirmation_approved
        assert config_tuple == execution_authorization.stage_4_approved_config_hash
        assert execution_authorization.context_injection_allowed is False
    if execution_authorization.scope == "approved_stage_5_independent_replication":
        assert execution_authorization.stage_4_locked_confirmation_passed
        assert execution_authorization.stage_5_independent_replication_approved
        assert config_tuple == execution_authorization.stage_4_confirmed_config_hash
        assert config_tuple == execution_authorization.stage_5_approved_config_hash
        assert execution_authorization.context_injection_allowed is False
    if execution_authorization.scope == "approved_product":
        assert execution_authorization.stage_4_locked_confirmation_passed
        assert execution_authorization.stage_5_independent_replication_passed
        assert execution_authorization.stage_6_product_decision_approved
        assert execution_authorization.frozen_calibrator_hash
        assert execution_authorization.product_risk_policy_hash
        assert execution_authorization.renderer_policy_hash
        assert config_tuple == execution_authorization.stage_4_confirmed_config_hash
        assert config_tuple == execution_authorization.stage_5_replicated_config_hash
        assert config_tuple == execution_authorization.stage_6_approved_config_hash
        assert execution_authorization.stage_6_product_signature_covers(
            config_tuple_hash=config_tuple,
            run_manifest_tuple_hash=run_manifest_tuple,
            scope="approved_product",
            authorized_run_id=frozen_config.run_id,
            authorization_tuple_hash=authorization_tuple,
        )

    # 0. 候选不可见时冻结查询侧竞争机制和唯一探针。
    q_commit = freeze_query_commitment(
        query=query,
        candidate_access=False,
        hypotheses={"focal", "named_nearest_rival", "other"},
        probes=1,
        release_mode=frozen_config.release_mode,
        hypothesis_priors=frozen_config.hypothesis_priors,
        action_axis_fields={
            "measure", "baseline", "post_level", "positive_endpoint", "zero_band"
        },
        result_axis_fields={
            "subject", "measure", "baseline", "window",
            "positive_endpoint", "zero_band"
        },
    )
    query_mechanism_status = (
        "valid"
        if distributions_are_discriminable(q_commit.H_F, q_commit.H_R)
        else "mechanisms_not_discriminable"
    )

    # 1. 严格结果盲召回。BlindIndex 只含 X、A 及其无结果派生表示。
    assert blind_index.schema_hash == frozen_config.blind_schema_hash
    assert blind_index.outcome_canary_reads == 0
    pool = blind_index.retrieve(q_commit.need, fixed_pool_size=True)
    top_k = fixed_rank(pool, k=8, tie_rule=frozen_config.tie_rule)

    # 共同纯 Brier 面板由外部协议持有者事前冻结，绝不由各方法 top-k 的
    # 揭封后交集生成；本提交不参与 native top-k 排序。
    assert frozen_common_panel.created_before_test_method_scores
    assert frozen_common_panel.outcome_access is False
    panel_commit = precommit_common_panel_forecasts(
        benchmark_run_id=frozen_config.run_id,
        benchmark_config_tuple_hash=(
            frozen_method_set.benchmark_config_tuple_hash
        ),
        method_id="SOS-PAR",
        panel=frozen_common_panel,
        blind_index=blind_index,
        q_commit=q_commit,
        forecast_rule=frozen_config.panel_forecast_rule,
        query_mechanism_status=query_mechanism_status,
        frozen_failure_distribution=(
            frozen_config.precommitted_failure_distribution
        ),
        prohibit_selection_use=True,
        outcome_access=False,
    )

    # 2. 在不知道 Y 的情况下冻结每个候选的 transport 与概率分布。
    candidate_commits = []
    for rank, candidate in enumerate(top_k):
        if query_mechanism_status != "valid":
            candidate_commits.append(precommit_failure(
                candidate,
                rank,
                query_mechanism_status,
                frozen_distribution=(
                    frozen_config.precommitted_failure_distribution
                ),
            ))
            continue
        assert candidate.visible_fields <= {"X", "A", "pre_outcome_provenance"}
        taint_report = verify_no_outcome_taint(candidate)
        if not taint_report.clean:
            candidate_commits.append(precommit_failure(candidate, rank, "tainted"))
            continue

        transport = map_roles_and_axes(
            q_commit=q_commit,
            source_context=candidate.X,
            source_action=candidate.A,
            outcome_access=False,
        )
        p_focal = predict_source_outcome(q_commit.H_F, transport)
        p_rival = predict_source_outcome(q_commit.H_R, transport)
        p_base = frozen_result_blind_baseline.predict(
            candidate.X, candidate.A, candidate.stratum
        )
        p_sos = frozen_mixture(
            q_commit.hypothesis_priors,
            p_focal=p_focal,
            p_rival=p_rival,
            p_other=p_base,
        )
        candidate_commits.append(commit_candidate(
            candidate_id=candidate.id,
            pre_reveal_rank=rank,
            transport=transport,
            sigma_A=transport.action_orientation,
            sigma_O=transport.outcome_orientation,
            delta_A=q_commit.action_axis.zero_band,
            delta_O=q_commit.result_axis.zero_band,
            p_focal=normalize_distribution(p_focal, labels=(-1, 0, +1)),
            p_rival=normalize_distribution(p_rival, labels=(-1, 0, +1)),
            p_base=normalize_distribution(p_base, labels=(-1, 0, +1)),
            p_sos=normalize_distribution(p_sos, labels=(-1, 0, +1)),
            visible_payload_hash=candidate.payload_hash,
            runtime_hash=actual_runtime_hash,
            model_prompt_hash=actual_model_prompt_hash,
            seed_decoder_and_budget_hash=actual_execution_hash,
            failure_rule=frozen_config.failure_rule,
        ))

    candidate_commits = pad_precommitted_missing_slots(
        candidate_commits,
        k=8,
        failure_reason="missing_slot",
        frozen_distribution=frozen_config.precommitted_failure_distribution,
    )

    run_commit = append_only_commit(
        run_id=frozen_config.run_id,
        method_id="SOS-PAR",
        confirmed_config_tuple_hash=config_tuple,
        confirmed_run_manifest_tuple_hash=run_manifest_tuple,
        authorized_scope_tuple_hash=authorization_tuple,
        q_commit=q_commit,
        query_mechanism_status=query_mechanism_status,
        pool_hash=hash_ordered(pool),
        top_k_hash=hash_ordered(top_k),
        candidate_commits=candidate_commits,
        p0=frozen_config.query_independent_source_base_rate,
        base_model_hash=frozen_result_blind_baseline.commitment_hash,
        common_panel_hash=frozen_common_panel.commitment_hash,
        common_panel_prediction_hash=panel_commit.hash,
        frozen_method_set_hash=frozen_method_set.commitment_hash,
        selection_protocol=frozen_config.selection_adjustment_protocol,
    )
    assert execution_authorization.runtime_token_covers(
        run_id=run_commit.id,
        sos_commitment_hash=run_commit.hash,
        config_tuple_hash=config_tuple,
        run_manifest_tuple_hash=run_manifest_tuple,
        authorization_tuple_hash=authorization_tuple,
        scope=execution_authorization.scope,
        audit_strategy=execution_authorization.audit_strategy,
        current_request_inspiration=inspiration_explicitly_requested,
        frozen_method_set_hash=frozen_method_set.commitment_hash,
        protocol_hashes=run_commit.protocol_hashes,
    )

    # 3. 先进入跨方法屏障。Stage 1S 的 SOS、WIT-min、CMI、BM25/dense
    # 等预注册方法必须全部提交 native top-k 与共同面板概率，才允许对它们的
    # 候选并集和共同面板执行唯一一次揭封。产品运行也走同一接口，但其冻结
    # method set 可只含获批方法。任何方法都不能自己提前揭封后再等其他方法。
    method_registration = benchmark_orchestrator.register_pre_reveal_method_commit(
        benchmark_run_id=frozen_config.run_id,
        method_id="SOS-PAR",
        frozen_method_set_hash=frozen_method_set.commitment_hash,
        benchmark_config_tuple_hash=(
            frozen_method_set.benchmark_config_tuple_hash
        ),
        native_commitment_hash=run_commit.hash,
        native_candidate_ids=[
            c.candidate_id for c in candidate_commits
            if c.candidate_id is not None
        ],
        panel_commitment_hash=panel_commit.hash,
        panel_candidate_ids=frozen_common_panel.ordered_candidate_ids,
        append_only_commit_log_position=run_commit.log_position,
    )
    (
        locked_method_set,
        locked_evaluator_authorization,
        locked_reveal_receipt,
    ) = (
        benchmark_orchestrator.close_barrier_and_reveal_union_once(
            benchmark_run_id=frozen_config.run_id,
            frozen_method_set=frozen_method_set,
            registrations_for_exact_required_methods=True,
            require_benchmark_config_tuple_hash=(
                frozen_method_set.benchmark_config_tuple_hash
            ),
            prohibit_unregistered_or_missing_methods=True,
            require_native_and_panel_predictions=True,
            require_every_commit_log_position_precedes_first_reveal=True,
            require_pre_reveal_owner_signature_on_locked_manifest=True,
            require_returned_authorization_binds_evaluator_protocol=True,
            outcome_store=outcome_store,
            common_panel=frozen_common_panel,
        )
    )
    assert method_registration.is_in(locked_method_set)
    assert locked_method_set.benchmark_run_id == frozen_config.run_id
    assert locked_method_set.frozen_method_set_hash == (
        frozen_method_set.commitment_hash
    )
    assert locked_method_set.commitment_hash == canonical_hash({
        "benchmark_run_id": frozen_config.run_id,
        "benchmark_config": frozen_method_set.benchmark_config_tuple_hash,
        "frozen_method_set": frozen_method_set.commitment_hash,
        "registrations": hash_ordered(locked_method_set.registrations),
    })
    assert locked_method_set.method_ids == frozen_method_set.required_method_ids
    assert locked_method_set.all_required_methods_committed_before_reveal
    assert locked_evaluator_authorization.benchmark_run_id == frozen_config.run_id
    assert locked_evaluator_authorization.locked_method_set_hash == (
        locked_method_set.commitment_hash
    )
    assert locked_evaluator_authorization.evaluator_protocol_hash == (
        execution_authorization.common_panel_evaluator_protocol_hash
    )
    assert locked_evaluator_authorization.created_before_first_outcome_reveal
    assert locked_reveal_receipt.revealed_once
    assert locked_reveal_receipt.benchmark_run_id == frozen_config.run_id
    assert locked_reveal_receipt.evaluator_authorization_hash == (
        locked_evaluator_authorization.commitment_hash
    )
    assert locked_reveal_receipt.pre_reveal_evaluator_authorization_verified
    assert locked_reveal_receipt.method_set_commitment_hash == (
        locked_method_set.commitment_hash
    )
    assert locked_reveal_receipt.exact_native_candidate_ids == (
        locked_method_set.union_native_candidate_ids
    )
    assert locked_reveal_receipt.exact_panel_candidate_ids == (
        frozen_common_panel.ordered_candidate_ids
    )
    outcomes = locked_reveal_receipt.native_outcomes_for("SOS-PAR")
    assert append_only_commit_is_unchanged(run_commit)

    # 4. 对所有预提交候选计分，包括失败、unknown 和未通过者。
    audit_rows = []
    for c in candidate_commits:
        y = (
            sealed_missing_slot_outcome()
            if c.candidate_id is None
            else outcomes[c.candidate_id]
        )
        if (
            c.precommit_status != "valid"
            or c.transport.status != "valid"
            or not y.outcome_scoreable
            or y.label == "unknown"
        ):
            audit_rows.append(underdetermined_row(
                c, y,
                native_slot_brier_component=None,
                selection_aware_loss=1.0,
                candidate_localization_status="not_applicable",
                localized_release_status="not_eligible",
                passed=False,
            ))
            continue

        bs_f = normalized_brier(c.p_focal, y.label)
        bs_r = normalized_brier(c.p_rival, y.label)
        bs_base = normalized_brier(c.p_base, y.label)
        bs_sos = normalized_brier(c.p_sos, y.label)  # native slot 的损失分量
        D = bs_r - bs_f
        if q_commit.release_mode == "focal_only":
            tested_h, bs_h = "F", bs_f
            threshold = selection_adjusted_threshold(
                c,
                dimensions={"candidate", "probe"},
                protocol=run_commit.selection_protocol,
            )
            relative_pass = D > threshold.relative
        else:
            tested_h, bs_h = (("F", bs_f) if bs_f <= bs_r else ("R", bs_r))
            threshold = selection_adjusted_threshold(
                c,
                dimensions={"candidate", "mechanism_or_sign", "probe"},
                protocol=run_commit.selection_protocol,
            )
            relative_pass = abs(D) > threshold.relative
        G = bs_base - bs_h
        candidate_localization_status = validate_candidate_localization(
            candidate=c,
            tested_hypothesis=tested_h,
            threshold_object=threshold,
            protocol=run_commit.selection_protocol,
            valid_paths={
                "selection_aware_candidate_specific_test",
                "valid_joint_maxT+exchangeability+subset_pivotality+family_covered",
                "valid_closed_or_conditional_selective_inference",
            },
            require_at_least_one_complete_path=True,
            prohibit_subset_pivotality_alone=True,
            require_complete_selection_rule_covered=True,
        )

        all_candidate_gates_passed = (
            prediction_separation(c.p_focal, c.p_rival)
            and relative_pass
            and G > threshold.absolute_skill
            and negative_controls_pass(c, y)
            and candidate_localization_status == "valid"
        )
        localized_release_status = (
            "valid" if all_candidate_gates_passed else "not_eligible"
        )
        audit_rows.append(score_row(
            c, y,
            D=D,
            G=G,
            tested_h=tested_h,
            native_slot_brier_component=bs_sos,
            selection_aware_loss=bs_sos,
            candidate_localization_status=candidate_localization_status,
            localized_release_status=localized_release_status,
            passed=all_candidate_gates_passed,
        ))

    # 5. 默认到此为止。只有 scope→strategy→阶段授权完整闭合后才运行
    # post-SOS 审计；Stage 1S research scope 不运行结构审计，也不形成可见材料。
    structural_audits = []
    outputs = []
    product_release_audits = []
    if execution_authorization.permits_post_sos_candidate_audit:
        for row in audit_rows_in_pre_reveal_order(audit_rows):
            if row.localized_release_status != "valid":
                continue
            if execution_authorization.audit_strategy in {
                "wit_core_offline", "wit_slim_locked"
            }:
                structural = run_authorized_wit_audit(
                    row.candidate,
                    q_commit,
                    mode=execution_authorization.audit_strategy,
                    protocol_hash=(
                        execution_authorization.structural_protocol_hash
                    ),
                    stage_authorization=execution_authorization,
                    context_injection_allowed=False,
                )
                direction = (
                    cut_direction_audit(structural)
                    if structural.mapping_status == "supported"
                    else direction_unknown("wit_structure_not_supported")
                )
            else:
                assert execution_authorization.audit_strategy == "flat_integrity"
                structural = flat_transport_integrity_verifier(
                    row.candidate,
                    q_commit,
                    protocol_hash=(
                        execution_authorization.flat_verifier_protocol_hash
                    ),
                    prohibit_new_mapping=True,
                    prohibit_direction_judgment=True,
                    context_injection_allowed=False,
                )
                direction = direction_unknown("flat_branch_forces_unknown")
            base_structural_audit = freeze_immutable_base_audit(
                structural,
                explicitly_exclude_fields={"product_release"},
            )
            structural_audits.append(base_structural_audit)
            if base_structural_audit.mapping_status != "supported":
                continue
            diagnostic = render_conditional_diagnostic(
                source_outcome_consistency=row,
                structural_audit=base_structural_audit,
                direction=direction,
                fixed_warning="来源结果相对一致性不证明目标机制，也不决定行为",
            )
            if execution_authorization.scope == "approved_product":
                product_release = evaluate_frozen_product_policy(
                    row=row,
                    structural_audit=base_structural_audit,
                    direction=direction,
                    diagnostic=diagnostic,
                    calibrator=execution_authorization.frozen_calibrator,
                    calibrator_hash=execution_authorization.frozen_calibrator_hash,
                    risk_policy_hash=execution_authorization.product_risk_policy_hash,
                    renderer_policy_hash=execution_authorization.renderer_policy_hash,
                    current_request_inspiration=(
                        inspiration_explicitly_requested
                    ),
                )
                structural_audit_hash = canonical_base_audit_hash(
                    base_structural_audit,
                    explicitly_exclude_fields={"product_release"},
                )
                release_record = immutable_product_release_audit(
                    status=product_release.status,
                    audit_bundle_hash=structural_audit_hash,
                    config_tuple_hash=config_tuple,
                    run_manifest_tuple_hash=run_manifest_tuple,
                    calibrated_risk=product_release.calibrated_risk,
                    risk_gate_passed=product_release.low_risk_gate_passed,
                    reason_codes=product_release.reason_codes,
                    current_scope_injection_allowed=(
                        product_release.current_scope_injection_allowed
                        and inspiration_explicitly_requested
                        and execution_authorization.context_injection_allowed
                    ),
                    renderer_policy_hash=(
                        execution_authorization.renderer_policy_hash
                    ),
                )
                product_audit_pair = bind_product_release_without_mutating_base(
                    immutable_base_audit=base_structural_audit,
                    verified_base_audit_hash=structural_audit_hash,
                    product_release=release_record,
                )
                assert canonical_base_audit_hash(
                    product_audit_pair.base_audit,
                    explicitly_exclude_fields={"product_release"},
                ) == release_record.audit_bundle_hash
                product_release_audits.append(product_audit_pair)
                candidate_visible = (
                    release_record.status == "candidate"
                    and release_record.risk_gate_passed
                    and release_record.current_scope_injection_allowed
                )
                if release_record.status == "speculative":
                    assert release_record.current_scope_injection_allowed is False
                if candidate_visible:
                    product_envelope = render_product_release_envelope(
                        diagnostic=diagnostic,
                        release_audit=release_record,
                        frozen_renderer=(
                            execution_authorization.frozen_renderer_policy
                        ),
                        prohibit_shadow_fields=True,
                        fixed_warning=(
                            "这是可忽略、待核查的灵感材料，不是事实、指令或行为决定"
                        ),
                    )
                    assert verify_product_release_envelope(
                        product_envelope,
                        release_audit_hash=hash_canonical(release_record),
                        config_tuple_hash=config_tuple,
                        run_manifest_tuple_hash=run_manifest_tuple,
                        renderer_policy_hash=(
                            execution_authorization.renderer_policy_hash
                        ),
                        current_request_inspiration=(
                            inspiration_explicitly_requested
                        ),
                    )
                    outputs.append(product_envelope)

    # 6. 完整返回审计分母。除 Stage 6 明确批准的产品 scope 外，
    # diagnostic_material 必须为空；shadow 只保存不可注入的审计 artifact。
    return {
        "diagnostic_material": outputs[:frozen_config.max_visible_items],
        "post_sos_structural_audit_only": structural_audits,
        "product_release_audit": product_release_audits,
        "audit_denominator": audit_rows,
        "common_panel_prediction_commitment": panel_commit,
        "locked_method_set_commitment": locked_method_set,
        "locked_evaluator_authorization": locked_evaluator_authorization,
        "locked_reveal_receipt": locked_reveal_receipt,
        "query_selection_aware_loss": mean_fixed_k_loss(audit_rows, k=8),
        "risk_coverage_state": compute_risk_coverage(audit_rows),
        "commitment_hash": run_commit.hash,
    }
~~~

共同面板的 pure Brier 只能由独立 benchmark orchestrator 在**冻结方法集合的所有方法**提交完成后，消费同一次候选并集揭封产生的验签 receipt 计算；不能从上面的 native top-k 行派生，也不能再次调用 OutcomeStore：

~~~python
def evaluate_locked_common_panel(
    all_method_panel_commits,
    frozen_common_panel,
    locked_method_set,
    locked_reveal_receipt,
    evaluator_authorization,
):
    assert evaluator_authorization.stage == "1S"
    assert evaluator_authorization.stage_0S_A_data_gate_passed
    assert evaluator_authorization.stage_0S_B_firewall_gate_passed
    assert evaluator_authorization.stage_1S_approved
    assert evaluator_authorization.offline_only
    assert evaluator_authorization.context_injection_allowed is False
    assert evaluator_authorization.project_owner_signature_is_valid
    assert locked_method_set.all_required_methods_committed_before_reveal
    assert evaluator_authorization.benchmark_run_id == (
        locked_method_set.benchmark_run_id
    )
    assert frozen_common_panel.benchmark_run_id == (
        locked_method_set.benchmark_run_id
    )
    assert evaluator_authorization.benchmark_config_tuple_hash == (
        locked_method_set.benchmark_config_tuple_hash
    )
    assert evaluator_authorization.frozen_method_set_hash == (
        locked_method_set.frozen_method_set_hash
    )
    assert evaluator_authorization.locked_method_set_hash == (
        locked_method_set.commitment_hash
    )
    assert {c.method_id for c in all_method_panel_commits} == (
        locked_method_set.method_ids
    )
    assert len(all_method_panel_commits) == len(locked_method_set.method_ids)
    ordered_panel_commits = order_by_frozen_method_ids(
        all_method_panel_commits,
        locked_method_set.method_ids,
    )
    assert all(
        c.panel_hash == frozen_common_panel.commitment_hash
        and c.covers_every_candidate_in_panel
        and c.created_before_any_panel_outcome_reveal
        and c.benchmark_run_id == locked_method_set.benchmark_run_id
        and c.benchmark_config_tuple_hash == (
            locked_method_set.benchmark_config_tuple_hash
        )
        and c.hash == (
            locked_method_set.registration_for(c.method_id)
            .panel_commitment_hash
        )
        for c in ordered_panel_commits
    )
    pre_reveal_evaluator_tuple = canonical_hash({
        "stage": "1S",
        "benchmark_run_id": locked_method_set.benchmark_run_id,
        "benchmark_config": evaluator_authorization.benchmark_config_tuple_hash,
        "method_set": locked_method_set.commitment_hash,
        "required_method_ids": locked_method_set.method_ids,
        "common_panel": frozen_common_panel.commitment_hash,
        "panel_commitments": hash_ordered(ordered_panel_commits),
        "evaluator_protocol": evaluator_authorization.evaluator_protocol_hash,
    })
    assert evaluator_authorization.created_after(locked_method_set)
    assert evaluator_authorization.created_before_first_outcome_reveal
    assert pre_reveal_evaluator_tuple == (
        evaluator_authorization.authorized_evaluator_tuple_hash
    )
    assert evaluator_authorization.owner_signature_covers(
        evaluator_tuple_hash=pre_reveal_evaluator_tuple,
        benchmark_run_id=locked_method_set.benchmark_run_id,
        benchmark_config_tuple_hash=(
            evaluator_authorization.benchmark_config_tuple_hash
        ),
        method_set_hash=locked_method_set.commitment_hash,
        panel_hash=frozen_common_panel.commitment_hash,
        exact_panel_commitment_hashes=hash_ordered(ordered_panel_commits),
        evaluator_protocol_hash=evaluator_authorization.evaluator_protocol_hash,
    )
    assert locked_reveal_receipt.revealed_once
    assert locked_reveal_receipt.pre_reveal_evaluator_authorization_verified
    assert locked_reveal_receipt.benchmark_run_id == (
        locked_method_set.benchmark_run_id
    )
    assert locked_reveal_receipt.method_set_commitment_hash == (
        locked_method_set.commitment_hash
    )
    assert locked_reveal_receipt.exact_panel_candidate_ids == (
        frozen_common_panel.ordered_candidate_ids
    )
    assert locked_reveal_receipt.created_after_every_method_commit
    assert evaluator_authorization.append_only_log_position < (
        locked_reveal_receipt.first_outcome_reveal_log_position
    )
    assert locked_reveal_receipt.evaluator_authorization_hash == (
        evaluator_authorization.commitment_hash
    )
    evaluation_receipt_tuple = canonical_hash({
        "pre_reveal_evaluator_tuple": pre_reveal_evaluator_tuple,
        "locked_reveal_receipt": locked_reveal_receipt.commitment_hash,
    })
    assert locked_reveal_receipt.orchestrator_signature_covers(
        evaluation_receipt_tuple_hash=evaluation_receipt_tuple,
        benchmark_run_id=locked_method_set.benchmark_run_id,
        locked_method_set_hash=locked_method_set.commitment_hash,
    )
    # evaluator 只消费跨方法屏障产生的同一份已验签 receipt，绝不再次调用
    # OutcomeStore，也不能给某个迟到方法单独揭封或补跑面板预测。
    panel_outcomes = locked_reveal_receipt.panel_outcomes
    common_scoreable_ids = [
        cid for cid in frozen_common_panel.ordered_candidate_ids
        if panel_outcomes[cid].gold_valid
        and panel_outcomes[cid].outcome_scoreable
    ]
    return score_pure_brier_and_calibration(
        commits=ordered_panel_commits,
        outcomes=panel_outcomes,
        exact_common_ids=common_scoreable_ids,
        prohibit_native_top_k_intersection=True,
        prohibit_post_reveal_prediction=True,
        report_original_panel_size_and_scoreable_coverage=True,
    )
~~~

必须另有一个不调用生成模型的完整性检查器重放：字段白名单、canary 读取、pool／top-k／预测 hash、一次揭封次数、揭封前后提交不变性、全部分母、负控和输出顺序。该检查器不能判断语义真伪，但能阻断大部分后见泄漏和选择性报告。

### 12.1 WIT-VS 规范性总流程

DSR 阶段 1D 的 `WIT-min consensus` 基线与后续条件完整 WIT 必须是两个不同入口，不能靠一个 mode 开关共享完整 solver／checker 后再声称前者“最小”。1D 允许的 WIT 基线为：

~~~python
def wit_min_consensus_baseline(
    context,
    fixed_blind_cef_candidates,
    frozen_min_rule,
    frozen_probability_adapter,
    frozen_common_panel,
    frozen_method_set,
    benchmark_orchestrator,
    offline_authorization,
):
    assert offline_authorization.stage == "1S"
    assert offline_authorization.offline_only
    assert offline_authorization.context_injection_allowed is False
    assert offline_authorization.stage_0S_A_data_gate_passed
    assert offline_authorization.stage_0S_B_firewall_gate_passed
    assert offline_authorization.stage_1S_approved
    assert offline_authorization.project_owner_signature_is_valid
    assert offline_authorization.binds_protocol_hash == frozen_min_rule.hash
    assert offline_authorization.fixed_candidate_set_hash == (
        hash_ordered(fixed_blind_cef_candidates)
    )
    assert offline_authorization.common_panel_hash == (
        frozen_common_panel.commitment_hash
    )
    assert offline_authorization.probability_adapter_hash == (
        frozen_probability_adapter.hash
    )
    assert offline_authorization.frozen_method_set_hash == (
        frozen_method_set.commitment_hash
    )
    assert offline_authorization.benchmark_run_id == (
        frozen_method_set.benchmark_run_id
    )
    assert frozen_common_panel.benchmark_run_id == (
        offline_authorization.benchmark_run_id
    )
    assert "WIT-min-consensus" in frozen_method_set.required_method_ids
    wit_min_tuple = canonical_hash({
        "stage": "1S",
        "benchmark_run_id": offline_authorization.benchmark_run_id,
        "protocol": frozen_min_rule.hash,
        "candidate_set": hash_ordered(fixed_blind_cef_candidates),
        "common_panel": frozen_common_panel.commitment_hash,
        "probability_adapter": frozen_probability_adapter.hash,
        "method_set": frozen_method_set.commitment_hash,
        "benchmark_config": frozen_method_set.benchmark_config_tuple_hash,
        "offline_only": offline_authorization.offline_only,
        "context_injection_allowed": (
            offline_authorization.context_injection_allowed
        ),
    })
    assert wit_min_tuple == offline_authorization.authorized_wit_min_tuple_hash
    assert offline_authorization.owner_signature_covers(
        wit_min_tuple_hash=wit_min_tuple,
        stage="1S",
        benchmark_run_id=offline_authorization.benchmark_run_id,
        protocol_hash=frozen_min_rule.hash,
        candidate_set_hash=hash_ordered(fixed_blind_cef_candidates),
        common_panel_hash=frozen_common_panel.commitment_hash,
        probability_adapter_hash=frozen_probability_adapter.hash,
        method_set_hash=frozen_method_set.commitment_hash,
        benchmark_config_tuple_hash=(
            frozen_method_set.benchmark_config_tuple_hash
        ),
        offline_only=True,
        context_injection_allowed=False,
    )
    assert benchmark_orchestrator.no_outcome_reveal_has_occurred(
        offline_authorization.benchmark_run_id
    )
    assert frozen_probability_adapter.fit_only_on_protocol_development_queries
    assert frozen_probability_adapter.outcome_access_on_test is False
    assert frozen_min_rule.forbids_full_version_space_solver
    assert frozen_min_rule.forbids_active_layering
    assert frozen_min_rule.forbids_product_or_shadow_state
    consensus = fixed_cost_blind_consensus(
        context=context,
        candidates=fixed_blind_cef_candidates,
        use_outcome=False,
        output_status="offline_wit_min_audit_only",
    )
    native_ranked = fixed_rank(consensus, k=8, tie_rule=frozen_min_rule.tie_rule)
    predicted_native_distributions = (
        frozen_probability_adapter.predict_distributions(
            context=context,
            candidates=native_ranked,
            labels=(-1, 0, +1),
            outcome_access=False,
        )
    )
    native_top_k = pad_ranked_candidates_to_fixed_k(
        native_ranked,
        k=8,
        empty_slot=None,
    )
    native_distributions = align_distributions_to_fixed_slots(
        ranked_candidates=native_ranked,
        ranked_distributions=predicted_native_distributions,
        fixed_slots=native_top_k,
        missing_slot_distribution=(
            frozen_probability_adapter.precommitted_missing_slot_distribution
        ),
        missing_slot_loss=1.0,
    )
    panel_distributions = frozen_probability_adapter.predict_distributions(
        context=context,
        candidates=frozen_common_panel.candidates,
        labels=(-1, 0, +1),
        outcome_access=False,
    )
    commitment = append_only_commit(
        benchmark_run_id=offline_authorization.benchmark_run_id,
        benchmark_config_tuple_hash=(
            frozen_method_set.benchmark_config_tuple_hash
        ),
        method_id="WIT-min-consensus",
        protocol_hash=frozen_min_rule.hash,
        candidate_set_hash=offline_authorization.fixed_candidate_set_hash,
        native_top_k_ids=[c.id if c is not None else None for c in native_top_k],
        native_fixed_k=8,
        missing_slot_loss=1.0,
        native_distributions=native_distributions,
        panel_hash=frozen_common_panel.commitment_hash,
        panel_candidate_ids=frozen_common_panel.ordered_candidate_ids,
        panel_distributions=panel_distributions,
        probability_adapter_hash=frozen_probability_adapter.hash,
        frozen_method_set_hash=frozen_method_set.commitment_hash,
        prohibit_post_reveal_refit=True,
    )
    method_registration = benchmark_orchestrator.register_pre_reveal_method_commit(
        benchmark_run_id=offline_authorization.benchmark_run_id,
        method_id="WIT-min-consensus",
        frozen_method_set_hash=frozen_method_set.commitment_hash,
        benchmark_config_tuple_hash=(
            frozen_method_set.benchmark_config_tuple_hash
        ),
        native_commitment_hash=commitment.hash,
        native_candidate_ids=[c.id for c in native_top_k if c is not None],
        panel_commitment_hash=commitment.hash,
        panel_candidate_ids=frozen_common_panel.ordered_candidate_ids,
        append_only_commit_log_position=commitment.log_position,
    )
    return {
        "status": "offline_wit_min_audit_only",
        "native_top_k": native_top_k,
        "native_distributions": native_distributions,
        "panel_distributions": panel_distributions,
        "pre_reveal_commitment": commitment,
        "method_registration": method_registration,
        "context_injection_allowed": False,
    }
~~~

下述流程只定义**阶段 1W 的 WIT-core 审计**，不是阶段 2 的完整 WIT，更不定义产品 WIT-slim。它只有在 SOS-PAR 的 1S 全局锁定门已通过、WIT-min 非冗余增量已通过、WIT 子协议 0A–0D 已签署、项目所有者明确批准阶段 1W，并且具体候选 SOS 门与升级定位门都通过时才可执行；主动信息增益、完整响应张量和产品状态仍被禁止。阶段 2 若要开发完整 WIT 或产品 WIT-slim，必须另写协议、接口与授权 hash，不能复用本入口冒充已批准。它不替代第 12.0 节的新版主流程，也不能自行形成模型可见输出：

~~~python
def spark_wit_core_audit(
    context,
    blind_cef_store,
    fixed_candidate_bundles,
    execution_scope,
    audit_authorization,
    inspiration=False,
    tension=None,
    frozen_protocol=None,
    calibrator=None,
):
    if not inspiration:
        return unchanged_request_path()
    if execution_scope != "stage_1w_core_audit":
        return shadow_none("WIT 执行范围未获授权")
    assert audit_authorization.global_sos_1s_locked_gates_passed
    assert audit_authorization.wit_min_independent_increment_passed
    assert audit_authorization.wit_protocol_0A_0D_complete
    assert audit_authorization.stage_1W_core_approved
    assert audit_authorization.offline_only
    assert audit_authorization.context_injection_allowed is False
    assert audit_authorization.forbids_active_layering
    assert audit_authorization.forbids_full_response_tensor
    assert audit_authorization.forbids_product_state
    assert audit_authorization.project_owner_signature_is_valid
    assert audit_authorization.binds_current_sos_run
    assert all(
        c.sos_gate_status == "passed"
        and c.localized_release_status == "valid"
        and c.sos_commitment_hash == audit_authorization.sos_commitment_hash
        for c in fixed_candidate_bundles
    )

    if frozen_protocol is None or not frozen_protocol.is_version_locked:
        return shadow_none("缺少锁定 WIT-VS 协议，按 fail-closed 停止")

    wit_core_tuple = canonical_hash({
        "scope": execution_scope,
        "current_request_inspiration": inspiration,
        "sos_commitment": audit_authorization.sos_commitment_hash,
        "candidate_set": hash_ordered(fixed_candidate_bundles),
        "wit_protocol": frozen_protocol.content_hash,
        "model_prompt": frozen_protocol.model_and_prompt_versions,
        "evidence_schema": frozen_protocol.evidence_schema,
        "transport_grammar": frozen_protocol.transport_grammar,
        "codebooks": frozen_protocol.codebook_family_hash,
    })
    assert wit_core_tuple == audit_authorization.stage_1W_core_tuple_hash
    assert audit_authorization.owner_signature_covers(
        tuple_hash=wit_core_tuple,
        execution_scope=execution_scope,
        current_request_inspiration=inspiration,
        sos_run=audit_authorization.sos_commitment_hash,
    )

    # 任何校准器必须和协议、模型、prompt、schema、codebook 同版本。
    if calibrator is not None and not calibrator.matches(
        protocol_hash=frozen_protocol.content_hash,
        model_and_prompt_versions=frozen_protocol.model_and_prompt_versions,
        evidence_schema=frozen_protocol.evidence_schema,
        transport_grammar=frozen_protocol.transport_grammar,
        codebook_family_hash=frozen_protocol.codebook_family_hash,
    ):
        calibrator = None

    # A. 候选不可见：编译当前 Need Frame、允许坐标和探针划分。
    need_drafts = independent_compile_need_frame(
        context=context,
        explicit_tension=tension,
        candidate_access=False,
        memory_access=False,
        allow_unknown=True,
    )
    need = freeze_need_consensus_with_source_evidence(need_drafts)
    if not need.has_specific_unresolved_obligation:
        return shadow_none("当前上下文没有可识别的未决需要")

    probes = instantiate_and_split_probes(
        need=need,
        templates=frozen_protocol.probe_templates,
        candidate_access=False,
        deterministic_split_rule=frozen_protocol.probe_split_rule,
        splits=("structural_fit", "response_fit", "internal_audit_heldout"),
    )
    query_commitment = commit_query_bundle(
        need=need,
        probes=probes,
        claim_lattice=freeze_claim_lattice_before_candidate_access(
            need=need,
            schema=frozen_protocol.claim_schema,
            minimum_specificity=frozen_protocol.minimum_claim_specificity,
            minimum_need_coverage=frozen_protocol.minimum_claim_need_coverage,
            unique_fallback_rule=frozen_protocol.claim_fallback_rule,
        ),
        grammar=frozen_protocol.transport_grammar,
        epsilon_policy=frozen_protocol.version_space_epsilon_policy,
        codebooks=frozen_protocol.primary_and_sensitivity_codebooks,
        codebook_family_hash=frozen_protocol.codebook_family_hash,
        nulls=frozen_protocol.null_generators,
    )

    # 负结构状态不是正候选发布：只在穷尽证书经独立 checker 重放后保留研究审计。
    def checked_typed_negative_audit(
        candidate, mapping_status, reason, certificate
    ):
        bundle = typed_negative_bundle(
            candidate=candidate,
            mapping_status=mapping_status,
            direction_class="unknown",
            reason=reason,
            certificate=certificate,
            context_injection_allowed=False,
            product_release_eligible=False,
        )
        checker_result = wit_check.verify_typed_negative(
            bundle=bundle,
            frozen_protocol=frozen_protocol,
            require_every_original_completion_codebook_and_transport_covered=True,
            require_same_scope_and_modality_for_logical_contradiction=True,
            require_precommitted_universal_invariant_for_structural_impossibility=True,
            require_source_spans_and_hashes=True,
            prohibit_llm_calls=True,
        )
        bundle.certificate_checker = checker_result.certificate
        bundle.full_pipeline_null_gate = {
            "structural": {
                "control_mode": "not_applicable_typed_negative_audit",
                "candidate_localized": False,
                "passed": None,
            },
            "direction": {"control_mode": "not_applicable", "passed": None},
        }
        if not checker_result.passed:
            fallback = unknown_bundle(
                candidate, "typed_negative_certificate_check_failed"
            )
            fallback.pre_null_claim_audit = bundle.to_nonreleasable_audit_snapshot()
            fallback.certificate_checker = checker_result.certificate
            fallback.context_injection_allowed = False
            return fallback
        bundle.status = "shadow_hypothesis"
        bundle.risk_band = "uncalibrated"
        return bundle

    # B. 只消费调用方已冻结的候选，不在 WIT 内再次召回、补召回或改顺序。
    candidates = load_authorized_fixed_candidates(
        fixed_candidate_bundles,
        preserve_input_order=True,
        prohibit_retrieval=True,
        prohibit_shortlist_expansion=True,
    )

    audits = []
    accepted = []

    for candidate in candidates:
        # C. 严格版只读取查询到达前已提交的 CEF；缺失项仅为未来请求排队重建。
        cef = blind_cef_store.load_prequery_committed_blind_cef(
            candidate_id=candidate.id,
            protocol=frozen_protocol.cef_protocol,
            require_source_spans=True,
            require_current_source_hash=True,
            preserve_all_evidence_compatible_completions=True,
        )
        if cef is None:
            audits.append(unknown_bundle(candidate, "cef_not_precommitted"))
            continue
        if not cef.minimum_evidence_gate_passes:
            audits.append(unknown_bundle(candidate, "cef_evidence_insufficient"))
            continue

        surface = measure_surface_views_without_structural_reward(
            query=need.public_view,
            candidate=candidate,
            frozen_views=frozen_protocol.surface_views,
            return_interval_on_disagreement=True,
        )

        # D. 响应证据仍遮蔽；只在有限 DSL 内综合映射骨架。
        search = solve_evidence_witnessed_transport_family(
            need_skeleton=need.mask_response_direction().mapping_skeleton,
            candidate_skeleton=cef.mask_response_direction(),
            query_completions=need.completions,
            candidate_completions=cef.completions,
            structural_fit_probes=probes.structural_fit,
            grammar=frozen_protocol.transport_grammar,
            codebooks=frozen_protocol.primary_and_sensitivity_codebooks,
            epsilon_policy=frozen_protocol.version_space_epsilon_policy,
            return_union_and_per_codebook_optima=True,
            hard_constraints=frozen_protocol.transport_hard_constraints,
            budget=frozen_protocol.transport_solver_budget,
            prohibit_ops=(
                "invent_edge", "invent_role", "drop_required",
                "flip_response", "posthoc_axis", "posthoc_mixed_partition",
            ),
        )
        if not search.optimality_gap_closed_for_every_codebook_and_completion:
            audits.append(unknown_bundle(candidate, "solver_unclosed_gap"))
            continue
        if search.has_out_of_language_or_unclassified_completion:
            audits.append(unknown_bundle(candidate, "completion_out_of_language"))
            continue
        if search.all_original_completions_have_universal_contradiction_certificate:
            contradiction = search.exhaustive_incompatibility_certificate
            if contradiction.type == "logical_same_scope_contradiction":
                audits.append(
                    checked_typed_negative_audit(
                        candidate,
                        mapping_status="contradicted",
                        reason="logical_contradiction",
                        certificate=contradiction,
                    )
                )
            elif contradiction.type == "structural_impossibility":
                audits.append(
                    checked_typed_negative_audit(
                        candidate,
                        mapping_status="no_supported_correspondence",
                        reason="no_supported_transport",
                        certificate=contradiction,
                    )
                )
            else:
                audits.append(
                    unknown_bundle(candidate, "untyped_incompatibility_certificate")
                )
            continue
        if (
            search.transportable_completion_ids
            != search.original_evidence_compatible_completion_ids
        ):
            audits.append(unknown_bundle(candidate, "completion_class_disagreement"))
            continue

        # 所有敏感性 codebook × 最大 epsilon 的并集必须在响应揭封前一起提交。
        initial_space = build_symbolic_program_completion_version_space(
            search=search,
            per_codebook_completion_max_cost={
                codebook_completion: (
                    optimum
                    + frozen_protocol.version_space_epsilon_policy.maximum_release_epsilon
                )
                for codebook_completion, optimum
                in search.per_codebook_completion_optima.items()
            },
            retain_union_across_all_precommitted_codebooks=True,
            retain_all_evidence_compatible_program_completion_triples=True,
        )
        if (
            initial_space.codebook_completion_ids
            != search.original_codebook_completion_ids
        ):
            audits.append(unknown_bundle(candidate, "completion_coverage_missing_before_fit"))
            continue

        committed_space = commit_before_response_reveal(
            version_space=initial_space,
            axis_orientations=initial_space.axis_orientations,
            mixed_hypothesis_key_library=frozen_protocol.allowed_mixed_hypothesis_keys,
            query_commitment=query_commitment,
        )

        # E. response_fit 只允许淘汰已提交程序，不得新增或改写程序。
        fit_responses = answer_response_probes_independently(
            cef=cef.reveal_allowed_response_evidence(),
            need=need.reveal_allowed_response_evidence(),
            version_space=committed_space,
            probes=probes.response_fit,
            require_source_spans=True,
            allow_unknown=True,
        )
        fitted_space = eliminate_inconsistent_triples_only(
            committed_space=committed_space,
            fit_responses=fit_responses,
            rule=frozen_protocol.fit_elimination_rule,
            direction_hypothesis_family=(
                "aligned", "opposed", "H0_dir",
                *frozen_protocol.allowed_mixed_hypothesis_keys,
            ),
            retain_all_fit_consistent_direction_hypotheses_per_member=True,
            require_direction_class_symmetric_elimination=True,
            require_global_response_flip_preserves_member_set=True,
            prohibit_aligned_preference=True,
            prohibit_new_programs=True,
            prohibit_new_completions=True,
            prohibit_reorientation=True,
        )
        if fitted_space.is_empty:
            empty_certificate = classify_empty_fitted_space_reason(
                committed_space=committed_space,
                fit_responses=fit_responses,
                require_exhaustive_precommitted_necessary_constraint_conflicts=True,
            )
            if empty_certificate.proves_every_original_triple_incompatible:
                if empty_certificate.type == "logical_same_scope_contradiction":
                    audits.append(
                        checked_typed_negative_audit(
                            candidate,
                            mapping_status="contradicted",
                            reason="logical_contradiction",
                            certificate=empty_certificate,
                        )
                    )
                elif empty_certificate.type == "structural_impossibility":
                    audits.append(
                        checked_typed_negative_audit(
                            candidate,
                            mapping_status="no_supported_correspondence",
                            reason="no_supported_transport",
                            certificate=empty_certificate,
                        )
                    )
                else:
                    audits.append(
                        unknown_bundle(candidate, "untyped_incompatibility_certificate")
                    )
            else:
                audits.append(
                    unknown_bundle(
                        candidate,
                        "response_evidence_insufficient_or_unresolved",
                        certificate=empty_certificate,
                    )
                )
            continue
        if (
            fitted_space.codebook_completion_ids
            != initial_space.codebook_completion_ids
        ):
            audits.append(unknown_bundle(candidate, "completion_coverage_missing_after_fit"))
            continue

        # F. heldout 先只评价结构；方向审计必须等结构证书形成后才能启动。
        heldout = evaluate_blind_structural_heldout_for_all_triples(
            version_space=fitted_space,
            probes=probes.internal_audit_heldout,
            cef=cef.mask_response_direction(),
            need=need.mask_response_direction(),
            claim_lattice=query_commitment.claim_lattice,
            fixed_claim_fallback_rule=frozen_protocol.claim_fallback_rule,
            multiplicity_rule=frozen_protocol.claim_lattice_multiplicity_rule,
            prohibit_response_direction_access=True,
            nulls=generate_matched_nulls(
                candidate=candidate,
                need=need,
                generators=frozen_protocol.null_generators,
            ),
            prohibit_best_program_selection=True,
        )

        # 只从同一已提交并集切片，先做诊断；不得用有利子空间救活并集。
        sensitivity_views = summarize_precommitted_sensitivity_subspaces(
            fitted_union_space=fitted_space,
            epsilon_policy=frozen_protocol.version_space_epsilon_policy,
            codebooks=frozen_protocol.primary_and_sensitivity_codebooks,
            claim_lattice=query_commitment.claim_lattice,
            heldout=heldout,
            derive_subsets_only_from_committed_union=True,
            prohibit_new_programs_or_completions=True,
            prohibit_refit_or_heldout_selection=True,
        )
        epsilon_sensitivity = sensitivity_views.epsilon
        codebook_sensitivity = sensitivity_views.codebook

        structural_rival = search_near_optimal_rival_with_different_structural_claim(
            symbolic_space=fitted_space,
            fit_constraints=fit_responses,
            heldout_predictions=heldout.predictions_only,
            claim_lattice=query_commitment.claim_lattice,
            fixed_claim_fallback_rule=frozen_protocol.claim_fallback_rule,
            prohibit_conditioning_on_heldout_gold=True,
            include_unknown_as_release_rival=True,
            solver_budget=frozen_protocol.rival_search_budget,
        )
        if structural_rival.exists or not structural_rival.search_gap_closed:
            audits.append(
                unknown_bundle(
                    candidate,
                    reason=(
                        "mapping_nonidentifiable"
                        if structural_rival.exists else "rival_search_unclosed_gap"
                    ),
                    rival_programs=structural_rival.programs,
                    cheapest_discriminating_probe=structural_rival.best_next_probe,
                    epsilon_sensitivity=epsilon_sensitivity,
                    codebook_sensitivity=codebook_sensitivity,
                )
            )
            continue

        robust_mapping_state = classify_version_space_robust_mapping_state(
            version_space=fitted_space,
            heldout=heldout,
            claim_lattice=query_commitment.claim_lattice,
            fixed_claim_fallback_rule=frozen_protocol.claim_fallback_rule,
            multiplicity_rule=frozen_protocol.claim_lattice_multiplicity_rule,
            sensitivity_views=sensitivity_views,
            require_every_program_and_completion_same_supported_structural_claim=True,
            record_selected_claim_node_and_attempt_order=True,
            thresholds=frozen_protocol.release_thresholds,
        )
        if not robust_mapping_state.release_gate_passed:
            audits.append(
                unknown_or_rejected_bundle(
                    candidate,
                    robust_mapping_state,
                    epsilon_sensitivity=epsilon_sensitivity,
                    codebook_sensitivity=codebook_sensitivity,
                )
            )
            continue

        if epsilon_sensitivity.structural_claim_flips:
            audits.append(unknown_bundle(candidate, "epsilon_structural_claim_flip"))
            continue
        if codebook_sensitivity.structural_claim_flips_or_audit_incomplete:
            audits.append(unknown_bundle(candidate, "codebook_structural_claim_flip"))
            continue

        # 只有结构证书通过后，CUT 才能在同一已提交 (T, omega, codebook) 空间中审计方向。
        signed_state = spark_cut_direction_audit(
            fitted_version_space=fitted_space,
            robust_mapping_certificate=robust_mapping_state.certificate,
            query_commitment=query_commitment,
            cef=cef,
            internal_audit_heldout=probes.internal_audit_heldout,
            frozen_protocol=frozen_protocol,
        )
        if signed_state.has_complete_per_member_states:
            direction_sensitivity = summarize_signed_direction_by_precommitted_subspace(
                signed_union_state=signed_state,
                per_member_states=signed_state.per_member_states,
                sensitivity_views=sensitivity_views,
                derive_subsets_only_from_same_committed_union=True,
            )
            if (
                signed_state.direction_class in {"aligned", "opposed", "mixed"}
                and direction_sensitivity.direction_flips_or_audit_incomplete
            ):
                signed_state = direction_unknown(
                    reason="epsilon_or_codebook_direction_flip",
                    preserve_structural_candidate=True,
                    sensitivity_certificate=direction_sensitivity.certificate,
                )
            elif signed_state.direction_class == "unknown":
                signed_state.attach_diagnostic_without_replacing_primary_reason(
                    direction_sensitivity.certificate
                )
        else:
            direction_sensitivity = direction_sensitivity_not_applicable(
                primary_reason=signed_state.reason,
                preserve_primary_unknown_reason=True,
            )
        direction_rival = getattr(
            signed_state, "direction_rival", not_applicable_direction_rival()
        )

        # G. 结构通过后才检查当前未决槽位和表层近远，二者不能救活结构失败。
        need_value = audit_incremental_need_value(
            need=need,
            candidate=cef,
            version_space=fitted_space,
            require_new_supported_slot=True,
            prohibit_generic_interestingness_score=True,
        )
        if not need_value.has_nonredundant_supported_gain:
            audits.append(rejected_bundle(candidate, "structurally_valid_but_redundant"))
            continue

        mechanism_interval = conservative_mechanism_distance_interval(
            version_space=fitted_space,
            frozen_normalization=frozen_protocol.mechanism_distance_normalization,
        )
        relation_band = derive_near_or_remote_only_after_structural_pass(
            mechanism_interval=mechanism_interval,
            surface_interval=surface,
            thresholds=frozen_protocol.near_far_thresholds,
        )

        break_test = search_lowest_cost_mechanism_break(
            candidate=cef,
            version_space=fitted_space,
            frozen_space=frozen_protocol.break_search_space,
        )
        residual = search_highest_loss_residual_counterexample(
            candidate=cef,
            version_space=fitted_space,
            frozen_space=frozen_protocol.residual_search_space,
        )

        bundle = build_witness_bundle(
            candidate=candidate,
            query_commitment=query_commitment,
            source_hash=cef.source_hash,
            cef_payload_digest=cef.payload_digest,
            cef_commitment_hash=cef.commitment_hash,
            need=need,
            version_space=fitted_space,
            heldout=heldout,
            robust_mapping_state=robust_mapping_state,
            claim_release_certificate=robust_mapping_state.claim_release_certificate,
            epsilon_sensitivity=epsilon_sensitivity,
            codebook_sensitivity=codebook_sensitivity,
            direction_sensitivity=direction_sensitivity,
            signed_state=signed_state,
            structural_rival=structural_rival,
            direction_rival=direction_rival,
            surface=surface,
            mechanism_interval=mechanism_interval,
            relation_band=relation_band,
            need_value=need_value,
            lowest_cost_break=break_test,
            highest_loss_residual=residual,
            pre_null_claim_audit=None,
            context_injection_allowed=False,
        )

        # 整池 look-elsewhere 门尚未运行；此处无条件只能形成临时 shadow。
        bundle.status = "shadow_hypothesis"
        bundle.risk_band = "uncalibrated"
        bundle.full_pipeline_null_gate = "pending"

        audits.append(bundle)
        if bundle.status != "none":
            accepted.append(bundle)

    if not accepted:
        return wit_audit_none_with_complete_audit(audits)

    # H. 防止“大池里总能碰到一个”：结构与方向分别使用同 K、同预算整流水线 null。
    # 顶层 claim 字段只表示 null + checker 后可发布的状态；门前结果单独留作审计。
    def downgrade_structural_release_to_unknown(bundle, reason):
        if bundle.pre_null_claim_audit is None:
            bundle.pre_null_claim_audit = {
                "mapping_status": bundle.mapping_status,
                "direction_class": bundle.direction_class,
                "mixed_partition_id": bundle.mixed_partition_id,
                "mixed_block_signs": bundle.mixed_block_signs,
                "direction_hypothesis_key": bundle.direction_hypothesis_key,
                "relation_band": bundle.relation_band,
                "release_blocked_by": [],
            }
        bundle.pre_null_claim_audit["release_blocked_by"].append(reason)
        bundle.mapping_status = "unknown"
        bundle.direction_class = "unknown"
        bundle.mixed_partition_id = None
        bundle.mixed_block_signs = None
        bundle.direction_hypothesis_key = "unknown"
        bundle.logical_relation = "unknown"
        bundle.relation_band = "unknown"
        bundle.transported_candidate_response_hypothesis = direction_unknown(
            reason=reason,
            preserve_structural_candidate=False,
        )
        bundle.status = "shadow_unknown"
        bundle.risk_band = "uncalibrated"
        bundle.reason_codes.append(reason)
        bundle.context_injection_allowed = False

    def downgrade_direction_release_to_unknown(bundle, reason):
        if bundle.pre_null_claim_audit is None:
            bundle.pre_null_claim_audit = {
                "mapping_status": bundle.mapping_status,
                "direction_class": bundle.direction_class,
                "mixed_partition_id": bundle.mixed_partition_id,
                "mixed_block_signs": bundle.mixed_block_signs,
                "direction_hypothesis_key": bundle.direction_hypothesis_key,
                "relation_band": bundle.relation_band,
                "release_blocked_by": [],
            }
        bundle.pre_null_claim_audit["release_blocked_by"].append(reason)
        bundle.direction_class = "unknown"
        bundle.mixed_partition_id = None
        bundle.mixed_block_signs = None
        bundle.direction_hypothesis_key = "unknown"
        bundle.transported_candidate_response_hypothesis = direction_unknown(
            reason=reason,
            preserve_structural_candidate=True,
        )
        bundle.reason_codes.append(reason)
        bundle.context_injection_allowed = False

    candidate_level_control_certificate = None
    if (
        frozen_protocol.subset_pivotality_certificate.is_valid
        and frozen_protocol.subset_pivotality_certificate.covers_candidate_claim_family
    ):
        candidate_level_control_certificate = {
            "type": "subset_pivotality",
            "payload": frozen_protocol.subset_pivotality_certificate,
        }
    elif (
        frozen_protocol.conditional_candidate_resampling_protocol.is_valid
        and frozen_protocol.conditional_candidate_resampling_protocol
        .covers_candidate_claim_family
    ):
        candidate_level_control_certificate = {
            "type": "conditional_candidate_resampling",
            "payload": frozen_protocol.conditional_candidate_resampling_protocol,
        }
    strong_candidate_control = candidate_level_control_certificate is not None
    claim_null_gates = evaluate_precommitted_claim_level_look_elsewhere_gates(
        claim_levels=("structural", "direction"),
        structural_candidates=accepted,
        direction_candidates=[
            b for b in accepted
            if b.target_response_observed
            and b.direction_class in {"aligned", "opposed", "mixed"}
        ],
        audits=audits,
        null_reference=frozen_protocol.full_pipeline_null_reference,
        actual_pool_size=len(candidates),
        actual_search_budget=frozen_protocol.transport_solver_budget,
        structural_score="S_struct",
        direction_score="S_dir",
        direction_is_nested_and_eligible_only=True,
        multiplicity_rule=frozen_protocol.candidate_claim_multiplicity_rule,
        prohibit_reusing_structural_p_for_direction=True,
        prohibit_single_pair_null=True,
        prohibit_optional_stopping=True,
        null_control_mode=(
            "strong_candidate_level" if strong_candidate_control
            else "complete_null_pool_only"
        ),
        candidate_level_control_certificate=candidate_level_control_certificate,
        require_candidate_level_maxT=strong_candidate_control,
        prohibit_pool_only_gate_from_localizing_any_candidate=True,
        alpha_by_claim_level=frozen_protocol.alpha_by_claim_level,
    )
    if not claim_null_gates.every_applicable_reference_is_valid:
        for bundle in accepted:
            downgrade_structural_release_to_unknown(
                bundle, "matched_null_reference_invalid"
            )
        return shadow_unknown_with_complete_audit(
            audits, reason="matched_null_reference_invalid"
        )
    null_survivors = []
    null_audited_bundles = []
    for bundle in accepted:
        structural_gate = claim_null_gates.structural.certificate_for(bundle)
        direction_gate = claim_null_gates.direction.certificate_or_not_applicable(bundle)
        bundle.full_pipeline_null_gate = {
            "multiplicity_rule": claim_null_gates.multiplicity_rule,
            "structural": structural_gate,
            "direction": direction_gate,
        }
        null_audited_bundles.append(bundle)
        if structural_gate.control_mode == "complete_null_pool_only":
            downgrade_structural_release_to_unknown(
                bundle,
                (
                    "pool_signal_not_candidate_localized"
                    if structural_gate.pool_complete_null_passed
                    else "complete_null_pool_gate_failed"
                ),
            )
            continue
        if not structural_gate.passed_under_declared_mode:
            downgrade_structural_release_to_unknown(
                bundle, structural_gate.mode_specific_failure_reason
            )
            continue
        if (
            direction_gate.eligible
            and direction_gate.control_mode == "complete_null_pool_only"
        ):
            bundle.direction_class = "unknown"
            bundle.mixed_partition_id = None
            bundle.mixed_block_signs = None
            bundle.direction_hypothesis_key = "unknown"
            bundle.transported_candidate_response_hypothesis = direction_unknown(
                reason="direction_pool_signal_not_candidate_localized",
                preserve_structural_candidate=True,
            )
            bundle.reason_codes.append("direction_pool_signal_not_candidate_localized")
        elif direction_gate.eligible and not direction_gate.passed_under_declared_mode:
            bundle.direction_class = "unknown"
            bundle.mixed_partition_id = None
            bundle.mixed_block_signs = None
            bundle.direction_hypothesis_key = "unknown"
            bundle.transported_candidate_response_hypothesis = direction_unknown(
                reason=direction_gate.mode_specific_failure_reason,
                preserve_structural_candidate=True,
            )
            bundle.reason_codes.append(direction_gate.mode_specific_failure_reason)
        null_survivors.append(bundle)

    # I. 独立、确定性的 checker 重放冻结 artifacts；生成器不能自签名。
    checker_passed_bundle_ids = set()
    for bundle in null_audited_bundles:
        checker_result = wit_check.verify(
            bundle=bundle,
            frozen_protocol=frozen_protocol,
            require_hash_and_commitment_order=True,
            require_operation_specific_witnesses=True,
            require_all_codebook_completion_memberships=True,
            require_maximum_epsilon_envelope=True,
            require_claim_lattice_first_legal_node=True,
            require_typed_empty_and_null_certificates=True,
            require_response_fit_direction_flip_equivariance=True,
            require_fit_holdout_direction_intersection_per_member=True,
            require_mixed_partition_identity_in_direction_consensus=True,
            validate_external_challenge_schema_without_requiring_shadow_reveal=True,
            require_claim_level_null_separation=True,
            require_pool_only_never_localizes_candidate=True,
            require_class_expansion_non_strengthening=True,
            fail_closed_on_missing_evidence_or_solver_gap=True,
            prohibit_llm_calls=True,
        )
        bundle.certificate_checker = checker_result.certificate
        if checker_result.passed:
            checker_passed_bundle_ids.add(bundle.candidate_id)
        else:
            downgrade_structural_release_to_unknown(
                bundle, "independent_certificate_check_failed"
            )
    accepted = [
        bundle for bundle in null_survivors
        if bundle.candidate_id in checker_passed_bundle_ids
    ]
    if not accepted:
        return wit_audit_none_with_complete_audit(audits)

    # null 与独立 checker 证书回写后，只形成返回上游 SOS 的审计 artifact；
    # WIT 自己无权设置产品状态或注入上下文。
    final_accepted = []
    for bundle in accepted:
        if bundle.direction_class in {"aligned", "opposed", "mixed"}:
            challenge = bundle.heldout.external_challenge
            if not (
                challenge.split_commitment_hash
                and challenge.revealed_once
                and challenge.all_required_results_passed is True
                and challenge.covers_every_direction_or_mixed_block(bundle)
            ):
                downgrade_direction_release_to_unknown(
                    bundle, "external_challenge_not_passed_for_signed_audit"
                )
        bundle.risk_band = (
            calibrator.predict_selective_risk(bundle.features)
            if calibrator is not None else "uncalibrated"
        )
        bundle.status = "shadow_hypothesis"
        bundle.wit_audit_status = "structurally_supported"
        bundle.context_injection_allowed = False
        bundle.parent_sos_may_consider = True
        if bundle.status != "none":
            final_accepted.append(bundle)
    accepted = final_accepted
    if not accepted:
        return wit_audit_none_with_complete_audit(audits)

    # I. 只在全部硬门通过后做不可互偿的词典序／Pareto 排序。
    return select_pareto_optional_candidates(
        accepted,
        order=(
            "heldout_advantage_lower_bound",
            "need_value_lower_bound",
            "negative_mechanism_distance_upper_bound",
            "capped_surface_distance",
            "negative_cost",
        ),
        maximum=frozen_protocol.maximum_output,
        allow_empty=True,
        include_complete_unknown_audit=audits,
    )
~~~

外部 `external_challenge`、2×2 结构归因和自然后续见证只由锁定评估脚本运行，不得进入在线函数或反过来参与调参。

### 12.2 Spark-CUT 组件级细化

以下是 CUT 子层的唯一规范接口：它只接收 WIT 已提交并通过 response-fit 的程序—完成—codebook 并集版本空间，只返回方向审计，不召回、不新建映射、不排序候选、不设置产品状态，也不注入上下文。它不得绕过 12.1 的 CEF、completion／codebook coverage、heldout、rival-program 与整池 null 门。

~~~python
def spark_cut_direction_audit(
    *,
    fitted_version_space,
    robust_mapping_certificate,
    query_commitment,
    cef,
    internal_audit_heldout,
    frozen_protocol,
):
    # CUT 只能审计 WIT 已提交、已完成 response-fit 的 (T, omega, codebook) 成员。
    assert fitted_version_space.was_committed_before_response_reveal
    assert (
        fitted_version_space.codebook_completion_ids
        == fitted_version_space.original_codebook_completion_ids
    )
    assert query_commitment.matches(fitted_version_space.query_commitment_hash)
    assert internal_audit_heldout.was_frozen_before_mapping_fit
    assert not internal_audit_heldout.was_used_for_fit_or_layer_selection
    assert robust_mapping_certificate.version_space_hash == fitted_version_space.content_hash
    assert robust_mapping_certificate.heldout_hash == internal_audit_heldout.content_hash

    if not robust_mapping_certificate.release_gate_passed:
        return direction_unknown(
            reason="structural_claim_not_released",
            product_candidate_allowed=False,
        )

    # 当前未决事件没有独立响应证据时，只能报告候选搬运方向假设。
    if not query_commitment.target_response_has_independent_evidence:
        return direction_unknown(
            reason="unknown_target_response",
            transported_candidate_response_hypothesis=(
                summarize_candidate_response_in_query_coordinates(
                    fitted_version_space=fitted_version_space,
                    cef=cef,
                    non_authoritative=True,
                )
            ),
            preserve_structural_candidate=True,
        )

    cells = evaluate_signed_cells_for_every_version_member(
        fitted_version_space=fitted_version_space,
        fit_consistent_direction_hypotheses_per_member=(
            fitted_version_space.fit_consistent_direction_hypotheses_per_member
        ),
        sealed_query_response_handle=query_commitment.sealed_response_handle,
        cef=cef,
        probes=internal_audit_heldout,
        frozen_axis_orientations=fitted_version_space.axis_orientations,
        frozen_mixed_hypothesis_keys=frozen_protocol.allowed_mixed_hypothesis_keys,
        require_source_spans=True,
        allow_unknown=True,
        prohibit_program_elimination=True,
        prohibit_reorientation=True,
    )

    # 相同 T/omega 可共享 cell 计算，但必须按 (T, omega, codebook) 保留 provenance。
    per_member_holdout_hypotheses = aggregate_signed_cells_by_version_member(
        cells=cells,
        member_key=("transport_id", "completion_id", "codebook_id"),
        thresholds=frozen_protocol.signed_direction_thresholds,
        mixed_hypothesis_keys=frozen_protocol.allowed_mixed_hypothesis_keys,
        require_min_axis_coverage=True,
        require_mapping_supported=True,
        treat_required_axis_mismatch_as_unknown=True,
        prohibit_posthoc_mixed_partition=True,
    )
    per_member_states = intersect_fit_and_holdout_direction_hypotheses(
        fit_hypotheses=(
            fitted_version_space.fit_consistent_direction_hypotheses_per_member
        ),
        holdout_hypotheses=per_member_holdout_hypotheses,
        require_every_committed_member=True,
        prohibit_selecting_preferred_direction=True,
        preserve_member_provenance=True,
    )
    if any(
        state.release_hypothesis_set == {"H0_dir"}
        for state in per_member_states
    ):
        return direction_unknown(
            reason="no_stable_direction",
            per_member_states=per_member_states,
            preserve_structural_candidate=True,
        )
    if any(state.release_hypothesis_set_is_empty for state in per_member_states):
        return direction_unknown(
            reason="response_fit_holdout_direction_conflict",
            per_member_states=per_member_states,
            preserve_structural_candidate=True,
        )
    if any(not state.signed_release_gates_pass for state in per_member_states):
        return direction_unknown(
            reason="signed_cell_coverage_or_consistency_failed",
            per_member_states=per_member_states,
            preserve_structural_candidate=True,
        )

    direction_rival = search_direction_rival_inside_committed_version_space(
        fitted_version_space=fitted_version_space,
        heldout_predictions=per_member_states,
        compare_full_hypothesis_key=(
            "direction_class", "mixed_partition_id", "mixed_block_signs"
        ),
        include_unknown_as_signed_release_rival=True,
        solver_budget=frozen_protocol.rival_search_budget,
    )
    if not direction_rival.search_gap_closed:
        return direction_unknown(
            reason="direction_rival_search_unclosed_gap",
            preserve_structural_candidate=True,
        )

    if any(len(state.release_hypothesis_set) != 1 for state in per_member_states):
        return direction_unknown(
            reason="direction_nonidentifiable_within_member",
            per_member_states=per_member_states,
            preserve_structural_candidate=True,
        )
    hypothesis_key_set = {
        state.only_release_hypothesis.canonical_key
        for state in per_member_states
    }
    if (
        direction_rival.exists
        or len(hypothesis_key_set) != 1
        or any(
            key.direction_class not in {"aligned", "opposed", "mixed"}
            for key in hypothesis_key_set
        )
    ):
        return direction_unknown(
            reason="direction_nonidentifiable",
            hypothesis_key_set=hypothesis_key_set,
            rival_programs=direction_rival.programs,
            preserve_structural_candidate=True,
        )

    direction_key = hypothesis_key_set.pop()
    return signed_direction_audit_only(
        direction_class=direction_key.direction_class,
        mixed_partition_id=direction_key.mixed_partition_id,
        mixed_block_signs=direction_key.mixed_block_signs,
        direction_hypothesis_key=direction_key,
        cells=cells,
        per_member_states=per_member_states,
        direction_rival=direction_rival,
        rival_certificate=direction_rival.certificate,
        may_rank_candidates=False,
        may_set_product_status=False,
        may_inject_context=False,
    )
~~~

## 13. 复杂度与成本控制

### 13.1 复杂度直觉

设记忆总量为 N，进入完整验证的候选数为 K，查询必要结构节点数为 q，候选 CEF 节点数为 n，近最优程序显式分支或符号分支数为 B，实际探针数为 P，扰动与审计数为 b，模型/seed 重复数为 S：

- ANN 或其他索引的复杂度依实现、索引参数和数据分布而定，经验上可亚线性，最坏可能退化为 O(N)；
- 首次为所有记忆建立查询无关 CEF 需要 O(N) 次量级的抽取工作，并受文本长度支配；
- 一般类型化部分图映射／编辑路径搜索是组合问题，粗略直接上界可达 \(O(Kn^q)\)，不能承诺多项式实时；若查询模式图 treewidth 为 w，专门约束动态规划才可能接近 \(O(Kn^{w+1}\operatorname{poly}(q))\)；
- version space 的目的不是全部自然语言解释枚举，而是在有限 DSL 内证明“成本阈值内是否存在另一类别程序”；超时未关闭 gap 必须 unknown；
- 忽略批处理共享时，完整验证的模型调用量近似为：

\[
O\!\left(S\cdot K\cdot(BP+b)\right)
\]

- 映射搜索可能组合爆炸，必须冻结 q、n、DSL 深度、\(\epsilon\)、候选数、求解 gap、分支上限和超时；分支上限只控制资源，触顶而未用符号证书覆盖剩余预测等价类时必须返回 unknown；
- 文本 token 长度、缓存命中、重试和比较器调用也必须计入真实成本；
- 系统不对所有记忆两两建图，但这不自动意味着在线成本可接受；
- 必须先用便宜证据筛选缩小 K，再为少量候选建立完整响应矩阵和反例审计。

### 13.2 可缓存内容

可以缓存：

- 候选记忆的查询无关 CEF、证据 span 和 source hash；
- 多层抽象视图；
- 通用状态变化表示；
- 原文证据指针；
- 抽取模型版本、schema、transport grammar、codebook 和内容哈希。

不应缓存为事实：

- 查询特定的反事实结论；
- 未经证据支持的因果边；
- 本轮角色映射；
- 本轮 candidate 或 legacy audit-only speculative 判断；
- 模型应采取的态度。

缓存的 CEF 只能作为可丢弃派生索引：原文、证据 schema 或抽取协议任一 hash 变化就失效；缓存不能成为另一份记忆真源。查询特定 version space 和 Witness Bundle 只按最短审计生命周期保存，并服从原文访问权限。

### 13.3 在线节流

可以采用：

- 只有 inspiration=true 才运行；
- 候选池先便宜后昂贵；
- 只有冻结规则已经证明不存在阈值内竞争程序时才早停；best-so-far 很清晰不构成早停理由；
- 仅对歧义候选运行主动探针；
- 批量处理同一探针下的候选；
- 达到预算仍不确定时返回空；
- 在实验阶段完整记录 token、延迟和模型调用次数。

离线开发开始时可以暂不固定 K、P 或阈值，以便探索。开发结束后、读取独立校准标签前，必须先冻结 K、q、n、P、\(\epsilon\)、DSL、codebook、求解 gap、抽象层与扰动预算、特征定义、成本上限、重试、早停和失败处理规则；校准阶段只拟合预先声明的风险映射和产品阈值。若主动选 probe 或可选停止参与统计证据，必须满足相应 predictable／null 条件；否则使用固定 heldout 数量并只称预测码增益。随后在打开锁定确认集前，再冻结校准器、覆盖率和全部阈值，之后不得根据确认结果调整。

## 14. 数据与缓存安全

### 14.1 真实记忆保护

所有开发与回归必须使用：

- 独立测试 vault；
- 临时目录；
- 专用 Docker 卷；
- 创建时明确标注为 test_data 的测试桶。

禁止：

- 对真实 buckets 写入派生测试数据；
- 为复现实验清空真实记忆；
- 将合成反事实写回真实条目；
- 用真实记忆内容制作可公开的数据集；
- 将私有记忆传给未经授权的外部服务。

### 14.2 Prompt injection

候选记忆中的文本只能作为数据，不得作为指令执行。

建议：

- 明确分隔系统指令、探针和候选原文；
- 结构化输出采用严格 schema；
- 忽略记忆正文中要求修改流程、泄露数据或跳过验证的指令；
- 记录并测试记忆正文包含提示注入的情况；
- 输出只能引用证据位置，不执行证据中的命令。

### 14.3 派生缓存的生命周期

派生缓存应：

- 与原文哈希绑定；
- 与抽取器模型及提示版本绑定；
- 原文变化时失效；
- 可整体删除并重建；
- 不参与真实记忆的删除、归档或恢复逻辑；
- 不进入公开备份，除非隐私边界已明确。

是否建立独立缓存表、sidecar SQLite 或文件索引，属于后续架构决策，本文不提前创建正式模块。

### 14.4 来源结果防火墙的数据安全边界

结果盲封不能靠修改或删除真实记忆实现。允许的做法仅是从经授权只读来源构建可重建 sidecar，并在 sidecar 内把 `pre_outcome_payload` 与 `sealed_outcome_payload` 分开授权。原文始终是唯一真源，盲封边界、结果标签和机制预测都只是可丢弃派生数据。

- 结果持有服务不得向检索／映射进程暴露原文、摘要、向量或访问时序代理；
- commitment 与审计日志只保存必要 hash、ID 和状态，不复制可识别私人原文；
- 研究人员、模型供应商和外部 API 的访问权限必须逐 vault 明确授权；
- 未经许可的私人记忆不得用于公开 benchmark、论文附录、错误案例或模型训练；
- 数据主体撤回授权时，删除可重建 sidecar 与实验副本，不改变真实记忆的保留／遗忘语义；
- “盲封”是实验权限边界，不是加密强度或匿名化承诺；如需安全声明，必须另做威胁模型和数据保护审计。

### 14.5 目标 shadow 的授权、最小化与非监控边界

`TargetObservationContract` 可能把后续自然记录与此前预测关联，因此真实 future shadow 必须单独 opt-in，不能由一般 `inspiration=true` 或部署开关默示授权。只允许记录预冻结、任务局部、最小必要的可观察 endpoint、时间窗、censoring 和 hash；禁止推断人格、情绪、意图、可信度、服从度或建立长期行为画像。

- shadow 预测与候选正文在观察窗内不得进入模型上下文、Dashboard 可见区域、用户提示、共激活边或真源；
- 不为补标签主动追问、诱导行动或改变现实条件；
- 只读取正常产品流程中自然出现且已授权的后续记录；
- 研究 sidecar 设独立 retention、访问控制与删除策略；撤回授权时删除 sidecar，不改真实记忆；
- 报告按聚合机制族／时间块进行，非必要不输出个人级“可靠性分数”；
- 如果无法证明 `never_shown`、授权链或最小化，禁用 future shadow，只保留离线 rolling-origin。

## 15. 实验设计与确认协议

### 15.0D DSR-CT Benchmark、唯一确认主效应与复制协议

本节是新版规范；后续 15.0S 保留旧 SOS-PAR 基线的标注与泄漏检查，但其固定 top-8、空槽最大损失和 \(\Delta_{selection}\) 不再支配 DSR-CT。

#### 15.0D.1 三层不同问题，禁止用一个数字代替

DSR-CT 必须分别回答：

1. **发现／召回**：结果盲条件下，真机制 seed 与独立验证事件是否进入可接受预算的候选池；
2. **自动筛选**：在允许 abstain 的条件下，发布候选中有多少是人工锁定定义的 useful-far，而非 surface foil／null；
3. **目标价值**：被选材料对目标回答是否比无记忆和等长干扰更有帮助。

来源结果预测好不等于目标有帮助，目标回答变好也不证明自动选中了真实类比。三层必须有各自分母、盲审和停止线。

#### 15.0D.2 数据原子与独立切分

主终点与 DSR 内部机制审计必须使用两种不同数据原子，不能让 DSR 自己的产物定义所有方法的 gold：

~~~text
CommonSelectionGoldUnit                    # 所有方法共同的主终点原子
- query_id / owner_id / query_time
- canonical_candidate_id
- raw candidate_memory_ids / candidate_event_cluster_id / source hashes
- query text + candidate pre-outcome source spans + ACL/provenance
- independent labels: appropriate / nonredundant / surface_far
- independent span-grounded structural_relevance: yes|no|unknown
- adjudication codebook / reviewer IDs / disagreement

DSRMechanismAuditUnit                      # 仅作 DSR 机制诊断，不定义共同 gold
- query_id / frozen Need Frame
- discovery_seed_cluster / MechanismCard / rivals
- natural validation clusters + hidden gold cells
- outcome refs / cutoff / axes / windows / censoring
- source-side prequential receipts
- historical target rolling-origin receipt, if scoreable

SparkGoldRelationUnit                      # CMI／结构筛选开发门；不进入在线 selector
- query_id / canonical_candidate_id / event_cluster_id
- target_mechanism_chain / candidate_mechanism_chain
- role_bijection
- constrained_relations[]                  # 至少两条相互约束、非通用关系
- causal_direction / temporal_order / outcome_polarity
- exact_target_spans[] / exact_candidate_spans[]
- minimal_falsifier
- generic_template_rejection_reason
- status: strict_positive | boundary | hard_negative | unknown
- reviewer_ids / independent labels / adjudication
~~~

`canonical_candidate_id = hash(owner_id, canonical_event_cluster_id, canonical_pre_outcome_span_set)`；raw bucket／entry ID 只作 provenance，不参与 gold 身份。所有方法的输出先跨方法映射到该 ID，同一事件的切片、跨桶副本或 supersedes 链因此只能共享一个 gold。`CommonSelectionGoldUnit` 的审核包由 benchmark 统一从原始、结果前文本构建；审核者能看到对所有方法相同、揭封前冻结的 shared query／Need view、同预算候选原文 spans 与 provenance，却看不到方法名、rank、score、pass、certificate、MechanismCard 或验证 panel。审核者按共同 codebook 自己指出支持 structural relevance 的原文 spans；`unknown` 不得自动转成正例。主终点要求每个方法在每个 query 只提交一个冻结的 primary `canonical_candidate_id`；原生输出是列表／图／合成卡的方法，必须由 confirmation 前冻结的 neutral adapter 选出该 ID。集合质量另作 set-level 指标；方法生成解释则以 `(CommonSelectionGoldUnit, artifact_hash)` 建立次要 artifact endpoint；最终 renderer 帮助度再单列，三者都不能回填主终点。这样 BM25、dense、CMI、结构映射与 DSR 都在同一种 `(query_id, canonical_candidate_id)` 上比较，不会因缺少 DSR 专属证书而被先验判负。

`SparkGoldRelationUnit` 只在方法盲、结果盲的人工研究标注中使用。`strict_positive` 必须在同一角色映射下同时保持至少两条相互约束的非通用关系、因果方向、时间顺序与结果极性，并能用逐字来源 span 支撑；需要补写原文未陈述事件时不得通过。“目标—障碍—解决”“努力—成功”“表象—真相”“先投入—后受益”等可套用于大量故事的框架不能单独成为正例。`boundary/unknown` 不得为了达到样本量门槛转成正例，Spark-Gold 也绝不能成为在线 selector 的输入、特征或提示内容。

按 `owner_id × event_cluster_id × time block` 做外层切分：

- `discovery`：生成候选机制和探索阈值；
- `calibration`：冻结排序器、coverage、risk threshold 与 renderer；
- `confirmation`：只运行一次，不允许改 prompt、模型、DSL、检索器或阈值；
- `future shadow`：确认后另行批准的前瞻数据，不与 confirmation 混用。

一个事件的多个副本、同一次对话切片、同桶 supersedes 链和语义近重复必须落在同一 split。同一个用户的分布漂移以 rolling-origin 报告，不把未来信息随机分回训练集。

#### 15.0D.3 Stage 0D 的文本信息量与全库召回可行性

现有 60 query 只用于以下只读 feasibility，不用于确认世界首创或产品精度：

- 至少 20／60 query 能形成 `minimal_three_cell_panel`：`analogue+bridge>=2` 且包含 bridge，`foil/null>=1`，总计至少 3 个事件簇；用于评估响应签名密度，不作产品确认；
- 至少 6／60 query 能形成 `full_four_cell_panel`：analogue、bridge、foil、null 各至少 1 个、总计至少 4 个独立 validation clusters；即 full-panel 覆盖率至少 10%，低于该值立即停止 DSR-CT full；
- source span provenance 完整率至少 95%，ACL／canary／outcome permutation 违规为 0；
- 独立人工对角色、轴、cutoff、cell assignment 的一致率至少 0.75；
- 在冻结全尺寸 vault 快照或等价规模模拟上，结果盲 `Recall@64` 点估计至少 0.80、cluster bootstrap 95% 下界至少 0.65；
- 随机抽 15–20 条真实记忆，若多数轴只能是 unknown，先停止上层形式化并修复证据密度。

人工 relevance pooling 必须合并多检索器 top-N 与随机长尾样本；未判定候选不能自动记作负例。只在 32–64 条人工候选池上的 `Recall@8` 不计为全库召回证据。

#### 15.0D.3A 2026-08-03 AnaloBench 关系级开发门实测

在任何新回答生成之前，已对冻结的 AnaloBench 60 对 Current／official-candidate 文本执行一次关系级开发诊断。输入副本删除了 `motif`、`ground_truth`、答案标签、旧评分和条件名称；两名相互隔离的模型盲审者只按上述严格标准判断 `pass / boundary / fail`，`pass+boundary` 再交给第三名新盲审者保守裁决。该过程是**模型盲审开发诊断，不是人类 gold 或确认实验**。

- 初筛 A：`pass=16`、`boundary=14`、`fail=30`；
- 初筛 B：`pass=20`、`boundary=13`、`fail=27`；
- 两者一致严格通过 12 对；8 个 `pass+boundary` 案例的第三方裁决为 `pass=0 / boundary=2 / fail=6`；
- 一致通过的 P033 与 P057 使用完全相同的候选故事，按事件簇独立性折叠后只剩 11 个独立严格 pair；
- 预先冻结的 CMI 最低门为 12 个独立严格 pair，因此触发 `STOP_BEFORE_HARD_NEGATIVE_AND_CMI`。

这次停止只说明“当前 60 例不足以按该冻结标准支持 12–20 例的小样本 CMI”，不能解释为 Spark、CMI 或自动筛选已经失败。官方 story-level oracle 不能直接继承为 relation-level gold。为防止为了凑数而漂移标准，本轮没有制作硬负例、没有调用生成 API、没有运行 N/T/H、没有计算 p 值，也没有实现比较器。完整冻结协议、盲筛、裁决与停止记录保存在桌面目录 `Spark-Gold_v0_CMI_方向性验证_2026-08-03`。

#### 15.0D.3B 2026-08-03 前瞻补批与 Hard-Negative 第二停止门实测

项目所有者随后单独批准了前瞻补批和 Hard-Negative 开发门，但没有批准比较器或产品实现。补批只使用 [AnaloBench 官方 Hugging Face 数据](https://huggingface.co/datasets/jhu-clsp/AnaloBench) 的 `AnaloBench-T1-Subset-S10.csv`；固定源文件为 340 行、SHA-256 `0763fad528f2c9b961f585095ae78986fd76d97c5d55bd764723356746025a9e`。在读取新 pair 质量之前先冻结抽样、全角色节点去重、5-token containment、B1/B2 停批和双 `pass` 规则。

复核旧 11 对时发现两处 target/candidate 跨角色故事节点复用；按字典序保留规则移出 P030、P053 后，旧主分析基线从 11 修正为 9。B1 固定 48 对，两名新模型盲审者分别得到 `pass=16/boundary=7/fail=25` 与 `pass=16/boundary=10/fail=22`；严格 `pass∩pass` 为 14 对。第三名只允许降级的盲审者保留 9、剔除 5，故 `K1=9`，按冻结规则不打开 B2。新增 9 对与旧主 9 对在 target/candidate 全角色故事节点上互不复用，开发期正例 roster 达到 18。它们仍是 `model_screened_candidate`、`human_gold=false`、`confirmation_eligible=false`，不是人工 gold。

随后对 18 对按 pair ID 字典序一次性循环分配 `role_mapping / causal_direction / temporal_order / outcome_polarity`，失败后不重排、不换轴、不从 reserve 补例。对同一官方行的全部 54 个非 T option 执行 natural-first 六项门，没有一个同时满足自然完整、节点独立、长度 ±15%、只破坏冻结轴、保留其他三轴与关键关系且不是一眼普通 distractor。两名隔离作者各自只提交一次最终判断：10 对为 `hard_negative_unconstructable`，8 对产生 `synthetic_metamorphic` 候选；因此在盲检前理论上限已是 `8<12`，CMI 数量门不可能通过。

为直接检查“合成负例是否太假”，仍把这 8 对以两套独立、4/4 平衡的 X/Y 位置交给两名看不到 target、轴、作者和 T/H 身份的新模型会话做自然度盲检。机械解码结果为 `pass=0 / fail=8`：8 例均触发高置信语言或编辑痕迹，7 例至少一侧三项中位数低于 4，5 例被两名评审同时以至少 4/5 置信度正确识别 H，1 例 T/H 中位差超过 1。原始 T 也暴露出语法／逻辑质量问题，说明 shortcut 同时来自 AnaloBench 长故事底噪与局部合成修改。由于自然度门没有一例通过，按不可互偿规则不再启动单轴审查；最终有效 H 为 0，触发 `STOP_AFTER_HARD_NEGATIVE_GATE`。

这一结果直接支持“不能用一眼假的角色交换／因果反转证明结构筛选有效”的提醒，但仍**不能**解释为 Spark 的 T 没有下游价值或 CMI 已失败：本轮 DeepSeek API 调用数为 0，没有任何 N/T/H 回答，也没有 p 值或效应方向。失败位置是等质 Hard-Negative 的可构造性与自然度，而不是下游效果。不得润色这 8 个 H 后重测、放宽语言门、回收边界正例或打开 B2 补数。完整源包、作者记录、随机化键、两份盲审与机械停止结果仍保存在桌面实验目录。

#### 15.0D.3C 2026-08-03 HN-F0 独立成文 authored-benchmark Hard-Negative 供给门实测

上一节要求的“全新、结果不可见、先保证 T/H 语言质量的独立成文批次”随后被具体执行为 **HN-F0**。本轮问题严格限于：冻结的公开 authored-benchmark 来源池，能否提供至少 12 条语言同质、T/H 节点各不复用、并有机会在后续双盲评审中满足“只破坏预分配单轴”的 `(Q,T,H)`。它不评价 Spark、比较器、CMI、帮助度或产品效果。

**来源与证据等级。** 主来源是 ARN v1 数据记录；固定 CSV 实测为 1,095 行、1,256,913 bytes、MD5 `38484f48176fd0bfa0b569acb55f1176`、SHA-256 `a866fe5341ce4a29f00f24987a12278303b2b8ad788352f549b0fe051ad4a7a8`，与论文正文所述 1,096 个实例存在 1 行计数差异。本轮只使用 `analogy_level=far` 且 `distractor_similarity=high` 的冻结来源。证据等级必须写成 `authored_benchmark`；本轮可用的 `natural_observed` 主分析样本数为 **0**。这里的 `independently_authored` 只表示故事在看到本轮 Q、T、预分配轴和结果之前已经成文，不表示真实自然事件、人类 gold、独立复制或跨团队确认。ARN 标签只参与候选构造，不能证明本轮四轴关系或单轴 H 成立。

**正例门与污染处置。** 在读取主批质量前冻结 120 个 main、4 个只允许技术故障替换的 reserve、四轴分配和失败后不换轴／不补例规则。两名隔离模型评审分别得到 `pass/boundary/fail=51/30/39` 与 `66/18/36`，严格 `pass∩pass=43`。第一版第三审包错误泄漏了 `pass-pass`、`down-only`、`keep/downgrade/no-upgrade`，会产生确认锚定；该第三审及其派生 positive gate、eligible、旧 slate 和运行期容量文件已在语言门前永久标记 `INVALIDATED_BEFORE_LANGUAGE_GATE`，其通过数不得用于产率、稳定性或新旧敏感度比较。随后对同一冻结 43 项使用不显示上游结论的中立第三审，得到 `pass/boundary/fail=26/12/5`；有效严格正例为 26，四轴分布为 `role/causal/temporal/outcome=8/8/5/5`。这只是流程纠错，不是独立复制；全部记录仍为 `model_assisted_development=true`、`human_gold=false`、`confirmation_eligible=false`。

**V2 检索供给。** 旧检索实现还因 K/R 可变、协议链未强制和容量基线可覆盖而失效；V2 固定 `K=64`、`R=8`、`RRF_k=60`，把容量基线改为不可覆盖输入并增加外部 root runner、并发唯一发布和失败路径测试。冻结 authored H 池含 674 个唯一节点；124/124 个冻结 T 的机械容量均不少于 8；最终 26 个 slate 承诺 208 条边、172 个唯一 H，机械自测 `35/35` 通过。以上只证明机械供给、来源闭合和可复现性，不证明语言同质、事件独立或单轴纯度。

**singleton 语言门。** 在看分数前冻结 198 个唯一文本（26 个 T、172 个 H）、两套 blind ID 与独立顺序、两名隔离评审和不可补偿阈值：每个 singleton 的三项语言分在两位评审处共六分均须 `>=4`，两位 `grammar_or_edit_artifact` 均须为 `no`；边级还要求 T/H 的三项双评审中位数绝对差均 `<=1`。`test_constructed_suspicion` 只作 shortcut 诊断，不参与通过判定。实际结果为：

- T singleton：通过 11，失败 15；
- H singleton：通过 71，失败 101；
- 208 条承诺边：语言门通过 45，失败 163；
- 可重叠失败原因命中：`T_singleton_language_fail=120`、`H_singleton_language_fail=120`、`T_H_median_difference_over_1=79`；
- suspicion 诊断：评审 A 为 `yes/uncertain/no=165/22/11`，评审 B 为 `152/24/22`；这些数字不是 gold，也不能单独证明文本“人工合成”。

语言判定器第一次冻结运行因完整性校验把构建器的规范化 `story_hash` 误写成原始字符串 SHA 而 fail-closed；它机械解析和格式校验过评审文件，但没有完成角色解码、聚合或发布任何结果。旧脚本／根锚被保留并标记 `INVALIDATED_BEFORE_SCORE_DECODING`。V2.1 只把四处校验统一为构建器相同的 `NFKC → 连续空白折叠 → strip → lower → SHA-256`，评审结果哈希、语言阈值和不可补偿规则均未改变，396 个 pack 映射、198 个 registry 文本和 26 个上下文预检查均为 0 不一致。另一个上界证明 V2.1 因最终文件非原子写入在执行前失效；V2.2 改为同目录临时文件、`flush+fsync`、发布前复核和原子改名后才运行。这两次失效均保留原件、根锚、失效记录与后继链，不能被静默覆盖。

**零调用上界停止。** 语言白名单的 45 条边只覆盖 11 个唯一 source/T 和 42 个唯一 H；语言合格 source 的轴分布为 `causal/outcome/role/temporal=3/2/3/3`。任何 T 唯一、H 唯一的匹配都满足 `|matching| <= min(|T|,|H|)=11`，已经严格小于冻结总门槛 12；因此四轴配额是否还能满足不再影响总量不可能性。纯机械、只读白名单的 V2.2 证明触发：

`STOP_HNF0_LANGUAGE_ELIGIBLE_T_UPPER_BOUND_LT_12`

按事前的不可回收规则，本轮不得用单轴评审救回语言失败边，不追加第 9 个 H，不换轴、不替换 T、不放宽阈值。单轴语义评审调用数为 **0**，单轴分数从未出现；N/T/H 下游生成数为 **0**；CMI 调用数为 **0**，`cmi_authorized=false`。因此没有 p 值、效应方向、帮助度、比较器准确率、Spark 代码或产品验证。这个停止只说明“当前冻结 ARN authored-benchmark 开发批次无法越过 HN-F0 供给门”，不能推出 Spark／CMI 有效或无效，也不能外推到真实 OB 记忆。完整协议、冻结输入、盲包、原始模型评审、失效链、语言结果和上界证明保存在桌面目录 `Spark-HN-F0_独立成文硬负例供给可行性_2026-08-03`。

#### 15.0D.3D 2026-08-03 HN-F1 自然来源与语言优先供给协议（未执行）

HN-F0 停止后，下一步已被收窄为一个新的、完全独立的数据供给协议，而不是继续回收 ARN 边或直接运行 CMI。自包含协议见 [OB HN-F1 自然来源与语言优先供给协议](./OB_HN-F1_自然来源与语言优先供给协议_2026-08-03.md)。其当前状态是 `DRAFT_FOR_OWNER_REVIEW`、`execution_authorized=false`；本轮没有接触真实 vault、没有收集私密材料、没有调用模型、没有单轴评审、没有 CMI，也没有产品代码。

HN-F1-N 只把招募前已存在、可追溯、获许可且去标识后仍保留 X/A/Y 证据的 N3/N2 文本称为 `natural_observed`；OB 自然运行产生的可追溯系统派生文本另属 HN-F1-OB，公开材料、独立人工写作与任务合成材料也全部分 cohort，不得合并跨过门槛。真实 OB 只允许所有者主动选择的只读研究副本，评审标签、轴和边均不得写回记忆真源。

协议采用永久隔离的 P0 Pilot `24 Q–T / 96 H` 与固定 M1 Main `96 Q–T / 384 H`；Rtech 仅含评分前一对一技术替补 `8 Q–T / 32 H`，不能因语言、结构或匹配结果不理想启封。顺序固定为：来源与同意闭包 → singleton 语言门 → 纯机械 `G_surface` 语言兼容上界 → 严格 Q–T 正例门 → 明示为 `axis-aware oracle_supply` 的既有 H 排序 → single-axis-review 零调用配额上界 → role/axis 双盲单轴门 → 最终唯一配额匹配。任何阶段都不能靠补样、换轴、润色或第三审救回。

正式门要求两个不同模型家族与独立人工轨严格交集，并在单一冻结 provenance/donor cohort 内形成至少 12 个 Q/T/H 节点均唯一、四轴各至少 2 个的有效三元组。即使通过，也只允许另写 CMI 协议，仍保持 `cmi_authorized=false`；其产率不能代表自动 selector 的精度。执行前还必须由项目所有者填写来源全集、同意与外部模型权限、donor 模式、人工分工、proposer、最坏规模预算、机器 schema、solver 和研究副本处置规则。

#### 15.0D.3E 2026-08-03 HN-F1 Stage -1 来源容量补充协议（未执行）

为避免把“先建完整实验系统”置于最便宜的数据可行性检查之前，HN-F1 第 22 节已展开为 [OB HN-F1 Stage -1 执行补充协议](./OB_HN-F1_Stage-1执行补充协议_2026-08-03.md)。它只允许在受限区用假名化来源元数据检查固定容量，不读取正文、向量、相似度、语言分、关系分、assigned-axis 结果或任何模型输出；当前状态为 `DRAFT_FOR_OWNER_COMPLETION`、`execution_authorized=false`，真实来源盘点、模型调用、单轴评审和 CMI 均为 0。

固定账本是 P0 `24 Q–T + 96 H = 144`、M1 `96 Q–T + 384 H = 576`、Rtech `8 Q–T + 32 H = 48`，合计恰好 768 个跨批次不复用的事件节点。768 不是贡献者人数，也不是已经拥有的样本量；总数达到 768 仍不能证明 role、donor、源文档、事件簇、时间块和替补映射可同时满足。按每位 donor 最多进入 `floor(0.20M)` 个最终三元组的规则，P0 与 M1 数学硬下限各为 15 位，Rtech 为 8 位，跨批次总硬下限是 38 位；`18/18/8=44` 只是待批准的保守规划阈值，不是必要条件。S1-A 的假名化 metadata-only exact upper-bound solver 只负责证明设计是否已必然不可能，通过也不能冒充 proposer 后的 actual roster 或 Rtech map。

Stage -1 明确区分来源全集、事前分配规则和 positive proposer 后的 actual roster；前两者冻结不能被写成正式 Q/T/H roster 已经形成。桌面投影只允许批级批准、聚合计数、metadata-only receipt 和状态，不保存逐条来源关联图。2026-08-03 已把 `HN-F1-N / N3 / cross_donor` 记录为可撤回的 `PROVISIONAL_OWNER_DIRECTION`，`natural_observed` 仍是协议固定定义；这不是 owner approval 或数据处理许可。38 位是数学硬下限，44 位是 `PROPOSED_UNAPPROVED` 保守规划阈值。

招募截止、来源窗口、受限目录、角色隔离、去重阈值、同意与外部服务范围、保留／撤回、metadata-only solver 实现和不可覆盖 anchor 位置仍未冻结，当前状态为 `STAGE1_NOT_READY_METADATA_INCOMPLETE`。因此仍不进入 P0，不实现 Spark 产品逻辑，也不授权另写或运行 CMI。

由于当前没有证据确认 38 位硬下限、44 位拟议保守规划阈值与 768 个潜在事件节点现实可得，下一步被前移为 `HN-F1-PAS`（Pre-S1-A Source Availability Screen）无正文来源可得性普查。PAS 只在另行授权后于受限区收集假名化枚举元数据，以及联合唯一节点与事件／文档／时间块诊断边际的预冻结数量档位；一人一 donor 由独立招募台账机械去重，不收正文、不分配 Q/T/H、不调用模型。它以协议有效乐观上界和单 donor 粗容量 cap 判断当前框架 hard no-go，以保守联合下界判断是否达到拟议规划门；38–43 位、招募 frame 不可穷尽、规则未冻结或上下界跨门槛时只能记为 `PAS_INCONCLUSIVE`。

当前文档化工作流状态为 `hnf1_pas_status=PAS_NOT_AUTHORIZED`，执行授权、联系 donor、metadata 记录和正文收集均为 0；现实全局零状态仍需管理员另行签署 attestation。PAS 不能输出 HN-F1 正式停止码、`PASS`、S1-A `READY` 或 Spark 可行；即使未来输出 `PAS_PLAUSIBLE_NOT_VERIFIED`，仍需完整冻结 S1-A 治理与 exact solver 后另行授权，PAS 数字也不能直接成为 S1-A 合格计数。

Stage -1 补充协议第 20 节现已加入 PAS01–PAS14 可审批冻结包、PAS10 有效计数档位草案、PAS12 exact 粗分区合同，以及角色／存储／撤回／小单元抑制边界；当前仓库已有未冻结的 PAS10 候选归一化器与 PAS12 候选数学核，但没有完整 PAS 治理状态机、正式 test receipt、PAS13 隐私投影、PAS14 hash-bound authorization、现实执行附件或任何真实数据运行，正式状态仍未批准。“继续”只授权完善候选实现与文档，不构成 donor 联系、PAS 执行、S1-A、Spark 或 CMI 授权。

#### 15.0D.4 最小机制特异性诊断 2×2（观察性）

核心最小 2×2 是：

| | singleton outcome | natural contrast panel |
|---|---:|---:|
| discovery seed 自身验证 | 旧 SOS-PAR 单结果 | 同源多切点／对照，但仍可能自证 |
| event-cluster 外验证 | 事件外 singleton | **DSR-CT 主臂** |

它回答两个非冗余问题：

- discovery／validation 分离本身是否减少事后拟合；
- natural contrast panel 是否在事件外验证之外继续降低 surface-foil 假通过。

另做两个局部 2×2，不把所有模块无条件叠成一个巨大黑箱：

1. `BlindCEF / BlindSourceLedger+WitnessedTCA` × `无 target conditioning / 有 target conditioning`；
2. `query direct recall / query+Need-Path` × `普通高召回 / event-cluster diversity quota`。

结果可见、全文 embedding 和 seed outcome 可用条件只作泄漏正控；它们表现更好不能算方法收益。

#### 15.0D.5 唯一锁定主效应

确认阶段唯一自动筛选主效应为：

\[
\Delta_{PF}(c^*)=
\operatorname{Precision}^{DSR}_{useful\text{-}far}(c^*)-
\operatorname{Precision}^{base^*}_{useful\text{-}far}(c^*)
\]

其中：

- 确认 run 必须同时生成两个互不回流的回执：`AutomaticSelectionReceipt` 用与产品相同的自动 adjudicator、风险模型、排序和预算，在 human useful-far gold 首次可读前冻结每个方法的 primary rank、score、exact-coverage 选择与 deployment release flag；`GoldDiagnosticReceipt` 随后只产生 \(Y_{qm}\)、cell 指标和解释性审计。主效应中的 \(I^{(c^*)}_{qm}\) **只能**由前者产生，人工 gold 只能产生 \(Y_{qm}\)，不能参与候选生成、排序、阈值、panel pass 或同轮历史 replay。否则该实验测到的是 gold-assisted selector，不是自动筛选；
- \(N_{eligible}\) 由方法无关、候选生成前冻结的 consent、ACL、query 时间窗、语言／字段可用性、去重与 scoreability 外壳定义；不得按“DSR 是否找到 seed／形成 panel”或“某基线是否能输出”删除困难 query。方法特有失败留在共同分母，并按 abstain／invalid／technical failure 单列；
- \(c^*\) 是 calibration 结束前冻结的**确认比较覆盖率**；默认研究值为 10%，不能看 confirmation 标签后改变。令 \(K^*=\lceil c^*N_{eligible}\rceil\)，每个方法必须在不看 gold 的条件下，用其 calibration 冻结的 primary-candidate ranking、candidate cap 与 tie rule 在同一 eligible query frame 上恰好选出 \(K^*\) 个 `(query_id, canonical_candidate_id)` release。少于 \(K^*\) 直接 coverage 不合格，不允许用较低覆盖率进入 precision 对比；`base^*` 及其同预算 neutral adapter 也必须在 confirmation 前按独立 calibration 冻结，不能看正式结果后选“最弱的最强基线”；
- `useful-far` 是对 `CommonSelectionGoldUnit` 的**方法盲 gold**：`appropriate=yes ∧ nonredundant=yes ∧ surface_far=yes ∧ structural_relevance=yes`。它不要求候选被任何方法叫作 analogue／bridge，也不得包含“通过 DSR replication gate”、MechanismCard 质量或其他算法内部条件；所有基线使用同一 adjudication protocol。DSR 的 foil／null cell 是单列机制审计标签，不进入共同 gold 的定义；
- 对方法 \(m\)，令 \(I^{(c^*)}_{qm}=1\) 表示 query \(q\) 属于上述 exact matched-coverage \(K^*\) 发布集合，\(Y_{qm}=1\) 表示该方法揭封前冻结的 primary `canonical_candidate_id` 通过共同 useful-far gold，则

\[
\operatorname{Precision}_m(c^*)=
\frac{\sum_q I^{(c^*)}_{qm}Y_{qm}}{K^*},
\qquad
\operatorname{Coverage}_m=
\frac{\sum_q I_{qm}^{deploy}}{N_{eligible}}.
\]

- 主效应 precision 的分母对两个方法都是同一个 \(K^*\)；另用 calibration 冻结的实际部署 threshold 得到 \(I_{qm}^{deploy}\)，报告 realized coverage、precision–coverage、AURC 和 failure。技术失败、无候选和 abstain 不得静默删除；如果导致方法凑不齐 \(K^*\)，该方法不合格，而不是缩小主效应分母；
- 合理 abstention 不被赋最大损失，但若 coverage 低于 \(c^*\)，该方法主终点直接不合格；
- \(\Delta_{PF}\) 在相同 owner／event／time query blocks 上重算两个方法的 exact-coverage precision 后做配对 cluster bootstrap；方法输出多个候选时，主终点只认 neutral adapter 揭封前选定的 primary ID，候选集合的 set-level gold、上限和汇总另作预注册次要指标；
- 样本量由目标 precision、\(\Delta_{PF}\)、聚类和预期 coverage 的预注册功效／精度模拟决定，不再把“30 个发布单元”写成充分确认数。30 只能作为早期 feasibility floor；例如零失败时要把单侧 95% 二项上界压到 5% 以下也需要约 59 个独立发布单元，若使用双侧区间或存在聚类则需要更多。现有 60 query 不能支持产品精度声明。

DSR 的 matched-null 假发布必须另有自己的 query 级 Bernoulli 单位，不能借用后文只适用于 legacy `localized_release_status` 的 \(Z_q\)。对每个独立 matched-null query pool \(q\)，定义：

\[
Z_q^{DSR}=\mathbf 1\{\text{完整 DSR 流水线在 }q\text{ 上产生至少一个 }release\_eligible\text{ 候选}\}.
\]

这里的 `release_eligible` 是**展示前**的自动发布决定：必须使用自然 query calibration 冻结的同一方法版本、候选预算、primary ranking、`full_four_cell` 自动 adjudicator、所有词典序门、风险模型和 deployment threshold；不得在 null pools 内重新取 top-\(c^*\)、换阈值或删去技术失败。每个 query pool 最多贡献一个 0／1；候选数、机制数和 probes 不能扩充样本量。主假发布门约束 \(\mathbb E[Z_q^{DSR}]\) 的 cluster-aware 单侧 95% 上界，旧 SOS-PAR 的 \(Z_q^{pool}/Z_q^{candidate}/Z_q^{any}\) 只在其 legacy 基线内报告，二者不得混算。

预注册研究目标建议为：

- 唯一主效应 \(\Delta_{PF}\) 的点估计至少 +0.10，单侧 95% 下界大于 0；
- DSR 自身 precision 的单侧 95% 下界至少 0.80，作为不可被主效应补偿的发布安全 floor；
- 如果功效分析显示这些界在可获得样本上不可识别，则先扩大确认集，不降低门槛或把“无结论”改写成通过。

#### 15.0D.6 必报的选择与机制面板

主终点之外必须同时报告：

- full-vault `Recall@16/32/64` 与候选池规模曲线；
- precision–coverage、AURC、AUPRC、Top-1 precision 和空结果率；
- `surface-foil FPR`、`mechanism-bridge recall`、`boundary-breaker FPR`；
- analogue／bridge 上 \(H_C\) 对每个 material rival 的 paired log-loss／Brier regret；
- 结果盲 applicability 的 \(S_{contrast}\)、foil／null 边界签名准确度／校准、outcome-swap false-pass、matched-null false-release；
- panel coverage、自然 cell 缺失率、每 query 独立 cluster 数和多切点相关性处理；
- leave-one-event-cluster-out 与 leave-one-implementation-out winner reversal；
- ACL／canary／proxy／cache／parameter-memory 风险与全部无效轮次；
- 成本、p50／p95 latency、LLM 调用数和 token；
- 目标侧 rolling-origin log score／Brier、校准、失访率和 transport coverage；
- 人工盲评 appropriateness、novelty、usefulness；novelty 只作次级终点。

任何报告都必须给完整流水线分母和 scoreable 子面板两个版本，不能只报成功候选。

#### 15.0D.7 强基线与同预算规则

第一轮必须忠实复现或做最接近的 OB 无标签适配：

1. 结果盲 BM25、dense、RRF 与随机多样性；
2. 直接 LLM analogy judge；
3. MAC/FAC／结构映射、ARN／YARN 风格多层抽象；
4. TCA-SIR 风格 target-conditioned transferable abstraction；
5. PGR 风格多 probe 召回，但严格替换为 informational-need probes；
6. CANA 风格机制分解 + cross-analogy confirmation；
7. MUSE-style functional／problem abstraction retrieval；
8. case-based prediction／CDH；
9. Green–Armstrong structured analogies：多来源类比、相似度评定、来源 outcome→目标 outcome 映射与机械聚合；
10. Creative Analogy Machine-style retrieval→mapping→inference validation；
11. CMI 的忠实 baseline 与 OB 不写回适配；
12. Remember When It Matters 风格的选择性注入／沉默基线，只在 sandbox 评价 abstention 与可见效用，不学习用户行为规范；
13. CICL 风格 decision-aware context selector，但把 action-shift 标签与 useful-far 共同 gold 分开报告；
14. Hindsight／Amory 的 native retrieval 与不写回真源的只读适配；Memory-R1 只作隔离实验上界，禁止其更新／删除语义触碰真实 vault；
15. 旧 SOS-PAR singleton；
16. `WIT-min consensus`、flat transport verifier 与条件完整 WIT/CUT；
17. 便宜复合臂 `PGR-style probes + TCA-style abstraction + simple CBR outcome aggregation`；
18. 上述简单组件中在独立 calibration 上最强的预冻结复合对手。

所有方法使用同一个候选 vault 快照、可见字段、LLM 调用预算、候选输出上限、renderer 和人工标注。TCA-SIR／PGR 若因训练数据或作者实现不可完全复现，必须写明 fidelity gap，并同时给“忠实到可实现程度”与“公平预算”两种臂。

冻结 \(\Delta_m=Precision_{DSR}-Precision_m\)。若其单侧区间上界 \(U_{\Delta_m}<\delta_{min}\)（主协议建议 \(\delta_{min}=0.10\)），则 DSR 没有证明预注册的实质增益；若成本更高，优先采用简单方法。若另要声明实践等效，必须预先定义独立等效界 \(\epsilon_{eq}\)（例如 0.02）并做 \(|\Delta_m|\le\epsilon_{eq}\) 的等效检验，不能把 0.02 与 0.10 两个含义不同的界混成一句停止线。

#### 15.0D.8 关键消融

最小必做消融：

- same-seed validation vs independent-event validation；
- singleton vs multi-cutpoint real signature；
- no natural contrast vs full contrast panel；
- remove bridge／foil／null one at a time；
- static BlindCEF vs BlindSourceLedger+WitnessedTCA；
- direct query vs Need-Path probes；
- one rival vs material rival set；
- random validation retrieval vs frozen discriminative retrieval；
- fixed panel vs robust-EPIG active reveal（后者只在第二版）；
- no sealing／outcome-visible／full-embedding leakage positive controls；
- no untouched holdout；
- no post-selection calibration；
- same-model seeds vs cross-family end-to-end robustness checks；
- no historical DSPT reliability；
- WIT／CUT 分别加回，检验是否有剩余增量。

若全量收益只来自某个简单组件，保留该组件并删除其余层。若收益只在 outcome-visible、seed self-validation 或合成 foil 条件出现，判定核心假设失败。

#### 15.0D.9 目标侧 DSPT 评价

按两个阶段进行：

1. **历史 rolling-origin**：对每个历史目标在 \(t\) 冻结预测，只读 \(<t\) 材料，再揭 \(>t\) 自然结果；按 owner／time block 外切分。
2. **真实 future shadow**：另行批准后，候选与预测保持 `never_shown`，到预定义窗口结束一次揭封。

必须同时比较 \(H_C\)、strong target baseline、nearest rival 与 no-stable-relation。目标指标包括 paired log score／Brier、校准、coverage、censoring、missingness sensitivity 和跨时间漂移。来源侧通过而目标侧没有剩余技能时，保留来源研究结果但停止“真实迁移”主张与产品化。

可见 Spark 的目标帮助度必须是另一项实验：`no-memory / equal-length distractor / DSR candidate` 由独立模型生成回答、人工盲评；不能把完全 shadow 的自然结果预测当成“显示后有帮助”的证据。

#### 15.0D.10 跨实现稳健性而非只换 seed

最低角色隔离：

- 模型家族 A 产生 discovery MechanismCard；
- 模型家族 B 在看不到 seed 结果的条件下执行验证映射与概率提交；
- 确定性程序评分原文 span、hash 与揭封结果；
- 独立人工只核验事件身份、轴、cutoff、cell assignment 与证据位置，不判断模型最终胜负；
- 在新的 confirmation block 交换 A／B 角色；
- 至少两个由不同模型家族主导、提交完整概率与签名的端到端实现，加一个与 LLM 分离的确定性 scorer／certificate checker；报告 leave-one-family／implementation-out winner reversal。确定性 verifier 不是第三个端到端模型。

同一模型不同 seed 只算稳定性检查；跨模型家族也只能称 robustness check，因为仍可能共享语料、标注、代码和 evaluator。真正的独立复制留到阶段 6D 的新数据／时间块、独立实现或团队。任何一个模型家族方向反转都必须进入结论，不得只报告多数票。

### 15.0S SOS-PAR Benchmark、旧首轮主终点与最小 2×2（冻结基线）

本节是旧 SOS-PAR 冻结基线规范。第 15.7 节的 WIT 目标内配对辨别率保留为条件审计终点；旧阶段 0S／1S 的 query 级 selection-aware 复合损失仅用于复现，不能替代第 15.0D.5 节的 DSR-CT 固定覆盖率主终点。严格 proper-score 性质只属于共同 gold-valid、outcome-scoreable 面板上的纯 Brier，不延伸到把 invalid／missing 记为 1 的端到端复合损失。

#### 15.0.1 新任务的最小标注原子

SOS-PAR Benchmark 的最小原子是：

\[
(q,H_F,H_R,b_i)
\]

每个 query package 至少包含：

- 一个只含当前问题前缀的目标 query；
- 查询盲冻结的 \(H_F,H_R,H_{other}\) 和一个诊断探针；
- 固定来源池，建议每个 query 有 32–64 个纵向候选；
- 每个候选的 \(X_i,A_i\) 可见包、独立盲封 \(Y_i\) 包与字段级 provenance；
- 至少一个真 transport、一个同主题异机制的表层假朋友、一个同机制但方向翻转候选、一个错误主体对照、一个时间反转对照和一个 null／零变化候选；
- 事件级 group ID，防止同一来源的多个摘要跨分割或重复计分。

当前已有 60 例只能用于“能否构造严格单元”和协议开发，不能在看过其结果后同时充当锁定确认集。最小下一批 signal 数据建议另建 60 个独立 query package：12 个只用于冻结提示、基率、阈值和失败规则；剩余 48 个从候选构造起保持结果封存。另建至少 72 个事件／主体独立的 null query pool，才可能在零假发布时使双侧 95% Clopper–Pearson 上界低于 5%；top-8 的候选数不能冒充独立 null 数。48 个 signal query 只能支持大效应 go／no-go，不能据此宣称普遍有效；正式结论还需要更大确认集和独立复制。

#### 15.0.2 三组隔离标注

gold 不能由一个看过结果的标注者一次完成：

- **A 组：transport gold**。只看 query、\(H_F/H_R\) 和来源 \(X_i,A_i\)，标注主体／时间有效性、角色映射与 `valid / invalid / unknown`，看不到 \(Y_i\)；
- **B 组：outcome gold**。只看冻结结果轴和结果原文 span，标注 `-1 / 0 / +1 / unknown`、证据位置与显式混杂，看不到 query、机制和映射；
- **C 组：揭封后审计**。只在 A、B 都提交后，审计结果 span、显式混杂、协议违例和输出是否越界成目标因果主张；“更符合 \(H_F\)、\(H_R\) 或 neither”由冻结概率和 proper score 机械计算，C 组不得另给一个循环性的主观机制 gold。

稳定主体、时间顺序、transport 与结果方向分别报告一致性；不能把一个总 kappa 掩盖某个关键字段的低一致性。

#### 15.0.3 旧 SOS-PAR 复合终点（仅作 legacy stress test）

以下 query 级选择感知复合损失只为忠实复现旧 SOS-PAR 而保留，**不是 DSR-CT 的首轮或确认主终点**。它在 gold-valid、outcome-scoreable slot 使用 Brier，对其余失败使用最大损失 1，因此不是 proper score；更重要的是，固定空槽罚 1 会奖励用低信息均匀预测填满槽位、惩罚正确 abstention。新版不得用它做 go／no-go。候选基线仍按旧协议复现，以便量化该缺陷和与新固定覆盖率选择风险的差异。

每个方法 \(m\) 可以产生自己的结果盲 top-8，但必须固定 \(K=8\)。令 \(c_{qjm}\) 明确表示方法 \(m\) 在 query \(q\) 的第 \(j\) 位所选**候选身份**；不足 8 个时 \(c_{qjm}=\varnothing\)。\(Y_{c_{qjm}}\) 是 OutcomeStore 对该候选身份一次揭封的结果，绝不能按共同 rank-j、另一方法的候选或位置索引替代。定义：

\[
\ell_{qjm}=
\begin{cases}
BS\!\left(p_{qjm},Y_{c_{qjm}}\right),&\text{gold-valid 且 outcome-scoreable}\\
1,&\text{invalid／unknown transport、unadjudicable、重复、缺失 slot 或技术失败}
\end{cases}
\]

在这一 legacy arm 中，\(p_{qjm}\) 必须是揭封前唯一提交的分布；SOS-PAR 使用 \(p_{SOS}\) 的冻结先验混合，绝不能使用揭封后 \(\min(BS_F,BS_R)\) 形成方法损失。旧复现若返回不足 8 个候选，空 slot 按 1 计；该规则不得进入 DSR-CT 主臂。旧 query 级损失与主差定义为：

\[
L_{q,m}=\frac1K\sum_{j=1}^{K}\ell_{qjm},
\qquad
\Delta_{selection}=\frac1Q\sum_{q=1}^{Q}
\left[L_{q,blind\text{-}baseline}-L_{q,SOS\text{-}PAR}\right]
\]

推断单位是 query；bootstrap 还必须按来源事件／主体保持 group，不得把 top-8 当成 8 个独立样本。系统级 ACL／canary／commitment 防火墙失败使整轮实验无效，不能用损失 1 吸收；候选级 invalid、unadjudicable、超时和缺失才按上式记分。积极门预冻结为：

\[
\Delta_{selection}\ge 0.03
\quad\text{且}\quad
CI_{95\%,lower}>0
\]

这一定义防止在效果不明显时改用下游回答质量、几个漂亮案例或另一个消融做“主要终点”。PairAUC、Recall@8、precision–coverage 和下游盲评是必要的非互偿门或次要终点，不与主终点取平均。

所有进入 Brier 比较的方法都必须在揭封前提交同一标签空间上的完整概率分布。原生只给排序／硬标签的方法只能使用在 12 个协议开发 query 上冻结的共同校准适配器；若无法产生合法分布，则其 Brier 记为 N/A，只参与排序指标，不能把不可比较的 raw score 强行归一化。正式主对手必须是预冻结且能合法输出概率的最强结果盲方法。

纯 Brier／calibration 的共同面板不能在揭封后由各方法 top-8 的交集拼出。协议持有者必须在任何测试结果揭封、方法测试分数或 transport gold 可见前，仅从固定 BlindIndex 候选身份与结果前分层字段，按冻结的确定性 hash 抽样规则生成 \(\mathcal P_q\)，并提交面板 ID、顺序与 hash。每个参加该比较的方法必须在同一次预揭封承诺中同时提交：

1. 自己 native top-8 的 \(p_{qjm}\)，用于选择感知主终点；
2. 对每个 \(c\in\mathcal P_q\) 的独立 \(p^{panel}_{qmc}\)，只用于共同面板的纯 Brier／calibration。

全部承诺完成后，才按事前独立封存的 gold-valid、outcome-scoreable 标志形成共同可评分子面板；必须同时报告其原始面板规模、scoreable coverage 和排除原因。某方法缺少任一面板预测时，其共同面板结果为 N/A，不能揭封后补跑、缩成交集或用 native top-8 预测代替。该面板指标检验同一候选上的预测质量，但不替代端到端选择主终点。

“全部承诺完成”由一个预注册的**跨方法揭封屏障**机械判定，而不是各方法自报。阶段 1S 开始前冻结唯一 `benchmark_run_id`、精确方法 ID 集合、benchmark 配置、共同面板和方法集合 hash；每个方法提交自己的 native top-8 身份与概率、完整面板概率及追加日志位置。orchestrator 验证 run／配置／方法 ID 不多不少、每个实际 panel commit hash 与锁定 registration 逐一相等、所有 commitment 均早于任何结果访问，再让项目所有者对该精确 locked manifest 与 evaluator 协议做预揭封签名，之后才对所有 native 候选的去重并集加共同面板执行唯一一次揭封。随后各方法只取得与自己预提交身份对应的 native 结果，pure Brier evaluator 只消费同一验签 receipt 的 panel 视图。缺方法、迟到提交、跨 run 混拼、揭封后换另一份预先生成的 panel commit、第二次揭封、先跑完一个方法再揭封后跑基线，均使整轮实验无效；不能以缺失损失 1 代替系统级失败。

#### 15.0.4 最小机制 2×2

所有有效实验臂都保持来源结果盲封，只改变证据单元与裁决规则：

| 来源证据单元 | 冻结的 WIT-min consensus | 命名 rival＋proper score |
|---|---|---|
| 单情节 BlindCEF | 当前 WIT-min 共识基线 | 只替换裁决规则 |
| 同主体纵向对照 | 只替换来源证据单元 | 完整 SOS-PAR |

四臂必须由**同一批基础事件**生成：单情节臂只使用该事件的一次前因—结果单元，纵向臂使用同主体冻结窗口内的完整重复对照；不能分别挑两套更适合各方法的文本。候选池、query、模型、预算与结果权限完全相同。flat verifier 另作 leave-one-out，不与 WIT 共识混成一个因子水平。

令 \(e=0/1\) 表示单情节／纵向，\(r=0/1\) 表示 WIT 共识／proper-score 规则，\(\mu_{er}\) 是 query 级平均损失。组件增量为：

\[
\Delta_{proper\mid long}=\mu_{10}-\mu_{11},
\qquad
\Delta_{long\mid proper}=\mu_{01}-\mu_{11}
\]

两者都越过各自实质界，才能说两个组件分别有非冗余增量。若要声称两者存在协同交互，还必须单独检验：

\[
\Gamma_{SOS}^{int}=(\mu_{10}-\mu_{11})-(\mu_{00}-\mu_{01})
\]

因此该 2×2 分别估计：

1. 同主体纵向观察对照是否比推断式单情节响应更可靠；
2. 命名最近 rival 与 proper score 是否比本 2×2 唯一的 \(r=0\) 水平 `WIT-min consensus` 更有效；flat verifier 只在独立 leave-one-out 中比较；
3. 联合臂是否优于两个单组件，以及是否存在额外的统计交互。

联合臂同时优于两个单组件，只支持“两个组件都贡献非冗余增量”；只有 \(\Gamma_{SOS}^{int}\) 也越过预注册交互界时才可称为协同。否则保留有效单组件并删除无增益复杂度。允许检索器或映射器查看来源结果的条件只作为泄漏正控，必须醒目标注 `invalid_for_claim`，不能参与有效方法排名。

为定位泄漏发生在哪一层，可另运行诊断性 2×2：

| 检索可见结果 | 映射可见结果 | 解释 |
|---|---|---|
| 否 | 否 | 唯一有效的完整盲封条件 |
| 是 | 否 | 检索选择已可能泄漏 |
| 否 | 是 | 映射／预测泄漏 |
| 是 | 是 | 后见信息上界 |

后三臂只用于判断旧性能有多少来自结果信息；不能因分数更高被选作产品方法。

#### 15.0.5 指标面板与不可互偿门

必须同时报告：

- query 级 selection-aware 复合主损失（仅 scoreable slot 使用 normalized Brier），以及共同 gold-valid／scoreable 面板上的纯 normalized Brier、calibration 和相对强结果盲基线的 skill；
- `Recall@8` 与 MRR，检验结果盲检索是否漏掉 gold；
- 真远 transport 对表层假朋友的 PairAUC；
- **仅对 `localized_release_status=valid`、即全部候选门与有效 candidate-level 定位证书共同通过的候选**报告 localized-candidate audit precision–coverage 曲线与 AURC，禁止只报高置信通过者精度；`complete_null_pool_only` 没有候选级 precision／coverage，只报告池级检出与假发布；
- eligible 样本上的方向 macro-F1 与完整混淆矩阵；
- query 级 matched-null `pool_false_detection`、`candidate_false_release` 与保守合并失败率及各自 95% 上界；
- 无合法候选 query 的正确 abstain 率；
- outcome-permutation 后 pool／top-k／mapping／prediction hash 的不变率；
- 结果前字段和结果 span 的 provenance precision；
- 结果盲选择相对人工 oracle 恢复的增益，但不把 oracle 当可部署方法；
- 通过者是否改善 query-side 机制判断和人工盲评灵感效用，作为次要外部价值证据。

null 的 Bernoulli 单位必须在协议里唯一化。对每个相互独立的 null query pool \(q\)，定义：

\[
Z_q^{pool}=\mathbf 1\{\text{完整 pool gate 报告 }pool\_signal\_detected\}
\]

\[
Z_q^{candidate}=\mathbf 1\{\text{完整流水线至少产生一个 }localized\_release\_status=valid\text{ 的候选}\}
\]

\[
Z_q^{any}=\max\!\left(Z_q^{pool},Z_q^{candidate}\right)
\]

`pool_false_detection` 与 `candidate_false_release` 分别是前两个变量的 query 均值；主 matched-null 门约束更保守的 \(Z_q^{any}\) 均值。`pool_signal_detected` 即使 audit-only 也属于一次 pool-level 科学检出，不能在分母中消失；候选是否展示不改变 \(Z_q^{candidate}\) 的定义。每个 null query pool 最多贡献一个 0／1，top-8 候选、多个机制或多个探针都不能扩充样本量。若 pool 与 candidate 两个分量另作确认性声明，必须在冻结层级内另外分配 alpha；否则它们只作主 \(Z^{any}\) 的诊断分解。

#### 15.0.6 强结果盲基线与 null

至少比较：

- 结果盲 BM25、dense 与相同通道的 RRF；
- case-based prediction／CDH 风格的结果盲适配；
- 阶段 1S 的 `WIT-min consensus + 共同概率适配器`；完整 WIT-VS／WIT-slim 只在 1W 或以后阶段另列，不参与首轮基线选择；
- OB label-free CMI；
- 最强普通 reranker 与“结果盲检索＋冻结最强 verifier”的复合基线；
- 人工 oracle 只作上界，不参与可部署方法胜负。

所有方法共享候选全集、结果权限、模型、token、调用、延迟、失败预算和 renderer；各自 top-8 允许不同，但必须在揭封前固定。null 至少覆盖：在有交换性依据的层内结果置换、action shuffle、错误主体、时间反转、角色交换、重复事件、无机制 query 和同样大小的空／随机来源池。多候选多探针选择使用与完整选择规则一致的 max-statistic、closed testing 或预注册 alpha 分配，禁止逐候选未校正阈值。若纵向观察数据不满足交换性，outcome permutation 只作压力测试，不作为正式错误率证明。

### 15.1 证据阶段必须隔离

现有 AnaloBench_60 已用于发现问题、形成方法和影响测试设计，因此只能作为探索性开发集，不能再提供严格确认性证据。完整链条必须是：

~~~text
现有 60 例开发
→ 独立校准集，或开发集内严格 cross-fitting
→ 冻结算法、校准器、阈值、预算和分析代码
→ 全新锁定确认集，仅评估一次
→ 独立复制集
~~~

在继续使用 60 例前，正式协议必须写明允许的设计修改轮数这个整数；本文不替项目所有者事后选数。用它得到的效应只能标为探索性，不能直接作为确认功效分析的乐观效应。样本量应以预先批准的最小产品意义效应和保守方差做模拟，并计入案例聚类、调用失败与缺失。

开发、校准、确认和复制必须按人物、vault、基础事件、来源记忆和机制组整体切分，不能只拆候选对；同源改写不得跨集合。

### 15.2 两个互补测试层

机制证伪层要求每个目标都有完整、平衡、随机排位的四联体：

| 类型 | 结构评分预期 | 表层评分预期 | 主要用途 |
|---|---:|---:|---|
| 远距离结构对应 | 高 | 低 | 检查跨领域结构识别 |
| 近距离结构对应 | 高 | 高 | 检查常规对应识别 |
| 表面假朋友 | 低 | 高 | 检查表层偏置 |
| 无关候选 | 低 | 低 | 检查基本拒答 |

结构 × 表层四联体回答“有没有结构、表层近还是远”，但还不能回答“可比结构是同向还是逆向”。因此另设一个与四联体正交的**有符号方向机制层**：

| gold 状态 | 受控构造 | 必须发生的变化 |
|---|---|---|
| `aligned` | 至少两个非冗余探针在冻结坐标中同向 | 表层换皮后类别保持 |
| `opposed` | 保持 \(\Pi,\sigma_U,\sigma_O\) 不变，真实翻转 aligned 条件中候选的全局响应极性 | `aligned ↔ opposed` |
| `mixed(P)` | 在响应揭封前冻结两个轴／阶段分组，各组方向稳定且相反 | 单一同向或逆向假设失败，冻结 P 在 `internal_audit_heldout` 成立并在 `external_challenge` 复现 |
| `no_supported_correspondence` | 角色、控制权、必要条件或因果顺序被有证据破坏 | 返回有证据 `mapping_status=no_supported_correspondence`，不能靠 unknown 通过 |
| `contradicted` | 在相同实体、时间、条件与模态范围内制造不可同时成立的必要命题 | 与 `opposed` 分开，不得只因方向相反就报矛盾 |
| `unknown` | 删除关键证据、隐藏基线、制造轴方向歧义或跨越非单调区间 | 原有强判定必须退化为 unknown |

每类都必须包含表层保持版和适用的结构破坏版；阶段 0A 为每个变形算子冻结 `source_class → expected_mapping_status / expected_direction_class / expected_reason_code`，不接受只有“应该变化”而没有目标状态的对照。以下五条均为必要硬门：

~~~text
表层换皮：方向类别不变
坐标反向命名并同步更新 σ：方向类别不变
固定坐标与 σ、真实翻转候选全局响应极性：版本成员／结构 claim 不变，仅 aligned ↔ opposed
固定坐标与 σ、真实翻转单点关键响应：纯类 → 预冻结 mixed 或 unknown
关键证据删除：有判定 → unknown
~~~

第一条符号相关变形检验的是坐标不变性，后两条才检验真实响应变化；禁止把坐标重参数化本身当成关系反转。坐标不变性必须覆盖 \((\sigma_U,\sigma_O)\) 的 `(+,+) / (+,-) / (-,+) / (-,-)` 四种组合，而不是只测单轴示例。为避免把两级状态机压扁成含义混乱的单一六分类，方向机制门定义两个冻结宏平均召回：

\[
BA_{map}=\frac14\sum_{y\in\{supported,no\_supported,contradicted,unknown\}}Recall_y
\]

\[
BA_{dir}=\frac14\sum_{y\in\{aligned,opposed,mixed,unknown\}}Recall_y
\quad\text{（在 gold mapping=supported 的样本上）}
\]

并定义 `SignedOrientationGate`：

\[
O=\min(BA_{map},BA_{dir})
\]

受控集必须分别构造 `mapping_status=unknown, direction_class=unknown`（结构证据不足，reason code 指向 mapping）与 `mapping_status=supported, direction_class=unknown`（映射成立但方向证据不足、非单调或跨转折点）两种 unknown；前者只进入 mapping 门，后者进入 supported 条件下的 direction 门。任一预注册类零分母都表示测试集无效／需补足，不能把 recall 记为 1 或临时删类。

`unknown` 是真实类别，不是可从分母删除的 abstain：对非 unknown gold 输出 unknown 必须计错；调用或解析等技术失败保留在对应 gold 分母中并计错，不能因失败而提高分数。所有表层变体与 metamorphic sibling 按其基础模板／机制组聚类，区间按该聚类单位估计，不能把同源改写当独立样本；报告中同时列每类基础模板数与变形实例数。公开 realized cases 只用于开发；隐藏 `external_challenge` 的 seed、模板与实例由独立持有者一次揭封，揭封后永久降级为开发数据，不得在同一研究版本中反复重跑到过门。除两个 4×4 混淆矩阵外，还必须报告六种最终状态的投影混淆、aligned↔opposed 互换错误率、坐标重命名不变性错误率、mixed 被压成纯类的比例、false-mixed 率、无证据补全率、证据删除后仍强判的比例和 unknown risk–coverage 曲线。

宏平均不能掩盖关键类完全失效。阶段 0A 还必须为 mapping 各类 recall、`aligned/opposed/mixed` 各类 recall 分别冻结下限，并为非 unknown 方向 gold 定义 `SignedOutputCoverage`（输出 `aligned/opposed/mixed` 的比例）下限及 abstention 上限；任一关键类或覆盖门实质失败都否决有符号方向主张，不能靠其他易类拉高平均。

这一门不替换第 15.7 节的唯一选择器主终点；前者检验“方向判据是否真的工作”，后者检验“自动筛选是否解决产品核心失败”。有符号方向分类器整体是待证研究核心；`opposed` 的产品输出是另加独立门禁的扩展。若方向机制门失败，当前冻结的 signed 研究版本终止，所有 `aligned/opposed/mixed` 方向输出强制为 `unknown`，不得进入后续 signed 阶段；只可在独立命名的 unsigned structural-retrieval baseline 中继续记录召回，不得把它称作正反方向成功。任何 prompt、模型、P、阈值或对照修订都必须提升研究版本、重新预注册并使用新的校准集与未揭封 `external_challenge`。

自然开放库层保留真实候选数量、相关度和难度分布，用于外部有效性。它不能被合成四联体替代，确认集中应有预先冻结的、占实质比例的未经算法语言重写的自然记忆；具体比例由协议在看确认结果前批准。

“结构高低”和“表层高低”不是构造者自封标签。与算法输出隔离的人工评审者必须分别做连续评分，并预注册高低边界、局部映射规范、歧义处理和一致性门槛。

自然开放库的 qrels 必须在确认前选择并冻结一种构建方案：

1. 对规模有界的候选库做穷尽人工判断；或
2. 在所有基线和 WIT 输出锁定后，对它们的 pooled-depth 候选并加预先抽取的随机审计样本统一盲审。

第二种方案中，未评审候选只能记为 missing，不能当作负例。若 qrels 不能达到预注册完整度，自然层只能报告“所有实际输出经盲审后的 Precision”和覆盖范围，不能声称 Recall@K。自然层的主要指标、pool depth、随机审计比例、缺失处理和通过门槛都必须在开标签前冻结；也不能只评 WIT 输出而不评基线输出。

### 15.3 防止数据集迎合 WIT

- 自然类比由不了解 WIT 内部打分的独立人员收集；
- 候选构造者、结构标签员、输出评审员和忠实度评审员隔离；
- 标签员看不到候选预期类型、selector、模型或条件；
- 合成扰动只承担机制压力测试，不能独占主要外部有效性结论；
- 确认集包含人工原生困难负例和自然不变文本；
- 若使用生成器制作扰动，测试至少包含未参与开发的 generator-disjoint 子集；
- 每个合成破坏版由独立人工确认自然、难度相近且只改变预定机制。

### 15.4 公平基线与相同资源

至少比较：BM25、原文 dense、BM25+dense RRF、行动过程表示、LLM 直接排名、NLI／contradiction 与否定感知 reranker、同调用预算的 best-of-N LLM 映射／自一致性选择、忠实复现的静态结构映射、Sultan–Shahaf 风格关系映射、YARN／CANA 风格机制分解、adaptation-guided retrieval、Akifuji–Tsuji／Fanizzi DiVS-style version-space 类比适配、Relish-style 关系版本空间适配、GED／partial GW、查询无关只读 sidecar 的 label-free RootMem 适配、GSW／Panini native 表示及其 OB 安全适配、Claim-Selective／SURE-RAG／可适配 MedRAGChecker 语义 verifier、PCC-style 独立 deterministic checker、最近邻复合对手、OB label-free CMI、旧 Spark 最强自动选择器、Spark-CUT、WIT-VS、人工 oracle 和无记忆／随机记忆条件。

相邻方法不能只用名称相似的简化 prompt 充当基线。AGR 需要有冻结适配代价；GED／partial GW 既做“共享 WIT-CEF 表示”的诊断臂，也做各自 native／文献忠实表示的强实现，前者不能替代后者，否则 WIT 可能用自己的表示限制基线；CANA 风格基线要有机制分解和结构反馈，CMI 必须区分论文忠实／oracle 与 OB label-free；RootMem、GSW 与 Panini 都先运行尽可能忠实的 native 表示／检索臂，再运行满足 OB 边界的查询无关、原文可追溯、只读 sidecar 适配臂。两臂都与 WIT 使用相同历史文本和结构化 token／调用预算；安全适配不得把训练标签或未来对话泄漏进结构记忆，不得让持续整合的 workspace 替代原文真源。若官方实现或关键数据不可得，应明确标为 reconstruction baseline，并由不了解 WIT 输出的人员审查忠实度。

RA-RFT 属于需要检索监督与策略后训练的相邻方法，若没有等价训练数据和算力，只能作为**资源不匹配的外部近邻**单列，不能把“无法同预算复现”写成 WIT 胜出；若条件允许，应在额外训练臂比较其 reasoning-utility retriever，而不混入主要在线只读预算对比。

所有能 abstain 的方法都要在同一覆盖率或同一风险预算上比较；另设一个只对普通 reranker 做独立校准的 selective-prediction 基线，防止 WIT 仅凭更频繁返回 unknown 获得表面精度。best-of-N 基线必须使用与 WIT 相同的模型调用、token 和候选尝试数，检验收益是否只是“试得更多”；NLI／contradiction 基线专门检验 signed/CUT 子层是否胜过成熟的蕴含、矛盾与否定识别。

开发结束时必须从非 WIT 方法中，在与正式测试隔离的适配数据上按预注册规则冻结一个最强自动基线 `auto*` 作为主要比较对象。所有自动方法访问相同记忆池，使用相同查询信息、候选上限和提示脚手架；同时报告固定计算预算下的准确率，以及达到固定准确率所需的成本。预算账本必须包括 CEF、RootMem／GSW／Panini sidecar 的冷启动抽取或写时整合、source 变化后的刷新、存储、索引、solver 和维护成本，并按阶段 0A 预注册的查询寿命／请求数情景摊销，同时单列冷启动和稳态在线成本。不能只比较单次在线 token，让任一方法把主要成本藏到离线管线，也不能让 WIT 仅靠更多 token、模型调用或更长输出获胜。

选择器能力必须用固定 renderer 隔离评估：不同 selector 只负责给出来源条目，随后全部经过同一个、与 selector 无关的冻结 renderer，再交给同一回答模型。各条件的候选数、原文预算、结构说明预算、置信提示和输出 schema 必须相同。完整 WIT 自带映射、反例和未知项的端到端呈现可以另作次要产品效果，不能与选择器因果效果混为一谈。若研究 renderer 价值，则采用预注册的 `selector × renderer` 因子实验。

A3E、YARN 或其他文献方法若作为基线，必须冻结忠实实现、提示、模型版本和预算，不得用简化稻草人替代。

### 15.5 将召回与验证拆开测

在 qrels 足以支持相应指标的测试层中，必须分别报告：

1. 候选生成：全库检索的 Recall@K；
2. 结构验证：强制将 gold 放入 Top-K 后的选择、排序和拒答表现；
3. 真实端到端：从全库召回到最终输出的整体表现。

这样才能区分“正确记忆根本未被召回”和“召回后被验证器误杀或误选”。候选池扩大压力测试还应报告性能下降斜率。

### 15.6 核心消融

WIT-VS 最小核心消融：查询无关 CEF、Need Frame、有限 DSL、逐操作 witness、\(\epsilon\)-近最优 version space、竞争类别程序搜索、`internal_audit_heldout`、matched null、Need Frame 增量门和 CUT 方向验证。每个关键部件都要有明确退化条件：

- version space → 只取单一最低成本映射；
- 查询无关 CEF → 看见 query 后重新抽取候选；
- heldout → fit 与评价共用同一探针；
- witness → 只给自然语言解释、不绑定 span；
- rival search → 只抽样几个映射；
- Need Frame → 只判结构相似、不判当前是否冗余；
- matched null → 只与随机无关文本比较；
- CUT → 通用 cosine 或单次 LLM 正／反打分。

为了检验“查询无关证据承诺 × 版本空间全体一致”是否真有不可约作用，而不是把两个熟悉组件并排包装，阶段 1W 先运行冻结的 **CEF × version-space 2×2**：`严格 pre-query CEF / query 后重抽 CEF` × `全并集版本空间发布 / single-best 发布`。四臂保持候选盲 Need／claim、DSL、候选池、模型、fit／heldout、null、调用与 token 预算一致；query 后重抽臂只能作为研究反事实，永不进入产品。主报告在同一有效输出覆盖率上给出表面假朋友误接纳、结构 rival escape、matched-null family error 和 WIT 分支终点，并预注册两个主效应、交互和删模块动作。若严格 CEF 或 version-space 主效应的区间上界低于相应最小实质界，删除对应复杂度；若只有完整联合臂改善且交互达到冻结界，才保留“证据预承诺与全体一致联合降低自证”这一机制假设。这个因子实验检验组合价值，不自动证明原创性。

但上面的 2×2 仍没有隔离第二个承诺。阶段 1W 因此再运行一个互不替代的 **双承诺 2×2**：`查询前 actual-payload CEF / query-conditioned 候选表示` × `candidate-blind Need＋claim lattice / candidate-visible Need＋claim`。此实验固定使用同一个全并集 version space、逐操作 witness、rival search、冻结 neutral proposer、候选池和预算，防止把 version-space 差异混入双承诺效应。两种事后臂同样只用于研究反事实。必须预先冻结两个主效应、交互、完整双承诺臂相对每个单承诺臂的配对差值，以及“某承诺无非冗余增量就删除该承诺复杂度”的动作；只报告完整臂最好而不报告主效应／交互，不足以支持“双承诺”解释。

#### 15.6.1 共同盲接口与非稻草人比较

近邻比较统一使用一个在正式数据揭封前冻结的 `CommonBlindOutput`：`candidate_id`、全预序或 reject、`structural_claim`、角色／条件／阶段／结果 binding、双侧 evidence spans、被填补的 Need slots、abstain／技术状态和成本。人工 gold 只由查询、原始候选与冻结任务定义产生，标注者看不到方法名、分数、承诺条件和预测方向；四联体角色、支持 span、Need slot 与允许 binding 的 gold 版本及仲裁日志必须先哈希提交。

比较分成两条同时报告的轨道：

1. **native 轨道**：只读取方法原生能够给出的字段，不把缺失字段事后脑补；构念本来不产出某字段时记 `N/A`，不算算法错误，但该方法也不能靠这个轨道支持相应机制主张；
2. **共同 proposer 轨道**：同一个看不到方法名和 gold 的冻结 neutral proposer，为所有方法生成同一组候选结构／binding 提案；各方法只能接受、拒绝或弃权。该轨道隔离“提议能力”和“验证能力”，是 Claim-Selective／SURE-RAG／可适配 MedRAGChecker 语义 verifier、PCC-style 独立 checker、DiVS／Relish-style version-space 适配与 WIT 机制比较的主轨道。

`N/A` 不计作失败，也不得从一个不适用指标偷换成成功；若共同 proposer 轨道本应可运行，API、解析、schema、超时或版本漂移属于技术失败，保留在预注册目标分母并按该安全指标最坏结果计分，同时另报可用率。所有阈值只用正式测试隔离的适配集确定；比较按 target／基础事件／机制组聚类，在同一有效 mapping／Need output coverage 上完成。不能靠更严 abstain 降低错误，也不能把同源 paraphrase 当独立样本。

最强近邻不是某一篇论文的裸实现，而是一个信息权限、neutral proposer 和总预算均相同的冻结复合对手：`GSW/Panini workspace + DiVS/Relish-style version space + 最强 semantic evidence verifier + 独立 PCC-style deterministic checker`。semantic verifier 只可从 Claim-Selective、SURE-RAG 与资源可忠实适配时的 MedRAGChecker 中，用正式集隔离的适配集按预注册同覆盖安全指标选定；没有足够独立适配集时三者全部保留并做 multiplicity 调整，MedRAGChecker 的 KG／蒸馏资源不匹配记 N/A 而非失败。PCC-style checker 只检验确定性 artifact，不得冒充自然语言证据真值 verifier。复合对手与各单项基线分别报告。

另做逐项 leave-one-out：去掉 actual-payload 预承诺、去掉候选盲 Need／claim、去掉逐操作双侧 witness、把并集版本空间缩成 single-best、去掉 rival search，以及把“结构先发布、方向后嵌套”替换为同预算的 flat／post-hoc 单层 gate。最后一臂必须分别报告 unsigned 与 signed coverage／error，不能用 signed 总分掩盖结构误杀或方向误放。若复合对手或任一更简单 leave-one-out 在实质界内等效，就删除没有非冗余贡献的 WIT 部件；不能用“组合名字不同”维护复杂度。

#### 15.6.2 四个区分指标的严格操作化

1. **`PostHocAtomDrift`**：单位为 `source candidate × 预注册 CEF field`。固定源字节、抽取器／配置／seed、允许访问的输入、哈希算法和基准 pre-query 承诺；仅在隔离实验臂中改变本应不可访问的 query-canary／候选摘要暴露。每个实验臂都必须从输出重新规范序列化并计算自己的内容摘要 `cef_payload_digest` \(d_M\) 与完整 commitment \(h_M\)，不能把 \(d_M\) 相同当作入样条件，否则被测字段按定义不可能变化。以冻结 field ID 对齐两臂字段并在其并集上比较 atom identity、source span、required／optional、scope 与 modality；任一新增、删除、字段变化或 \(d_M\) 不同都计 drift。完整 \(h_M\) 只验证各臂自身 provenance；它因提交时间戳／run metadata 不同而跨臂不等时，不能单独计 drift。分母是完整预注册来源上两臂字段并集，不能只保留稳定字段。native 无候选表示的方法记 N/A；共同 proposer 轨道中的技术失败按 drift 计，并另报失败率。
2. **`PostHocNeedDrift`**：单位为 `target × query-only blind reference slot`，并把 candidate-visible 条件新增的槽位纳入对称差。独立标注者先只看查询，冻结 goal、constraint、unresolved transition、槽位顺序、requiredness、specificity 和 claim lattice；随后只改变候选可见性。主值为规范化槽位集合的对称差率，另报 requiredness／specificity 变化率和 `NeedCoverage`。候选内容没有查询内证据却改变或新增槽位即计 drift；技术失败按 reference slot 丢失计，不能通过少输出制造稳定。
3. **`SurfaceFalseFriendAcceptanceAtMatchedCoverage`**：单位为完整四联体中的 gold `surface_fake` 候选；分子是达到冻结结构发布／排序阈值而被接纳的表面假朋友，分母是全部预注册 surface-fake 候选。阈值仅在隔离适配集上选到共同 output coverage，正式集不得重调；目标级技术失败按安全保守规则计误接纳，并同时进入 OutputAvailability 报告。另给 available-case 敏感性分析，但不能替换主值。
4. **`UnsupportedCrossDomainBindingRate`**：单位为共同 proposer 轨道中预声明的 binding operation。一个已接受 binding 只有在查询侧 span、候选侧 span、scope／modality 相容且独立盲 gold 支持其局部传输时才算 supported；缺任一双侧证据、越出 scope、把 unknown 补成已知或 gold 否定时计 unsupported。分母是 matched mapping coverage 下全部应裁决的预声明 operation；漏裁、malformed 或技术失败按 unsupported 计并另报，避免以不输出 binding 降低错误。

第五个配套量 `StructuralRivalEscapeRate` 沿用第 15.15 节定义：分母固定为全部预注册、经 blind gold 确认在冻结 DSL／证据／codebook×completion×\(\epsilon_{max}\) 与预算内至少存在一个合法另一结构 claim 或 unknown rival 的 `rival-positive` 候选；分子是系统仍发布原结构 claim 的候选。独立 challenger 的技术失败、超时或 search gap 未闭合保留在该分母并按 escape 计，同时另报 ChallengerAvailability；不能只在成功找到 rival 的运行上计算。该量以候选为单位，不能与 operation 级 `UnsupportedCrossDomainBindingRate` 混成一个分数；自然开放库上的 rival 发现覆盖率另报，不拿未知真值样本稀释此门。

#### 15.6.3 两个可证伪的近邻预测

阶段 0A 为每个误差指标冻结最小实质改善 \(m_k>0\)、coverage 非劣界 \(\varepsilon_C\)、成本非劣界 \(\varepsilon_{cost}\)、绝对 coverage floor、聚类区间、层级和同时推断方法。若可用独立适配集预选最强近邻 \(b^\star\)，正式集只比较该一个；否则对全部危险近邻做 multiplicity 调整，不能看结果后挑最弱对手。对“越低越好”的指标统一定义 \(\Delta^{P}_{k}=E_{b^\star,k}-E_{WIT,k}\)。另定义 \(\Delta^{NI}_{cov,x}=Coverage^x_{WIT}-Coverage^x_{b^\star}\)，其中 \(x\in\{need,map\}\) 分别表示 P1 的 NeedCoverage 与 P2 的 mapping coverage，并定义 \(D_{cost}=Cost_{WIT}-Cost_{b^\star}\)。coverage 非劣成功要求 \(L_{\Delta^{NI}_{cov,x}}\ge-\varepsilon_C\)，实质失败为 \(U_{\Delta^{NI}_{cov,x}}<-\varepsilon_C\)；成本非劣成功要求 \(U_{D_{cost}}\le\varepsilon_{cost}\)，实质失败为 \(L_{D_{cost}}>\varepsilon_{cost}\)。边界等号、成本摊销和同时区间方法一并冻结；所有误差改善、非劣与绝对 coverage floor 进入同一个预注册 hierarchical／simultaneous family：

1. **双承诺抗事后漂移预测 P1**：相对 GSW／Panini native workspace、OB 安全 sidecar、query-conditioned 表示和最强复合对手，`actual-payload CEF commitment × candidate-blind Need/claim commitment` 必须使 `PostHocAtomDrift`、`PostHocNeedDrift`、`SurfaceFalseFriendAcceptanceAtMatchedCoverage` 三项经调整的同时下界都超过各自 \(m_k\)，同时 `NeedCoverage` 满足 \(L_{\Delta^{NI}_{cov,need}}\ge-\varepsilon_C\) 及冻结绝对 floor。还必须在双承诺 2×2 中满足完整臂相对两个单承诺臂的冻结增量门。若任一误差改善上界低于其 \(m_k\)、coverage 的 \(U_{\Delta^{NI}_{cov,need}}<-\varepsilon_C\)，或绝对 floor 实质失败，则 P1 实质失败；其余为无结论。N/A 不算失败，但只要共同 proposer 轨道未覆盖三项，就不得声称 P1 成立。
2. **逐操作版本空间抗无支撑绑定预测 P2**：相对语义证据 verifier、PCC-style 独立确定性 checker、DiVS／Relish-style version-space 适配和最强复合对手，`双侧逐操作 witness × codebook/completion/ε 并集版本空间 × rival search` 必须使 `UnsupportedCrossDomainBindingRate` 与 `StructuralRivalEscapeRate` 两项经调整的同时下界都超过各自 \(m_k\)，mapping coverage 同时满足 \(L_{\Delta^{NI}_{cov,map}}\ge-\varepsilon_C\) 与绝对 floor，总摊销成本满足 \(U_{D_{cost}}\le\varepsilon_{cost}\)。任一误差改善上界低于实质界、coverage 的 \(U_{\Delta^{NI}_{cov,map}}<-\varepsilon_C\)、成本的 \(L_{D_{cost}}>\varepsilon_{cost}\)，或绝对 floor 实质失败，则 P2 实质失败；其余为无结论。若普通证据检查、确定性证书或历史 version-space 类比检索在同信息和预算下等效，删除 WIT 的 transport／并集／rival 复杂度。

这两个预测把“为什么不是结构化记忆工作区”和“为什么不是普通 version-space 检索＋证书验证器”变成有单位、有分母、有盲 gold、有失败处理的可失败实验。只有 P1、P2 与相应因子实验同时通过，才能把完整联合称为**相对已检索近邻的非冗余增量假设**；任何结果只在单模型、单 seed、非盲标注、不等覆盖率或未计冷启动／维护成本下成立，都不能支撑联合机制主张。

独立 `wit-check` 也要做故障注入而非只做“有／无”性能消融：随机删除 required span、替换 source hash、漏掉一个 completion／codebook membership、扩大 \(\epsilon\) 后保留更强 claim、伪造 weak-null 为 maxT、制造 solver gap。每种错误都必须确定性拒绝；checker 与 generator 共享同一判断函数或在故障上一起放行时，证书主张失效。

扩展模块分别消融：多模型/seed 跨家族检查、主动区分探针、完整响应矩阵、反向映射审计、两类反例搜索、2×2 结构归因、自然后续见证、选择性校准和共激活候选通道。

`CMI+WIT-core` 还必须与三个同预算组合对照同时运行：`CMI+ordinary-reranker`（独立普通 reranker，调用与 token 匹配）、`CMI+structure-ablated-WIT`（保留界面与预算但移除版本空间／逐操作 witness 的结构信号）和 `CMI+permuted-WIT-ranking`（保持分数分布与候选数，打乱 WIT 候选身份）。否则组合优于 CMI 只能说明“又加了一个排序器有帮助”，不能把增益归因于 WIT 的结构机制。

每个消融只改变一个预定义因素并保持模型、候选池、token 和停止预算一致。动态选择改固定层、多候选矩阵改两两打分等替代条件也必须在开发结束时冻结。

有符号核心至少要单独消融：去掉 \(\sigma_U\)、去掉 \(\sigma_O\)、允许逐探针选符号、去掉 `internal_audit_heldout`、把证据删除样本仍交给解释模型、以及用通用 cosine／`Enc(after)-Enc(before)` 直接代替机械方向比较。若放宽冻结规则后表现反而提高，应优先怀疑事后对齐或标签泄漏，而不是把它当成更强方法。

### 15.7 WIT 条件审计的排序终点、操作化与三分门禁

若阶段 1D 来源事件外验证通过、WIT 有独立增量且阶段 1W 被另行批准，WIT 条件审计的选择器终点冻结为：在同一个目标的完整四联体内，系统将远距离结构对应排在表面假朋友之前的目标内配对辨别率。为隔离“召回不到”和“自动筛选错误”，该条件实验强制把同一四联体的四个候选全部送入每个方法的评分适配器；开放候选池 Recall@K 另报。它不能替代第 15.0D.5 节的 DSR-CT 固定覆盖率主终点。

这个主终点只支持一个窄主张：系统是否缓解“真远类比被表面假朋友压过”的当前核心失败。它**不能单独证明一般自动选择器有效**，因为它不检查近距离结构对应、无关候选、无有效候选时的拒答或开放库召回。若要使用“一般自动筛选改善”或进入产品阶段，还必须把以下指标作为不可替代的层级门，而不是事后挑选的次要亮点：

1. 四联体内两个结构对应相对两个结构负例的全部四个配对比较，按目标聚合为 `StructuralPairAUC`；
2. `far > surface_fake`、`near > surface_fake`、`far > irrelevant`、`near > irrelevant` 四个分项均达到冻结下限，不能由一个容易分项补偿另一项完全失败；
3. 含“零有效候选”目标的正确 abstain 率、错误输出率与 OutputCoverage；
4. 自然开放库在冻结 qrels 规则下的 Precision@K／经审计输出精确率、召回边界和风险—覆盖曲线；
5. 相同覆盖率下相对 `auto^\star`、single-best transport 与最强选择性预测基线的错误率。

为使第 1–2 项真正可执行，对任一正结构候选 \(a\in\{far,near\}\) 与结构负例 \(b\in\{surface\_fake,irrelevant\}\)，沿用后文相同的有效性、reject、tie 和技术失败规则定义：

\[
q^{(j)}_{m,t,a,b}=
\begin{cases}
1,&r^{(j)}_{m,t,a}<r^{(j)}_{m,t,b}\text{ 且本次排序有效}\\
0.5,&r^{(j)}_{m,t,a}=r^{(j)}_{m,t,b}\text{ 且本次排序有效、二者并非同时 reject}\\
0,&\text{其他，包括技术失败、目标级 abstain 或两者同时 reject}
\end{cases}
\]

先对重复取均值，再定义四配对目标内平均：

\[
q_{m,t,a,b}=\frac1J\sum_jq^{(j)}_{m,t,a,b},
\qquad
A^{pair}_{m,t}=\frac14
\sum_{\substack{a\in\{far,near\}\\
b\in\{surface\_fake,irrelevant\}}}q_{m,t,a,b},
\qquad
StructuralPairAUC_m=\frac1T\sum_t A^{pair}_{m,t}
\]

四个 \(PairRate_{a,b}=T^{-1}\sum_tq_{m,t,a,b}\) 另行报告。阶段 0A 在任何结果可见前冻结一般 selector 的最低界 \(s_{min}\) 和四个不可互偿分项界 \(s^{pair}_{a,b,min}\)，并使用目标／基础事件聚类区间；四个分项必须分别通过，不能只让平均值掩盖一个类别完全失败。

因此，条件阶段 1W 可以用本 WIT 分支的 \(\Delta_{WIT}\) 决定该分支是否值得继续研究，但完整 selector 主张必须同时通过 `StructuralPairAUC`、四分项、零候选 abstention、自然开放库／相同覆盖率门与第 15.15 节机制门；它不替换 SOS-PAR 的 \(\Delta_{selection}\)。只过 `far > surface_fake` 时，结论必须写成“修复了一个特定配对错误”，不能写成“自动 Spark 已经可用”。

每个方法 m、目标 t 和预冻结重复 j 必须通过冻结适配器输出覆盖四个候选的**全序或允许并列的全预序**。内部可以使用不同固定尺度的分数，但进入主终点前一律转成名次 \(r^{(j)}_{m,t,c}\)，数字越小表示越优，因此不要求 CMI、静态映射、CUT 和 WIT 的原始分数跨方法可比。成功返回的显式候选级 `reject` 可以进入冻结的 bottom/unknown tie 层；API、解析、schema 或候选级技术缺失不是拒绝，适配器虽仍以 bottom/unknown 层补齐形式排序，但整个目标记为无效。OB label-free CMI 以冻结的 intervention utility 排序；`CMI+WIT-core` 采用阶段 0D 冻结的 RRF 及固定常数和 tie 规则。定义：

\[
z^{(j)}_{m,t}=\begin{cases}
1,&r^{(j)}_{m,t,far}<r^{(j)}_{m,t,surface\_fake}\text{ 且本次排序有效}\\
0.5,&r^{(j)}_{m,t,far}=r^{(j)}_{m,t,surface\_fake}\text{ 且本次排序有效、两者并非同时 reject}\\
0,&\text{其他情况，包括远类比排名更低、两者同时 reject、技术缺失或目标级失败}
\end{cases}
\]

候选级显式 `reject` 与技术缺失必须使用不同状态码；技术缺失不得因恰好发生在表面假朋友上而奖励另一候选。四候选全部 reject 必须升级为目标级 abstain，\(z=0\)、OutputCoverage=0；far 与 surface fake 同时 reject 时也记 \(z=0\)，即使其他候选仍形成排序，也明确表示主配对没有得到有效结构判断。目标级 abstain、任何技术缺失和调用失败均记 0、保留在主终点分母；只有至少一个候选非 reject 且没有技术失败时，候选级 reject 才可作为 bottom tie 层的一部分形成有效全预序。多 seed／重复先在同一目标内平均，seed 不是新增样本：

\[
z_{m,t}=\frac{1}{J}\sum_{j=1}^{J}z^{(j)}_{m,t},\qquad
\theta_m=\frac{1}{T}\sum_{t=1}^{T}z_{m,t},\qquad
\Delta_{WIT}=\theta_{WIT\text{-}core}-\theta_{auto^\star}
\]

其中 \(auto^\star\) 是阶段 0C 结束后、正式比较揭封前，在隔离适配数据上按预注册规则从所有非 WIT 自动方法中冻结的最强基线。为单独检验“版本空间／witness 是否比旧 CUT 值得保留”，另定义：

\[
\Delta_{CUT}=\theta_{WIT\text{-}core}-\theta_{CUT}
\]

\(\Delta_{WIT}\) 仅是条件 WIT 分支内的唯一选择器主对比；\(\Delta_{CUT}\) 是该分支终点通过后、与 \(\Gamma\) 同层预排序的机制增量门，不得依据结果选择先检哪一个。阶段 0A 冻结 \(\delta^{CUT}_{min}\) 与 multiplicity 规则：开发期要求 \(\hat\Delta_{CUT}\ge\delta^{CUT}_{min}\) 且区间下界大于 0 才有继续信号；区间上界低于 \(\delta^{CUT}_{min}\) 则停止 WIT 扩展、保留 CUT；锁定确认要求下界超过 \(\delta^{CUT}_{min}\)。该指标相当于目标内配对 AUC／C-index，直接命中“真远类比是否被表面假朋友压制”的核心失败，而且不要求不同目标之间的模型分数可比。普通 Accuracy 易受四格比例和阈值影响，未配对全局 AUC 又会受候选基率影响，因此都不作为该条件分支的唯一终点，更不能替换 \(\Delta_{selection}\)。

主终点的目标集合和 \(T\) 必须在运行前冻结，全部目标始终进入分母。\(c^{pair}_{min}\)、\(c^{reject}_{min}\)、\(c^{output}_{min}\)、K、候选池、候选上限、renderer、token、延迟和调用预算都是额外门禁，不是事后筛样条件。abstain、合法并列、技术缺失、目标级失败、重试和版本漂移的处理必须在阶段 0D 冻结，不能把未覆盖目标静默排除。

CMI 与 `CMI+WIT-core` 使用同一 WIT 分支终点评分，但不替代主对比 \(\Delta_{WIT}\)。定义组合的增量效应：

\[
\Gamma=\theta_{CMI+WIT\text{-}core}-\theta_{CMI}
\]

\(\Gamma\) 只检验“组合是否比 CMI 好”，不单独识别 WIT 结构机制。另对三种同预算对照 \(k\in\{ordinary,structure\text{-}ablated,permuted\}\) 定义：

\[
\Gamma^{attr}_k=
\theta_{CMI+WIT\text{-}core}-\theta_{CMI+k}
\]

\(\Gamma\) 是主终点通过后才检验的第一层次组合增量门；只有 \(\Gamma\) 通过后，三个 \(\Gamma^{attr}_k\) 才按冻结闭合检验层级进入“结构归因”门。阶段 0A 必须在结果可见前分别冻结 \(\gamma_{min}\)、\(\gamma^{attr}_{min}\) 及 multiplicity 规则，阶段 0D 只能原样带入。开发期只有 \(\hat\Gamma\ge\gamma_{min}\) 且区间下界大于 0 才算“有组合开发信号”；只有三个归因对照的同时区间下界均大于 0 且点估计达到 \(\gamma^{attr}_{min}\)，才可说存在 WIT 结构机制的非冗余信号。任一归因对照失败时仍可报告组合效果，但不得归因给 WIT 结构。锁定确认要求组合与全部归因界的同时区间下界超过各自实质界。

为消除任意分数缩放，机制门也使用固定范围的目标内指标。结构破坏敏感性 \(B\) 是“完整远类比排在其结构破坏版之前”的 \(1/0.5/0\) 配对率，最低界为 \(b_{min}\)。结构保持改写用与主终点完全相同的目标、重复和记分函数得到 \(\theta_{preserved}\)，并定义正值表示退化的损失：

\[
D_{preserve}=\theta_{WIT\text{-}core,intact}-\theta_{WIT\text{-}core,preserved}
\]

其最大可接受上界为 \(\epsilon_{preserve}\)。不得再把 \(B\) 或 \(D_{preserve}\) 定义为任意原始模型分数的下降。另定义预算超支 \(K_{cost}=Cost_{WIT}-Budget_{WIT}\) 和 CMI 成本优势 \(A_{cost}=Cost_{WIT}-Cost_{CMI}\)；前者必须非正。成本完全确定时，只有 \(A_{cost}>\kappa_{min}\) 才能把“CMI 成本更低”用于全面占优判断；成本有随机性时，则要求其区间下界 \(L_A>\kappa_{min}\)。

为防止看过 CMI 开发表现后选择有利门限，阶段 0A 必须在运行 0B／0C 前冻结真实记忆审计门，并同时冻结正式比较所需的实质界：

- \(\delta^{WIT}_{min}\)：条件 WIT selector 终点的最小产品意义效应，也是该分支锁定确认时的优效界；
- \(\delta^{CUT}_{min}\)：WIT-core 相对 CUT 值得保留复杂度的最小增量；
- \(\gamma_{min}\) 与 \(\gamma^{attr}_{min}\)：CMI 之上值得保留组合的最小增量，以及完整 WIT 相对 ordinary／structure-ablated／permuted 三个同预算组合的最小结构归因增量；
- \(s_{min}\) 与四个 \(s^{pair}_{a,b,min}\)：一般 selector 的 `StructuralPairAUC` 下限及四个不可互偿正负配对分项下限；同时冻结零有效候选正确 abstain、自然开放库精确率与同覆盖率误差的实质界；
- \(m^{P1}_{atom},m^{P1}_{need},m^{P1}_{surface}\) 与 \(m^{P2}_{binding},m^{P2}_{rival}\)：两个近邻区分性预测中各必要误差的最小改善；同时冻结 \(\varepsilon_C\)、\(\varepsilon_{cost}\)、共同盲接口、native／共同 proposer 轨道、N/A／技术失败、matched coverage、聚类和 simultaneous-inference 规则；
- \(i^{struct}_{min},i^{dir}_{min},c^{dir}_{min},c^{dsl}_{min},u^{cef}_{max},p^{struct}_{min},p^{dir}_{min},a^{struct}_{max},a^{dir}_{max},n^{struct}_{max},n^{dir}_{max}\)：结构版本空间可识别率、eligible 方向可识别率、方向 eligible 覆盖、DSL 适用覆盖、无查询 CEF 不稳定率、分层 heldout 精确率、分层竞争程序残余接受率，以及结构／eligible 方向两条整流水线 matched-null 家族假阳性上限；query-canary 输入隔离违例不设容忍率，计数必须为 0；
- solver gap、版本空间枚举／符号覆盖、claim lattice 最低 specificity／Need 覆盖、\(\epsilon\) 合理区间与翻转、代码本翻转和 out-of-language evidence 的逐候选硬规则；
- \(\eta^{orientation}_{min}\)：SignedOrientationGate 的最低机制门；mapping 各类与 aligned／opposed／mixed 各类 recall 下限；SignedOutputCoverage 下限与 abstention 上限；以及表层换皮、坐标重参数化、真实全局／单点翻转、证据删除、结构破坏和无证据补全的各自错误率上限 \(g^{err}_j\)；
- \(c^{pair}_{min}\)、\(c^{reject}_{min}\) 与 \(c^{output}_{min}\)：正向响应证据、负向拒绝证据和有效输出覆盖下限；
- \(b_{min}\) 与 \(\epsilon_{preserve}\)：固定尺度的结构破坏优效界与结构保持非劣界；
- \(\psi_{min}\) 以及预注册的 \(\Omega\) 可移植性／异质性界：表层 × 机制 2×2 的最小结构主效应与不得被交互掩盖的解释规则；
- 各安全非劣界、CMI 相对 WIT 的结构／下游非劣界、绝对成本预算与成本优势界 \(\kappa_{min}\)；\(\kappa_{min}\) 只决定能否把“CMI 显著更便宜”用于全面占优停止，不是 WIT 科学成功门；
- 合法 tie、置信区间水平、检验层级、所有非主门的开发期动作、一次共享的扩样规则和最大总预算。

如果 \(\gamma_{min}\)、安全界或成本界确实依赖尚未知的调用成本，阶段 0A 必须冻结从延迟、调用数、价格和维护复杂度映射到实质界的确定决策函数。阶段 0D 只能代入 0C 测得的资源常数并冻结 RRF 常数、代理 utility 和 renderer，不能读取排序准确率或下游效果后修改决策函数或任何实质界。

数值决定顺序不得倒置：先由产品价值与错误成本确定 \(\delta^{WIT}_{min}\) 及其他固定实质界，再由冻结决策函数产生其余界，最后用这些界和保守方差做功效模拟以确定所需样本量；样本不足时只能扩样或接受无结论，不能提高实质界去迁就现有 60 例。未填完数值或函数的协议不可执行。

设开发期 \(\Delta_{WIT}\) 的按目标聚类置信区间为 \([L_{\Delta_{WIT}},U_{\Delta_{WIT}}]\)，阶段 1W 使用三分规则：

| 结果 | 判定 | 动作 |
|---|---|---|
| \(\widehat\Delta_{WIT}\ge\delta^{WIT}_{min}\) 且 \(L_{\Delta_{WIT}}>0\) | 有继续开发信号 | 仅在一般 selector、输出／响应覆盖、结构破坏敏感性和结构保持非劣门也通过时，按冻结顺序检验 \(\Delta_{CUT}\) 与 \(\Gamma\)；二者及所需归因门通过后才申请阶段 2 |
| \(U_{\Delta_{WIT}}<\delta^{WIT}_{min}\) | 有意义提升已基本被排除 | 停止当前 WIT 版本 |
| 其他 | 统计无结论 | 不进入复杂模块；只允许一次预注册扩样，否则停止 |

“95% CI 包含 0”可以成为保守的项目不推进规则，但只能表述为“当前证据不足以继续投入”，不能写成已经证明算法无效。条件 WIT 的正式锁定确认比开发门更严格，要求 \(L_{\Delta_{WIT}}>\delta^{WIT}_{min}\)。

阶段 1W 的非主门不再留给现场解释：CEF 输入隔离／无查询稳定性、solver／checker 证书、claim 与 \(\epsilon\) 敏感性、结构与 eligible 方向版本空间可识别、DSL／方向 eligible 覆盖、分层 heldout 共识、分层 rival 残余、结构／方向 claim-level matched-null、`StructuralPairAUC` 及四配对分项、零候选 abstention／自然开放库／同覆盖率错误、SignedOrientationGate、逐类 recall、SignedOutputCoverage、关键变形错误率、\(B\)、\(D_{preserve}\)、PairResponseCoverage、RejectionEvidenceCoverage、OutputCoverage、每个安全指标和 \(K_{cost}\) 均直接使用第 16.1 节对应方向的严格门。结构必要门失败停止当前 WIT 结构版本；只属于 signed 模块的方向门失败则停止 signed 版本、强制方向 unknown，但不自动否决已独立通过的 unsigned 结构基线。任一统计门无结论时，所有无结论门共享 WIT 分支终点那**唯一一次**预注册扩样，不得逐指标各扩一次，扩样后仍无结论则以“证据不足而不继续投入”停止相应主张。

若 \(\Delta_{WIT}\) 通过而 \(\Gamma\) 实质失败或扩样后仍无结论，当前 CMI+WIT 集成路线不得进入阶段 2。WIT-core 可以作为独立结构审计结果保留，但若要继续研究 standalone WIT，必须重新立项、重新冻结目的和门限，不能沿用本轮“已证明 CMI 之外增量”的表述。

“CMI 全面占优且组合无增益”也必须是联合门，而不是主观总结：CMI 相对 WIT 在结构与下游上通过预注册非劣门，在所有安全指标上通过非劣或伤害 veto，以预注册幅度和区间证明成本更低，且 \(U_\Gamma<\gamma_{min}\)。只有这些条件同时成立，才能据此停止当前 WIT；任一项无结论时只能写成证据不足。

现有 60 例已经参与方法形成，只能用于内部开发决策、目标级方差估计和功效模拟。若需要具有正式确认含义的区间，必须在方法冻结后使用全新数据；用于决定继续开发的数据随后也自动降级为开发数据。

在不删除任何冻结目标、并把 \(c^{pair}_{min}\)、\(c^{reject}_{min}\) 与 \(c^{output}_{min}\) 作为独立门报告的前提下，Precision@1、无有效候选时的正确拒答率、Recall@K、AURC、Brier、ECE、证据定位率、错误类型、稳定性、成本和下游帮助度均为次要指标或产品门禁，不能在看到结果后替换主终点。

### 15.8 安慰剂、随机与结构对照

至少包括：无记忆、只有类比提示、多个锁定随机记忆、同主题匹配随机记忆、表面假朋友、最强自动基线、Spark-CUT 子层、完整 Spark-WIT、同候选的结构破坏版和人工 oracle。

随机条件需匹配主题、长度、时间、来源质量、信息密度、记忆数量和 token 数；每个目标使用多个预先抽取并锁定的随机样本。所有记忆条件使用相同提示脚手架与输出预算。结构破坏版必须经盲审确认自然度、难度与信息量匹配，并检查没有固定生成痕迹。

每个硬负例只能破坏一个揭封前冻结的轴，并保存 `changed_axis`、`preserved_axes`、精确修改 span、预期状态变化和制作者身份。角色交换或因果／时间／结果反转后，故事本身仍须自然、连贯、信息完整且可独立成立；不得用突兀否定词、语病、机械角色互换、异常连接词、固定模板或生成器文风制造“一眼假”负例。制作者不能审核自己的负例，且每对必须依次通过两个不可补偿的门：

1. **自然度盲检**：至少两名不知道正负身份的评审只判断流畅度、连贯性、信息完整性、长度／信息量匹配和人工修改痕迹；
2. **单轴审查**：另一名独立评审确认指定轴确已破坏，其他关键角色、事件、关系和极性没有连带变化。

任一门失败，该对照作废且不得进入训练、CMI 或性能分母。自然 foil 优先；合成硬负例只能提供 metamorphic 机制诊断，不能单独成为真实效果的确认性证据。如果系统或盲评者能依据语言质量而不是结构识别负例，所有由此得到的高精度均视为 shortcut，实验无效。

~~~text
检索差分 = 完整 Spark-WIT - 同主题匹配随机条件
结构差分 = 原候选 - 经审计结构破坏版
提示差分 = 类比提示但无记忆 - 普通无记忆
~~~

检索差分和结构差分同时成立只是结构检索主张的必要证据，不是充分证明；完整 WIT 还必须优于冻结的最强自动基线，并通过自然开放库测试。

### 15.9 三模型、多 seed 的冻结规则

确认前必须冻结：

- 三个不同模型家族的精确版本、提供方、已知训练路线、角色、温度、参数和重复次数；
- selector、candidate answerer、opaque comparator 与 evaluator 的预定分工；
- 多 seed 输出的聚合方法，例如多数票、均值或 medoid；
- 候选顺序、回答左右位置和评审顺序的随机化方案；
- 超时、拒答、API 失败、重试、缺失和模型版本漂移规则；
- 禁止事后选择表现最好的 seed、模型或聚合规则。

seed 是同一案例上的重复测量，不是新增独立样本。同一模型家族的多个 seed 只能测偶然采样稳定性，不能排除共享训练分布导致的系统性共病。不同提供方或模型家族也可能共享数据和训练范式，因此只能称“跨家族校验”，不得宣称统计独立。

阶段 2 至少使用两个预冻结模型家族，锁定确认阶段使用三个。每个家族必须分别端到端运行，不能只让一个家族提出结构、另一个家族在其框架内确认。额外的交叉角色消融可以采用：

~~~text
家族 A 抽取 → 家族 B 验证
家族 B 抽取 → 家族 A 验证
家族 A、B 各自独立端到端
~~~

不能把多数投票当作真值。除家族内和家族间一致率外，还必须报告：人工标签下各家族的 precision/recall、跨家族共同假阳性率、多个家族同时接受同一人工负例的比例、unknown 与无证据补全的家族差异，以及候选集合 Jaccard@K、排名 Kendall tau 和证据位置一致性。

只有三个预选模型时，应把模型作为固定分层逐一报告，而不是作为可推广到“所有模型”的随机总体。最终外部依据仍来自人工标签、受控变形程序和锁定未见数据，而不是模型共识。

### 15.10 人工盲评协议

每项至少由三名独立人工评审；LLM judge 只能作为次要敏感性分析。三个隔离层分别是：

1. 结构评审：只判断角色、行动、因果、约束、边界和表层/结构连续评分；
2. 输出评审：只看目标与回答，不知道检索条件；
3. 忠实度评审：核查来源证据和局部对应是否超出原文。

评审界面隐藏模型、条件、selector、候选类型和预期扰动，采用固定量表、培训样本、随机左右位置和必要时的不完全区组设计。协议必须预定义多数票、仲裁和歧义处理，并报告 Krippendorff’s alpha 或 Gwet’s AC1。若核心结构维度一致性低于预设门槛，应判为实验无效并暂停，不得强行制造 gold 或删除不一致项。

### 15.10A 12–20 例 CMI 的小样本解释协议

在投入任何自动比较器之前，先用 12–20 个独立、严格的 `SparkGoldRelationUnit` 运行最小三臂 CMI：`N=no-memory`、`T=strict-positive`、`H=matched-hard-negative`。其唯一作用是判断是否值得继续工程投入；成功不能成为确证，失败也不能从理论上证明所有结构方法无效。

独立统计单位是案例／事件簇，不是回答次数、seed、模型或评审人数。重复生成和多个评分必须先在案例内聚合。CMI 的任务提示必须是中性下游分析，不能询问“是否存在类比”、不能告诉模型某段记忆相关，也不能因 T/H 改变提示脚手架；候选只被表述为可采用、忽略或质疑的未经核实材料。

每例分别冻结并报告：

~~~text
T vs H: better | tied | worse
T vs N: better | tied | worse
case_direction:
  +1 = T 同时优于 H 与 N，且无严重错误
   0 = T 未输给二者，但至少一项持平
  -1 = T 输给任一对照，或引入严重错误
~~~

主要结果是全部案例的方向、幅度、评审理由、改善／持平／恶化计数，以及最严重退化案例。必须列出所有反向案例，不能让少数极端值或均值遮蔽它们。p 值、精确符号检验、区间和效应量最多作为探索性附录；无论 p 值多小，12–20 个经筛选开发案例都不能写成确认、复制或产品证据。

开发期绿色门只允许称为“值得继续的方向性信号”，并须同时满足：有效独立案例至少 12；`+1 >= ceil(0.70n)`；`T>H >= ceil(0.75n)`；`T>N >= ceil(0.67n)`；`T<H <= floor(0.15n)`；T−H 与 T−N 的盲评分中位差均大于 0；没有严重安全／事实退化；结果不由一两个极端案例贡献。

任一情况触发红色停止：`T>H` 不超过半数；`+1` 不超过半数；T−H 中位差不为正；`T<H` 至少占四分之一；至少两个独立案例出现严重错误；硬负例能被语言质量或合成痕迹可靠识别。其余一律为黄色无结论，不实现比较器；只能修订协议并用新的前瞻案例重新测试，不能反复调整同一批案例直到通过。

### 15.11 统计与功效

- 主要分析按目标案例配对；
- 模型处理目标、基础事件派生四联体、来源记忆、人物、vault、机制组和评审员的聚类或分层；
- seed 作为重复测量，模型作为固定分层；
- 预定配对 bootstrap、置换检验或适当的层级模型；
- 多基线、多消融和多指标使用 Holm 或预定层级检验；
- 报告效应量与置信区间，不只报告 p 值；
- 12–20 例 CMI 以逐案例方向一致性和反向案例为主，p 值无论多小都只能作为方向性描述；
- 功效模拟使用最小产品意义效应和保守方差，不使用多轮选择后的峰值效应；
- 确认集原则上只分析一次；若允许中期分析，必须预先规定 alpha spending；
- 确认失败后若修改方法，必须重新建立确认集。

### 15.12 下游帮助度是次要证据

人工盲评可评价：局部对应是否成立、是否产生非平凡视角、是否与张力相关、是否忠实引用、是否引入无关记忆、是否把假设说成必然、是否替模型作决定、是否比基线有帮助。

下游回答变好不能替代选择器与结构对照指标，因为随机上下文、额外 token 或“请类比思考”的提示也可能带来收益。所有条件必须使用相同回答模型、提示和输出预算。

若下游结果进入产品通过链，确认协议必须预注册一个下游主要终点或固定权重的复合终点，并冻结检验层级：`WIT－最强自动基线`、`WIT－匹配随机`、`原候选－结构破坏版`。有用性和新颖度使用预定优效界；错误引用、无关侵入、把假设说成必然和替模型决定使用预定安全非劣界。多维度、多对照必须纳入同一个层级检验或 multiplicity 校正；不得在确认后挑选最有利维度宣称“对照通过”。

### 15.13 压力测试矩阵

至少覆盖：人物、领域与词汇替换但结构保持；词汇保持但因果反转；角色交换；机制相关时序变化；否定、条件与模态变化；同结果不同机制；同机制相反结果；关键调节项缺失；同主题假朋友围攻；重复记忆频率偏差；高共激活但无关；近期偏置；候选池扩展；无有效候选；多个有效候选；证据在长文本尾部；跨语言改写；提示注入；预算耗尽；服务超时和部分模型失败。

压力测试局部失败不自动等于核心科学假设失败，除非它命中预注册主要终点或安全 veto；失败类型按下一节分类。

### 15.14 独立复制

复制集由不同人员、未见领域和锁定分析代码完成，并对主要效应独立充分功效。“不同 vault”只能是独立测试／合成 vault，或数据主体逐 vault 明确同意用于只读研究的数据；项目所有者对方案的批准不能代替其他数据主体授权。复制环境必须访问隔离，不汇集可识别原文，不把任何 vault 内容发送到未经该数据方批准的外部 API。协议还应要求：

- 复制效应方向与确认一致，并且复制集自身达到预注册的最小可接受效应界；也可预先采用 sceptical replication 或 prediction-interval 规则；
- 确认与复制的合并估计下界超过最小产品意义效应；
- 仅仅“置信区间排除零”不构成实质复制；
- 不以“达到原效应某个百分比”作为唯一标准，避免赢家诅咒；
- 任一数据集失败时分别报告，不能只用合并显著性掩盖失败；
- 复制结束前不得根据其结果改动算法或分析代码。

### 15.15 WIT-VS 专属的可识别性与竞争程序门

第 15.7 节的“远结构对应排在表面假朋友前”只是在阶段 1W 内的 \(\Delta_{WIT}\) selector 终点。WIT-VS 另外必须通过以下机制门，不能用该分支终点提升掩盖，也不能替换 SOS-PAR 的 \(\Delta_{selection}\)：

1. `CEFSpanPrecision`：派生原子确实由所指原文支持；
2. `CEFUnknownRecall`：删掉关键证据、主体／模态／时间歧义时能否退化为 unknown；
3. `StructuralVersionSpaceIdentifiability`（\(I_{struct}\)）：在**事前冻结的结构可识别 gold 分母**中，全部合法 completion 与近优 \((T,\omega,k)\) 版本成员是否在 heldout 上支持同一个非空结构主张；技术失败、solver gap 未闭合和 completion 缺失留在分母并计失败；
4. `SignedDirectionIdentifiabilityOnEligible`（\(I_{dir\mid eligible}\)）：只在“结构已支持且查询侧 target response 有独立证据”的事前方向 eligible 分母中，每个版本成员的 \(\mathcal D^{fit}\cap\mathcal D^{hold}\) 能否成为同一个已知 singleton `aligned/opposed/mixed(P)`；同时对全结构 gold 报告 `DirectionEligibleRate`，防止通过缩小 eligible 分母制造高方向精度；
5. `NonidentifiableRecall`：gold 本就有多个合理映射且导向不同结构或方向时，能否分别报 `mapping_status=unknown` 或 `direction_class=unknown`，并给出相应 reason code；
6. `RivalEscapeRate`：分成 `StructuralRivalEscapeRate` 与 `DirectionRivalEscapeRateOnEligible`；独立挑战器找到阈值内另一类别程序而系统仍错误发布的比例，二者不得合并平均；
7. `SolverClosedCoverage`：在冻结预算内真正关闭逐 completion 最优性／竞争程序 gap 的目标比例；
8. `HeldoutPrecision`：分成结构主张精确率与 eligible 方向精确率；相对 shuffled、surface-only、mechanism-matched random 与 renderer-only null 的优势另报，不得用一个高方向子集掩盖结构误报；
9. `NeedIncrementPrecision`：声称填补的未决槽位是否真的在查询不可见候选时已声明、且不是查询已有内容；
10. `CEFInputLeakViolationCount`：在源文本、seed、抽取器和可见输入完全相同，仅改变本应不可访问的 query canary 时 CEF 发生变化的运行数；这是确定性隔离完整性检查，必须恰为 0，不能用比例置信区间容忍；
11. `CEFNoQueryInstability`：在始终不给 query 的条件下，跨预冻结 seed／重复抽取的证据原子、span 和 required/optional 不稳定率；它是可统计的抽取不确定性，不得冒充 query effect；
12. `StructuralFullPipelineNullFamilyError` 与 `DirectionFullPipelineNullFamilyErrorOnEligible`：同 K、同预算 matched-null 记忆池经过整条自适应流水线后，是否仍能轻易产生至少一个 unsigned 结构或 signed 方向发布候选；方向不 eligible 记 N/A，结构 p 值不得复用为方向 p 值；
13. `CodebookFlipRate`：在事前合理的敏感性代码本下，已发布类别发生翻转的比例；
14. `DSLApplicableRate` 与 `OutOfLanguageRate`：对完整预冻结抽样框报告多少目标能由当前 CEF／DSL 表达，不能只在“语言碰巧适用”的子集里计算 \(I_{struct}\)。

必须比较完整 version-space 门与 `single-best transport` 消融在**同一有效输出覆盖率**下的假阳性率。如果 WIT-VS 只是通过大量 unknown 提高 precision，则风险—覆盖曲线必须把代价完整展示；若在项目预注册的最低覆盖率上没有优势，不能说产品更好。\(I_{struct}\) 或 \(I_{dir\mid eligible}\) 单独都可能被过窄 DSL、过严 abstain 或过小 eligible 集合“做高”，因此只能与 `DSLApplicableRate`、`DirectionEligibleRate`、`NonidentifiableRecall`、heldout precision 和固定覆盖率误差联合解释。

Version-space 的关键正控制是：人为构造两个成本近似、fit 同样好但 heldout 方向不同的程序，完整 WIT 必须拒判，而 single-best 消融可能随 tie 规则翻转。关键负控制是：存在多个语法不同但 heldout 等价的程序时，不应仅因程序数量多就拒判；判断对象是关键预测类别是否一致，不是程序文本是否完全相同。

matched-null 的统计单位必须是完整候选池，而不是单个候选对。固定 K 时报告有限置换 p 值及其离散下限；候选池、探针或搜索预算自适应扩张时，使用事前批准的 max-statistic、alpha-spending 或 anytime-valid 方法。默认声明只到 complete-null family error，且它只支持池级存在信号、不定位具体候选；没有 subset pivotality、有效候选级条件随机化、closed testing 或对完整选择规则的 selective-inference 证书时，所有具体 bundle 都必须保持 audit-only。混合池中的强 FWER 只有在上述条件得到证明时才可声称。若空模型不满足交换性，只能把它作为压力测试，不能借名义 p 值声称家族错误率控制。

### 15.16 2×2 结构归因的分析协议

第 5.2.11 节的 \(Y_{11},Y_{10},Y_{01},Y_{00}\) 只在开发后冻结的评估集运行。每个目标采用配对随机顺序；同一基础案例的四个版本、多个 seed 和 paraphrase 都属于同一聚类单位。主要机制量为预注册的 \(\Psi_{structure}\)，\(\Lambda_{surface}\) 与 \(\Omega\) 同时报告；不能只挑最有利的一项。

必须加入以下质量门：

- 盲审者不能从固定措辞识别四个条件；
- 机制完整两臂的关键关系保持率达到预注册下限；
- 机制破坏两臂的目标关系确实被破坏，同时其他信息变化不超过界；
- 四臂自然度、长度、信息密度和可回答性达到非劣界；
- renderer、候选数、原文预算和额外解释预算完全相同；
- 技术失败保留在原条件分母中。

若版本质量门失败，2×2 实验无效，不得将 \(\Psi_{structure}\approx0\) 解释为 WIT 机制不存在，也不得将偶然正值解释为成功。若质量门通过而 \(\Psi_{structure}\) 的区间上界低于最小有意义结构效应，停止“结构承载下游价值”的当前主张。

### 15.17 自然后续见证协议

自然后续见证只使用独立测试／研究 vault，或数据主体明确同意的只读材料；不得因为研究价值高就读取其他人的真实记忆。首选真正的 prequential 队列：在结局尚未发生时提交前缀、Need Frame、探针、transport、version space、预测与时间戳 hash，并由独立盲持方在预定窗口后揭封。每个情节的时间切点、评价义务和缺失处理必须在提交时冻结。

如果只能用已有历史，Need Frame／探针构建者只能访问切点前快照，切点后文本由另一持有者隔离；算法、构建者和评审员都不能根据已知结局选择“当时的未决问题”。这一臂必须标为 retrospective temporal holdout，并与真正 prospective 结果分开报告。没有可信快照、访问日志、提交 hash 或独立盲持时，自然后续臂无效。

主要评价不要求系统猜中完整未来，而检查预冻结的局部关系义务：方向、阶段转移、边界激活或候选所填槽位是否在后续得到支持。人工标签员只能看前缀、后续和预声明义务，不看 selector 名称与预测理由。报告：

- 对全部预提交且事后适用义务的绝对见证一致率 \(W_{future}\)，技术失败和未按协议处理的 unknown 留在分母；
- 相对冻结最强自动基线的目标内预测损失改善 \(\Delta_{future}=Loss_{auto^\star}-Loss_{WIT}\)；
- 错误方向率、unknown 覆盖率和 AURC；
- 跨时间、跨人物、跨机制组与跨授权 vault 的分层结果；
- 同一模型抽取／验证与跨家族抽取／验证的差异；
- 实际后续不在 DSL 表达范围内的 `not_applicable` 比例。

阶段 0A 分别冻结绝对下限 \(w_{min}\) 与相对改善界 \(\delta^{future}_{min}\)。若自然后续在锁定确认中升级为共同主要终点，必须同时通过两者；高绝对一致率不能替代相对基线增益，低基线带来的大相对改善也不能替代最低可靠度。观察性后续存在未观测环境变化，故只能称独立时间留出见证。没有自然后续时，可以用人工 gold 或严格随机化 null 代替，但不能把同一模型生成的改写称为等强证据。

### 15.18 自动选择接近人工 oracle 的程度

现有 60 例开发性报告支持“人工 oracle 候选相对匹配干扰候选可能有价值”，但仍需按第 1 节所述复核。为了直接回答自动选择是否缩小这一落差，在固定 renderer 的下游评价中报告描述性 oracle recovery ratio：

\[
\rho_{oracle}=
\frac{\theta_{auto}-\theta_{matched\_distractor}}
{\theta_{human\_oracle}-\theta_{matched\_distractor}}
\]

分子、分母均按目标配对，区间用目标／基础事件聚类 bootstrap。若 oracle 与 distractor 的确认集分母效应不为正或区间过宽，\(\rho_{oracle}\) 不可解释；不得裁剪到 \([0,1]\)，因为自动方法可能比 distractor 更差或偶然超过当前人工 oracle。主报告同时给三项原始 \(\theta\) 与差值，不能只给比率。

\(\rho_{oracle}\) 是产品可解释的次要量，不替换第 15.7 节选择器主终点，也不替换 2×2 结构归因门。一个方法可能排序结构正确但 renderer 下帮助有限，也可能下游变好却未通过结构机制门；两种结果必须分开解释。

## 16. 证伪与停止标准

停止原因必须分成三类，不能把算法失败、数据失效和产品风险混为一谈。

新版不允许用“方案有理论价值”替代数字。DSR-CT 阶段 0D 和锁定确认使用少数核心门；旧阶段 0S 只保留为基线协议。阶段 0D 失败时不进入完整 WIT 或产品开发。

### 16.0D DSR-CT 的 Stage 0D 与锁定 go／no-go

本节覆盖旧 16.0S。DSR-CT 的核心成功门只保留少数直接问题；其余检查归为协议有效性或安全 veto，避免几十个低功效正门共同制造 `unknown` 泛滥。

#### 16.0D.1 Stage 0D 可行性门

必须同时满足：

1. 结果盲全库 `Recall@64` 点估计至少 0.80，event-cluster bootstrap 95% 下界至少 0.65；
2. 60 个 query 中至少 20 个能形成第 5.1A.3 节的 `minimal_three_cell_panel`；其中至少 6 个能形成 analogue、bridge、foil、null 各不少于 1 个且总计不少于 4 个事件簇的 `full_four_cell_panel`；
3. 结果前 span provenance 完整率至少 95%，轴／cutoff／cell assignment 独立一致率至少 0.75；
4. 结果 canary、ACL 越权、proxy 字段、outcome permutation hash 漂移、跨桶副本泄漏均为 0；
5. 至少能构造一个不读取 query 的强结果预测器、一个 material rival set 和可审计 rolling-origin 目标子集。

第 1–3 项达不到为数据／召回 no-go：只允许继续证据密度或召回研究，不实现上层 selector。第 4 项任一非零为实验无效并立即停止。第 5 项失败表示当前数据无法识别方法差异，不得用弱稻草人继续。

#### 16.0D.2 锁定确认的四个核心门

在独立 confirmation 上冻结以下四门：

1. **自动筛选**：在冻结 \(c^*\) 上，唯一主效应 \(\Delta_{PF}\) 点估计至少 +0.10 且单侧 95% 下界大于 0；DSR 自身 `Precision_useful-far` 单侧 95% 下界至少 0.80 是独立安全 floor。发布样本数由预注册功效／精度模拟决定，30 只可作为 feasibility floor。
2. **观察性对照特异性预测**：\(H_C\) 在 analogue+bridge 上对每个 material rival、query-free 强基线和 query-aware-no-discovery 对照的 paired proper-loss advantage 下界均大于 0，且 bridge-only 的 \(G_B\) 下界独立越过冻结实质界；结果盲 applicability 的 surface-foil FPR 相对最强基线至少下降 25%，mechanism-bridge recall 的非劣下界不差于 5 个百分点；foil／null 的预提交边界失效、零变化或反转签名达到冻结绝对准确度／校准门，并按 `no_relation/zero_change/base_rate_matched` 分层报告。该门不构成因果机制证明。
3. **假发布控制**：在独立 matched-null query pools 上，以第 15.0D.5 节 \(Z_q^{DSR}\) 定义的**query／pool 级** `any release_eligible` 为单位，其单侧 95% Clopper–Pearson 上界低于 5%。将自然 query calibration 得到的同一个冻结 release threshold 原样应用到 null pools，绝不能在 null 子集内部重新取 top-\(c^*\)；零失败时 59 只是单侧独立 Bernoulli 的数学下限，预注册若采用双侧 95% 门可至少取 72，存在聚类时按 cluster 重算并扩大样本。
4. **目标价值与历史迁移**：历史 rolling-origin 对 strongest target baseline 的 proper-score advantage 下界必须大于 0，才有产品资格；独立人工盲评中，DSR 候选相对 `no-memory` 与等长干扰的 usefulness 差单侧 95% 下界都大于 0。真实 future shadow 只在额外主张 prospective transport 时要求，并且只更新后续版本。

第 1 门中的 \(\Delta_{PF}\) 是唯一自动筛选主效应，绝对 precision 是安全 floor；第 2 门是观察性机制特异性预测门；第 3 门是发布安全门；第 4 门防止“来源预测漂亮但对当前任务无帮助”。只有四门同时通过才能进入产品决策。来源侧通过、目标侧历史迁移或帮助度未通过时，只允许发表来源方法负／部分结果，不得称为可用 Spark。

#### 16.0D.3 立即 no-go 或强制降级

出现任一情况立即停止完整 DSR-CT 或降级为明确研究状态：

- Spark-Gold 经过独立盲审、裁决和事件簇折叠后不足 12 个严格关系级正例：停止小样本 CMI，不制作硬负例、不以边界例补数；这属于数据充分性 no-go，不冒充算法效果结论；
- 硬负例的自然度盲检或单轴审查未通过：当前 CMI 无效，不能评价 Spark；
- 2026-08-03 前瞻补批虽把模型筛选开发 roster 扩至 18，但 54 个官方自然 option 无合格 foil、仅 8 个单次合成候选且自然度盲检 `0/8` 通过；当前实测因此停在第二条 Hard-Negative no-go，CMI 调用为 0；
- 随后的 HN-F0 全新 authored-benchmark 供给门把严格开发正例扩为 26，并从 674 个独立成文 H 节点机械承诺 208 条边；但双 singleton 语言门只保留 45 条边、11 个唯一 T 和 42 个唯一 H，故 T/H 唯一匹配总上界 `11<12`，触发 `STOP_HNF0_LANGUAGE_ELIGIBLE_T_UPPER_BOUND_LT_12`。单轴评审与 CMI 均为 0；该 `authored_benchmark` 停止不能外推为 `natural_observed` 失败或 Spark 效果结论；
- 真结构候选与等质硬负例的逐案例改善方向相近：只能解释为额外上下文、提示或文本质量效应，停止结构机制主张；
- 12–20 例 CMI 中真候选没有在大多数案例里同时优于两类对照，存在预注册数量的严重退化，或只有汇总 p 值好看而逐案例方向不一致：停止比较器工程并检查 gold／renderer；
- 小样本方向门通过也只允许进入使用全新案例的预注册确认，不得直接进入产品、公开 shadow 或“已证实”表述；
- 独立验证集上的效应消失，或效应只存在于提出机制的 seed 自身；
- 需要看结果后修改 MechanismCard、mapping、axis、rival、cell assignment、窗口或阈值；
- 自然 validation panel 覆盖率低于 10%，或只有 LLM 合成四元对照才能通过；
- 跨模型家族／独立实现出现实质方向反转，且无法由预注册分层解释；
- Need-Path probes 只增加噪声，WitnessedTCA 不优于静态 BlindCEF，或主动 probe 不优于冻结随机／分层选择；对应组件删除；
- TCA-SIR、PGR、CANA、structured analogies、simple CBR、CMI、SOS-PAR 或它们的冻结复合臂在同预算、同覆盖下，使 \(\Delta_m\) 的区间上界低于预注册实质增益 \(\delta_{min}\)；DSR 未证明实质增量，成本更高时采用简单方法；等效声明另按冻结 \(\epsilon_{eq}\) 检验；
- precision 提升完全来自覆盖率跌破 \(c^*\) 或只在删掉 unknown／技术失败后出现；
- 来源侧复制与历史／未来目标迁移无关联；停止 transport 主张；
- target shadow 候选曾被显示或影响当前模型／用户，却仍被解释为自然结果复制；该 shadow receipt 无效；
- 失访／censoring 具有明显选择性且敏感性分析无法界定；目标结论降级为 `unscoreable`；
- conformal／FDR 所需交换性、有效分数或 calibration 隔离不成立，却仍声称有限样本错误率保证；撤回保证，只报经验风险；
- 任一输出变成行动建议、拒绝、permit、计划、事实真值或认知画像；产品安全 veto。

#### 16.0D.4 成功、失败与无结论

所有统计门使用三分结论：

- 区间完整越过冻结成功界：成功；
- 区间完整落在实质失败界内：失败；
- 横跨界值：无结论，扩大新数据或停止，不能反复调 prompt 后复用同一 confirmation。

任何 prompt、模型、检索规则、MechanismCard schema、rival set、统计量、coverage 或阈值变化都视为新方法版本，当前 confirmation 变为 development data；必须使用新的时间块／事件簇确认。选择后方法只有在其严格假设成立时才能给保证，否则结论明确限定为经验性。

### 16.0S SOS-PAR 的 Stage 0S 与锁定 go／no-go（冻结基线）

#### 16.0.1 Stage 0S：文本与防火墙可行性

只有同时满足下列条件，才值得运行效果实验：

- 在固定 60 个真实 query 中，至少 18 个能形成严格来源对照并拥有可评估候选池；
- strict bundle 的人工精确率至少 85%；
- stable-unit、时间顺序、transport 和结果方向的逐字段 \(\kappa\) 或 \(\alpha\) 均不低于 0.75；
- 原文结果 span／provenance 精确率至少 90%；
- 至少 30% query 能冻结一对在同一探针上有实质分歧的 \(H_F/H_R\)；
- outcome permutation 后 pool、top-k、mapping、probe 和 prediction hash 100% 不变；
- 预揭封 canary 读取、结果 ACL 越权和跨臂字段漂移均为 0。

任一防火墙访问、canary 命中或 commitment 漂移都不是“一个失败样本”，而是整轮实验无效。若严格单元不足 18/60、标注不稳定或可辨竞争机制覆盖不足，就停止 SOS-PAR 主张；可以先改善数据采集与结果分段，但不能用合成的确定性标签替代真实可行性。

#### 16.0.2 锁定积极门

在新建封存集上，完整 SOS-PAR 只有同时满足以下条件，才取得申请下一离线阶段的资格；它仍不直接授权 shadow：

1. legacy 复现终点 \(\widehat\Delta_{selection}\ge0.03\)，且 query／来源事件 cluster-bootstrap 95% CI 下界 \(>0\)；该项不替代第 16.0D 节；
2. `Recall@8` 的单侧 95% 下界至少 0.85；若要主张结果盲检索本身有创新增量，其相对最强结果盲召回基线的配对增量下界还须至少 0.10；
3. 真远 transport 对表层假朋友的 PairAUC 单侧 95% 下界至少 0.75，且相对阶段 1S 获准的 `WIT-min consensus` 配对增量下界至少 0.08；
4. 只在 `localized_release_status=valid`（即 `row.passed` 且定位程序／证书有效）的候选上，localized-candidate audit precision 单侧 95% 下界至少 0.85；以“每个 query 至少一个该状态候选”定义的 query coverage 下界至少 0.30。若只有 pool-level 信号，本项自动不通过且不得计算候选证书精度；
5. 在独立 null query pool 上，以第 15.0.5 节 \(Z_q^{any}\) 定义的 query-level matched-null 合并失败率，其双侧 95% Clopper–Pearson 上界不超过 0.05；同时诊断报告 \(Z_q^{pool}\) 与 \(Z_q^{candidate}\)。若零次 \(Z^{any}=1\) 时要达到该门，至少需要 72 个相互独立的 null query pool，候选数不能替代 query 数；
6. null／空候选 query 的正确 abstain 单侧 95% 下界至少 0.90；
7. 完整 2×2 的 \(\Delta_{proper\mid long}\) 与 \(\Delta_{long\mid proper}\) 的单侧 95% 下界都越过各自预冻结实质界；否则只保留通过的单组件；只有 \(\Gamma_{SOS}^{int}\) 的下界也越过预冻结交互界时才宣称协同交互；
8. 以阶段 0S 冻结的人工盲评灵感效用量表归一化到 \([0,1]\)，预注册非劣界 \(m_{NI}=0.02\)；令 \(d_{utility}=U_{SOS}-U_{baseline}\)，只有其单侧 95% 下界 \(>-0.02\) 才算不劣。query-side 机制判断另作次要指标，不能在看到结果后替换本门；
9. 未支持的目标因果断言、行为决策、拒绝／permit 触发和记忆写回均为 0；
10. `inspiration=false` 的现有路径、返回结构和错误语义逐项不变。

阈值、区间算法、聚类单位、单／双侧方向和多重性控制必须在封存结果可见前冻结，并用功效模拟确认可识别性。统计积极门家族只包含第 1–4、6–8 项：它们使用 query／来源事件聚类的单侧区间，并通过预冻结 step-down maxT 或等价程序控制家族错误，不能逐项使用未校正 95% 区间后再取全过。第 5 项单独按独立 null query pool 的 \(Z^{any}\) 使用精确 Clopper–Pearson 上界；第 9–10 项是逐次确定性安全 veto，任何一次非零／不一致都硬否决，不进入置信区间或 maxT 家族。对任一“下界至少阈值”的统计门：下界达到阈值为成功，上界低于阈值为实质失败，其余为无结论；对非劣门以 \(-m_{NI}\) 为阈值同理。第 1 项保留“点估计至少 0.03 且下界大于 0”的双条件，其实质失败定义为 \(U_{\Delta_{selection}}<0.03\)。48 个锁定 signal query 只用于大效应 go／no-go；假发布率另用至少 72 个独立 null query pool。若样本只能识别更大的效应，就按其能力解释成功／实质失败／无结论，不能把未检出的小效应写成等效证明。

#### 16.0.3 明确 no-go、判重与降级规则

满足以下任一项时，停止相应主张而不是追加新模块：

- 完整盲封条件的 query 级 selection-aware 复合损失改善不优于最强冻结结果盲概率基线，或 \(\Delta_{selection}\) 未达到最小实质界；
- 结果可见正控很好、完整盲封约等于强结果盲来源预测器：旧信号主要来自后见信息；
- 以预冻结 total variation、Jensen–Shannon divergence 或期望 proper-score separation 判断，可辨 \(H_F/H_R\) 的 query 覆盖上界仍低于 30%：当前结果轴没有足够机制区分力；不能只用“众数是否相同”判定两个分布不可辨；
- 负控通过率的下界超过 10%，或真实候选相对负控通过率差的上界仍不足 10 个百分点；
- outcome-blind `Recall@8` 的上界低于绝对门，或其相对最强结果盲基线增量的上界低于预注册增量：当前文本／检索器不足；与结果可见正控的差距只描述信息上界，不能单独构成 no-go；
- localized-candidate query coverage 的上界低于 0.30，或在冻结最低覆盖率处仍无法达到精度门；pool-only 检出不得冒充候选覆盖；
- 人工盲评效用差 \(d_{utility}\) 的上界低于冻结非劣界 \(-0.02\)：不得进入产品路径；
- 加入 `WIT-slim` 后 Top-1 提升的上界仍不足 5 个百分点且 AUPRC 提升的上界仍不足 0.05：WIT 对 SOS-PAR 判重，删除在线 WIT，只保留离线审计；
- outcome sealing 相对已严格遮蔽同一结果的 WIT 没有剩余增量：SOS-PAR 只作为协议条款，不另立算法贡献；
- 任一组件只在看过结果、修改候选或揭封后重试时有效：该组件无效。

结果基率、叙事语气、文本长度、主题和重复簇的调整只作预先列出的敏感性分析，不是本版正式 no-go；它必须报告冻结协变量定义、模型式、残差诊断与调整后区间。未来若要把“调整后增益消失”升级为停止门，必须在揭封前另行冻结估计量、实质界、正则化／缺失处理和三分决策规则，不能凭一次事后回归宣判。

若纵向证据单元有效、proper-score 规则无效，保留纵向单元；若 proper-score 有效、纵向单元无增益，保留裁决规则；联合臂优于两个单组件但 \(\Gamma_{SOS}^{int}\) 未通过时，只说两个组件分别有增量，不宣称协同交互。复杂度没有“已经设计得很完整所以继续”的豁免。

### 16.1 条件 WIT／阶段 1W 的科学成功、实质失败与统计无结论

本节只在 DSR-CT 阶段 1D 来源事件外验证已通过、WIT 被保留且子协议 0A–0D 另行批准后，约束阶段 1W 及其后续确认。这里的 \(\Delta_{WIT}\)、\(\delta^{WIT}_{min}\) 指第 15.7 节 WIT selector 的目标内配对终点，**不是**第 15.0D.5 节固定覆盖率主终点；整体 go／no-go 按第 16.0D 节。每个 WIT 门都使用“成功／实质失败／无结论”三分思想，但统计方向不同，不能套用同一个不带方向的公式：

| 门禁 | 成功 | 实质失败／伤害 | 无结论 |
|---|---|---|---|
| 条件 WIT selector 锁定终点优效 \(\Delta_{WIT}\) | \(L_{\Delta_{WIT}}>\delta^{WIT}_{min}\) | \(U_{\Delta_{WIT}}<\delta^{WIT}_{min}\) | 其余 |
| WIT 相对 CUT 增量 \(\Delta_{CUT}\) | \(L_{\Delta_{CUT}}>\delta^{CUT}_{min}\) | \(U_{\Delta_{CUT}}<\delta^{CUT}_{min}\) | 其余 |
| 组合增量优效 \(\Gamma\) | \(L_\Gamma>\gamma_{min}\) | \(U_\Gamma<\gamma_{min}\) | 其余 |
| 三个结构归因组合增量 \(\Gamma^{attr}_k\) | multiplicity 调整后的同时下界均 \(>\gamma^{attr}_{min}\) | 任一上界 \(<\gamma^{attr}_{min}\)；仅撤销结构归因主张 | 其余 |
| `StructuralPairAUC` \(A_{struct}\) | \(L_{A_s}>s_{min}\) | \(U_{A_s}<s_{min}\) | 其余 |
| 四个不可互偿 \(PairRate_{a,b}\) | 四者下界分别超过 \(s^{pair}_{a,b,min}\) | 任一上界低于对应界 | 其余 |
| 近邻预测 P1：三项抗事后漂移 | 三项 multiplicity 调整后的同时下界分别 \(>m^{P1}_{atom},m^{P1}_{need},m^{P1}_{surface}\)，且 \(L_{\Delta^{NI}_{cov,need}}\ge-\varepsilon_C\) 并达到 NeedCoverage 绝对 floor | 任一必要误差差值上界低于对应界，或 \(U_{\Delta^{NI}_{cov,need}}<-\varepsilon_C\)，或绝对 floor 实质失败；P1 失败 | 其余；共同 proposer 轨道有 N/A 时不得宣称成功 |
| 近邻预测 P2：binding／rival 双门 | 两项调整后的同时下界分别 \(>m^{P2}_{binding},m^{P2}_{rival}\)，且 \(L_{\Delta^{NI}_{cov,map}}\ge-\varepsilon_C\)、\(U_{D_{cost}}\le\varepsilon_{cost}\) 并达到 mapping coverage 绝对 floor | 任一必要误差差值上界低于界，或 \(U_{\Delta^{NI}_{cov,map}}<-\varepsilon_C\)，或 \(L_{D_{cost}}>\varepsilon_{cost}\)，或绝对 floor 实质失败；P2 失败 | 其余；共同 proposer 轨道有 N/A 时不得宣称成功 |
| `CEF×version-space` 与“双承诺”两个 2×2 | 各自预注册的必要主效应、完整臂对单臂增量和交互同时越过对应实质界 | 任一拟保留部件的必要效应上界低于界：删除该部件；完整联合解释按冻结规则撤销 | 其余；与主终点共享唯一一次扩样 |
| 零候选正确 abstain／自然开放库精确率／同覆盖率误差 | 三者分别达到冻结下限／上限 | 任一实质越过失败界 | 其余 |
| CEF 输入隔离违例计数 \(N_{canary}\) | \(N_{canary}=0\) | \(N_{canary}>0\)：协议无效，停止并修复数据流 | 无统计灰区 |
| 无查询 CEF 不稳定率 \(U_{cef}\) | 上置信界 \(<u^{cef}_{max}\) | 下置信界 \(>u^{cef}_{max}\) | 其余 |
| 结构版本空间可识别率 \(I_{struct}\) | \(L_{I_s}>i^{struct}_{min}\) | \(U_{I_s}<i^{struct}_{min}\) | 其余 |
| eligible 方向可识别率 \(I_{dir\mid eligible}\) | \(L_{I_d}>i^{dir}_{min}\) | \(U_{I_d}<i^{dir}_{min}\) | 其余 |
| 方向 eligible 覆盖 \(C_{dir}\) | \(L_{C_d}>c^{dir}_{min}\) | \(U_{C_d}<c^{dir}_{min}\) | 其余 |
| DSL 适用覆盖 \(C_{dsl}\) | \(L_{C_l}>c^{dsl}_{min}\) | \(U_{C_l}<c^{dsl}_{min}\) | 其余 |
| 盲封留出结构／eligible 方向精确率 \(P^{struct}_{holdout},P^{dir}_{holdout}\) | 两者下界分别超过冻结下限 | 任一上界低于对应下限 | 其余 |
| 结构／方向竞争程序残余接受率 \(A^{struct}_{rival},A^{dir}_{rival}\) | 两者上界分别低于冻结上限 | 任一下界高于对应上限 | 其余 |
| 结构匹配空模型假阳性率 \(N^{struct}_{null}\) | \(U_{N_s}<n^{struct}_{max}\) | \(L_{N_s}>n^{struct}_{max}\) | 其余 |
| eligible 方向匹配空模型假阳性率 \(N^{dir}_{null}\) | \(U_{N_d}<n^{dir}_{max}\) | \(L_{N_d}>n^{dir}_{max}\) | 其余；仅约束 signed 主张 |
| 合理代码本类别翻转率 \(F_{code}\) | \(U_F<f^{code}_{max}\) | \(L_F>f^{code}_{max}\) | 其余 |
| 合理 \(\epsilon\) 结构／eligible 方向翻转率 \(F^\epsilon_s,F^\epsilon_d\) | 两者均为 0，或最大包络本身保持已知共识 | 任一候选只在缩小 \(\epsilon\) 后才能发布 | 无统计灰区；相关主张直接 unknown |
| Need Frame 增量价值 \(V_{need}\) | \(L_V>v_{min}\) | \(U_V<v_{min}\) | 其余 |
| SignedOrientationGate \(O=\min(BA_{map},BA_{dir})\) | \(L_O>\eta^{orientation}_{min}\) | \(U_O<\eta^{orientation}_{min}\) | 其余 |
| mapping／aligned／opposed／mixed 类 recall 与 SignedOutputCoverage \(R_j\) | \(L_{R_j}>r_j\) | \(U_{R_j}<r_j\) | 其余 |
| 表层换皮、坐标重参数化、真实全局／单点翻转、证据删除、结构破坏或无证据补全错误率 \(G_j\) | \(U_{G_j}<g^{err}_j\) | \(L_{G_j}>g^{err}_j\) | 其余 |
| 结构破坏敏感性 \(B\) | \(L_B>b_{min}\) | \(U_B<b_{min}\) | 其余 |
| 结构保持非劣损失 \(D_{preserve}\) | \(U_D<\epsilon_{preserve}\) | \(L_D>\epsilon_{preserve}\) | 其余 |
| 安全伤害增量 \(H_j\) | \(U_{H_j}<\epsilon^{safe}_j\) | \(L_{H_j}>\epsilon^{safe}_j\) 或越过绝对 veto | 其余 |
| coverage／可用率 \(C_j\) | \(L_{C_j}>c_j\) | \(U_{C_j}<c_j\) | 其余 |
| 证据精确率、SupportedFieldRecall、UnknownRecall \(E_j\) | \(L_{E_j}>e_j\) | \(U_{E_j}<e_j\) | 其余 |
| response-direction 混淆率或共同假阳性率 \(F_j\) | \(U_{F_j}<f_j\) | \(L_{F_j}>f_j\) | 其余 |
| 2×2 结构主效应 \(\Psi_{structure}\) | \(L_\Psi>\psi_{min}\) | \(U_\Psi<\psi_{min}\) | 其余 |
| 2×2 表层依赖／交互 \(\Omega\) | 达到预注册的可移植性或分层异质性规则 | 违反冻结可移植性界；不得用显著交互替代 \(\Psi_{structure}\) | 其余 |
| 自然后续绝对见证一致率 \(W_{future}\) | \(L_W>w_{min}\) | \(U_W<w_{min}\) | 其余 |
| 自然后续相对最强基线损失改善 \(\Delta_{future}\) | \(L_{\Delta_f}>\delta^{future}_{min}\) | \(U_{\Delta_f}<\delta^{future}_{min}\) | 其余 |
| 预算超支 \(K_{cost}=Cost_{WIT}-Budget_{WIT}\) | 确定成本时 \(K_{cost}\le0\)；随机成本时 \(U_K\le0\) | 确定成本时 \(K_{cost}>0\)；随机成本时 \(L_K>0\) | 确定成本无灰区；随机成本其余为无结论 |
| CMI 成本优势 \(A_{cost}=Cost_{WIT}-Cost_{CMI}\) | 仅用于“CMI 全面占优”：确定成本时 \(A_{cost}>\kappa_{min}\)，随机成本时 \(L_A>\kappa_{min}\) | 确定成本时 \(A_{cost}\le\kappa_{min}\)；随机成本时 \(U_A<\kappa_{min}\)：不得以成本宣称 CMI 全面占优 | 随机成本其余；不影响 WIT 科学成功判定 |

等号落在哪一侧、使用单侧还是双侧区间、聚类 bootstrap 或其他区间方法，必须在阶段 0A／0D 按各自职责冻结。若成本由冻结价格与精确调用数完全决定，就直接对精确值判门，不人为制造置信区间；只有随机延迟或重复成本才使用上表区间。coverage 首先必须通过绝对比例下限；P1／P2 还另外要求相对近邻的配对非劣，二者都不是 coverage 优效主张。安全通常是非劣或伤害 veto，也不能用“没有显著变坏”冒充安全。上表的证据与可用率 CI 门只用于未来更大独立范围确认；固定 18 条的阶段 0B 明显不可行筛查只用第 6.5 节预注册的绝对计数／点估计门，CI 仅描述，不能跨两套规则择优。

`solver_optimality_gap`、CEF 实际 payload／源哈希一致性、claim 最低 specificity／Need 覆盖、DSL 合法性、最大 \(\epsilon\) 包络和盲封时间戳不是靠统计平均“基本通过”的软指标，而是逐候选有效性条件：求解器未证明达到冻结的最优性间隙、证据形在查询／候选出现后改变、claim 退成平凡交集、程序使用禁用操作，或 heldout 提前泄漏，该候选直接返回 `unknown`，并计入可用率与失败成本。不得让大量简单样本稀释一次协议越界。

WIT 的成功条件必须按主张分层。**Unsigned structural WIT** 至少同时满足：CEF 隔离／稳定性、checker、claim／\(\epsilon\)／代码本、结构可识别与 DSL 覆盖、结构 heldout／rival／null、选择器主终点、一般 selector 的 `StructuralPairAUC`／四分项／零候选／开放库门、2×2 结构归因及全部安全 veto。**Signed WIT** 只能在前述条件之上，再满足方向 eligible 覆盖、方向可识别、方向 heldout／rival／null、SignedOrientationGate 与逐类方向门；锁定 signed 主张和产品可见方向还必须通过一次性 `external_challenge`，`mixed` 逐 block 通过。方向失败不得回写成结构失败。Need Frame 增量价值和自然后续见证可在首次离线开发中作为关键次要终点；一旦进入锁定确认，它们是否升级为共同主要终点必须在揭封前写死。

这里的“成功”只指条件 WIT 分支的锁定确认成功，其中 WIT selector 终点优效界明确就是 \(\delta^{WIT}_{min}\)。阶段 1W 的 \(\widehat\Delta_{WIT}\ge\delta^{WIT}_{min}\) 且 \(L_{\Delta_{WIT}}>0\) 只表示该 WIT 分支值得继续开发，不能写成 SOS-PAR 主终点已通过或已确认达到产品意义。

“没有显著越过优效界”不能自动写成已经证伪。无结论与实质失败都不得包装成产品通过或正面科学结论，但结果仍应完整记录并分别表述。追加样本只允许按事前冻结的序贯设计或 alpha-spending 规则执行。完整平衡四联体、结构破坏对照、最强自动基线、固定覆盖与预算及独立复制均按上表中与其构念对应的方向判定；只有满足预定义实质失败条件时才否定当前冻结版本，区间过宽时归为无结论。

单一探索性压力样本失败只记录边界，不自动证伪总体。确认主要终点、关键机制对照和复制分别报告，不能用一个成功结果掩盖另一个失败或无结论结果。

### 16.2 实验无效或暂停

以下情况不能被解释为算法成功或失败，应暂停并修复实验：

- 核心结构维度的人工一致性低于冻结门槛；
- 集合间存在人物、vault、基础事件、来源记忆或机制组泄漏；
- 标签员、候选构造者或选择器接触了不该看到的标签或外部挑战探针；
- \(\sigma_U\)、\(\sigma_O\) 或 mixed 分组在响应揭封后才确定，或同一轴逐探针改变正方向；
- CEF、角色／机制轴、Transport DSL、编码长度或允许的删减规则在看到查询候选、人工标签或 heldout 结果后被改写；
- 近优版本空间因启发式截断而漏掉方向相反的合法程序，却仍被报告为共识；
- `internal_fit`、`internal_audit_heldout`、`external_challenge` 或自然后续见证之间发生内容、同源事件、模板或时间泄漏；
- 经隔离人工或受控程序审计后发现对照构造本身失真、标签错误、同时改变多个非目标因素，或 metamorphic sibling 泄漏跨集合；
- API 异常、模型版本漂移或缺失率超过预定义上限；
- 结构破坏文本明显不自然或改变了多个非目标属性；
- 预注册代码、数据哈希或随机化记录无法复现。

若对照构造已经被独立验证为有效，而算法不能保持表层换皮／坐标重参数化不变性、不能随真实响应翻转作预期变化、删除证据后仍强判，或出现其他预注册 metamorphic 失败，这不是“实验无效”，而是有效的算法错误：必须计入对应 \(G_j\)，并按第 16.1 节判断当前冻结版本成功、实质失败或无结论。

对照有效性裁决必须由看不到算法预测的独立人员或受控程序在输出揭封前完成。协议预先冻结候补对照池、替换顺序、每类最大允许无效率和超限后整场暂停条件，不能在看到失败后选择性宣布困难样本无效。若结果揭封后才发现构造问题，原分析和按冻结规则排除／替换后的敏感性分析必须并列报告；该已揭封样本不得静默删除或换到确认集。

### 16.3 产品安全 veto

即使主要效应成立，出现以下任一项也禁止用户可见发布：

- 提示注入能越过数据/指令隔离；
- 错误引用、跨用户泄漏或真实记忆被测试流程改写；
- 输出被下游当作行为许可、拒绝指令、规范性权重或记忆真源；
- Spark 关闭或 `inspiration=false` 时仍改变正常路径；
- 延迟、成本、错误率或无关记忆侵入超过冻结安全上限。

确认集原则上不做中期查看；若协议允许，必须提前规定 alpha spending。确认失败后不得回看同一确认集调参；任何方法修改都需要新的确认集。

只有完成以下链条，才可以讨论公开产品化：

~~~text
开发集支持继续
→ 冻结算法、提示、阈值、模型和排除规则
→ 全新锁定确认集通过
→ 随机、提示和结构破坏对照达到预注册优效／安全非劣界
→ 三模型分层结果达到预注册异质性规则
→ 独立复制通过
→ 再决定是否进入用户可见 Spark
~~~

在此之前只能称为 shadow selector。

## 17. 主要失败风险与缓解

### 17.1 循环验证

风险：同一个模型生成探针、提出目标响应假设并验证候选，可能自我确认。同一模型家族的多个 seed 还可能稳定地重复同一种因果补全、否定忽略、角色错配或抽象模板偏差，形成看似稳定的系统性共病。

缓解：

- 候选出现前盲封；
- 提出者、候选回答者和比较者分离；
- 多 seed 只用于测量家族内随机稳定性；
- 使用至少两个跨家族端到端结果和交叉角色消融检查共同假阳性；
- 映射器和候选回答者看不到冻结目标响应；
- 算法内部审计池与外部挑战池隔离；
- 有受控依据的案例由独立人工或程序确认。

若跨家族共同接受人工负例的比例过高，应视为验证框架失效，而不是把“多模型一致”当作更高置信。

### 17.2 幻想因果

风险：文本没有写出的因果被模型补全，两边恰好补成相同故事。

缓解：

- 强制原文证据；
- 证据等级；
- unknown；
- 弱证据不能单独通过；
- 输出称为证据约束的局部响应假设，不称为真实因果证明。

### 17.3 抽象层作弊

风险：不断提高抽象程度，最终任何两件事都可以类比。

缓解：

- 预定义梯度范围；
- 只允许预定义相邻离散层，并明确不嵌套时逐层报告；
- 查询侧先冻结允许层；
- 查询表示复杂度、候选映射复杂度、Genericity、证据覆盖和 unknown 联合约束；
- 算法内部审计扰动和独立外部挑战探针；
- 空泛探针低信息权重。

### 17.4 过强双向要求

风险：来源完整、目标未完成，优秀灵感天然不对称。

缓解：

- 只检查局部映射一致；
- 不要求事实数量相同；
- 不要求推断完全可逆；
- 仅允许把 `speculative` 保留为 legacy audit-only 标签，且强制 `context_injection_allowed=false`；
- 输出非权威的局部对应假设。

### 17.5 破坏测试误杀冗余机制

风险：真实系统具有补偿路径，删除一个因素后结果不变。

缓解：

- 区分必要探针与诊断探针；
- 只有证据充分的必要机制冲突才硬拒；
- 保留替代机制；
- 多探针联合而非单探针裁决。

### 17.6 稀有灵感被多源确认压制

风险：个人记忆库中只有一个真正远类比。

缓解：

- 把 `seed_only_unverified` 的数量、人工 oracle 价值和被自动门漏掉的稀有灵感作为独立 recall 代价报告；
- 允许授权研究人员在**不注入当前模型**的离线审计中查看 singleton，帮助改进未来数据与召回；
- DSR 自动可见路径仍不允许 singleton／`speculative` 绕过事件外验证；若业务必须展示，另立非 DSR 功能与风险协议，不能降低 DSR 定义来迁就覆盖率。

### 17.7 成本过高

风险：候选 × 探针 × 模型 × seed 导致调用量过大。

缓解：

- 只在明确请求时触发；
- 宽召回后只验证少量候选；
- 主动信息增益；
- 批处理；
- 用可证明不可能进入任一 codebook／completion 对应 \(\mathcal V_{release}\) 的下界做分支剪枝；
- 缓存通用派生视图；
- 求解器超时、最优性间隙未闭合或版本空间枚举不完整时返回 `unknown`，不能拿当前 best-so-far 冒充稳健共识；
- 预算耗尽返回空。

### 17.8 单一最佳映射造成的过早确定

风险：两个或更多几乎同样好的角色映射可能给出相反方向。只保留 argmin 会把求解器的任意 tie-break 伪装成认知结论；多 seed 对同一提示达成一致也不能排除这一点。

缓解：

- 保留冻结 \(\epsilon\) 内的全部不同预测等价类，而不是只保存一个程序文本；
- 在所有合法证据补全和近优映射上计算每成员 \(\mathcal D^{fit}\cap\mathcal D^{hold}\)；
- 只有每成员交集都是同一个完整方向 key，并通过方向 rival 与 candidate-level null 时才发布该方向；
- 主动寻找能产生相反 heldout 预测的 rival program，并随 Witness Bundle 返回最强反例；
- 若结构 rival 无法排除，返回 `mapping_status=unknown`；若只有方向 rival 无法排除，保留结构并返回 `direction_class=unknown`。二者分别记录 `mapping_ambiguity`／`direction_nonidentifiable`，不得混写。

### 17.9 查询污染证据形

风险：先看到“需要什么灵感”，再从记忆中抽取恰好匹配的角色和机制，会让结构相似性成为提示工程的产物。即使随后盲封候选响应，也已经发生了上游泄漏。

缓解：

- 每条记忆先独立编译为查询无关 CEF，并绑定原文 span、源哈希、抽取器版本和时间戳；
- Need Frame 与候选 CEF 分离持有，映射阶段只允许有限 DSL 对已提交节点做绑定、对齐、粗化或可选删减；
- 禁止为当前查询新造边、翻转响应、删除 required 证据或事后追加隐变量；
- 用进程／服务级输入白名单、访问日志和 paired query-canary 检查 extractor 是否真的看不到 query；源文本、seed、版本与可见输入完全相同时，canary 改变却导致 CEF 变化就是协议违例；
- 另在始终无 query 的相同源文本上按冻结 seed 重复抽取，测 `CEFNoQueryInstability`；该波动来自抽取随机性／模型不稳定，不能计成 query 泄漏，也不能通过挑最有利重复解决。

### 17.10 DSL 与编码体系把偏见藏进形式化

风险：有限语法、代码长度和抽象层级本身就是归纳偏置。若设计者可随结果调节操作成本，MDL 或 transport objective 只会把人的偏好数学化。

缓解：

- 在任何正式样本结果可见前冻结 DSL、操作语义、编码表、required/optional 规则与版本号；
- 报告至少两个事前合理代码本的敏感性分析，但只指定一个为确认主规格；
- 做 `remove-one-op`、操作成本扰动和跨领域迁移消融；
- 如果结论随合理代码本翻转，令相关结构／方向状态为 `unknown` 并记录 `reason_codes=["codebook_sensitivity"]`，不能择优报告。

### 17.11 组合求解与版本空间爆炸

风险：部分图匹配、有限程序合成和 rival search 通常具有组合复杂度；为了在线延迟偷偷减少搜索，会优先漏掉难找的反向映射。

缓解：

- 首轮只做小图、低 treewidth、有限分支和离线 exact／symbolic solver；
- 保存下界、上界、最优性间隙、探索节点数、超时和剪枝证书；
- 对可枚举微型实例穷举核对求解器；
- 明确区分“未找到反例”和“证明不存在反例”；
- 在 exact 版本证明有效前，不把神经近似器或 LLM 生成程序当作规范性判定器。

### 17.12 自然后续见证仍可能有混杂

风险：记忆形成后的真实后续结果是最强的反自证证据之一，但环境改变、选择性记录和自我实现行为可能让结果不再对应原始机制。

缓解：

- 冻结只使用时间上严格晚于 CEF 与预测提交的事件；
- 将“未来观察一致”写为预测证据，不写成因果证明；
- 对可观察的环境改变、额外干预与记录缺失分层；
- 同时报原始一致率、可审计子集和缺失机制敏感性；
- 自然后续见证不能补救 CEF 泄漏、heldout 失败或安全 veto。

### 17.13 首创性被过度宣称

风险：结构映射、MDL、图编辑距离、Gromov–Wasserstein、主动探针、因果记忆干预、选择性预测和反事实稳健性分别都有直接先例。把这些组成项任一写成“世界首次”会削弱整个研究。

缓解：

- 只把可检验的完整联合协议作为暂定贡献对象；
- 逐项列出最近相邻工作并实现公平基线；
- 公开检索结论只使用“截至检索日，本轮非系统检索覆盖的一手来源中尚未定位到所述两层完整协议”，不使用“世界首创”或“已证明无专利”；
- 在投稿、公开或申请知识产权前，由信息检索／专利专业人员做独立系统检索。

### 17.14 来源结果通过代理字段泄漏

风险：即使删除结果段，全文摘要、标题、标签、重要性分数、关系边、后续对话、supersedes 链和看过全文生成的 embedding 仍可能压缩真实结果。检索器会“正确”找到候选，但正确性来自后见信息。

缓解：严格索引从结果前原文重新构建；所有派生字段进入 taint DAG；结果置换后要求 pool／top-k／mapping／prediction hash 逐字节不变；结果 canary 的预揭封读取必须为 0。任何代理泄漏使整轮实验无效。

### 17.15 揭封后选择、多重比较与只报成功者

风险：在大量候选、机制、探针和方向中只报告命中的组合，会使一个低信息三分类结果看起来很有区分力。揭封后补候选、改轴、改 rival 或重试都属于选择性分析。

缓解：固定 top-k、一个探针和一个命名 rival；提交所有概率分布、唯一先验混合与顺序后一次揭封；全部失败保留分母；双向诊断按 candidate×mechanism/winner-sign×probe 的完整选择规则使用有效 max-statistic 或预注册层级检验；同时报告 precision–coverage、AURC 和 matched-null 假发布率。

### 17.16 单个来源结果被误当作因果或目标机制证明

风险：个人行动通常是内生选择，记忆具有选择性记录和回忆偏差，行动与结果之间还可能有未记录共干预。即使焦点机制准确预测来源结果，也只提供来源内一致性见证，跨域 transport 仍是额外假设。

缓解：冻结结果轴和观察窗，保留零／相反结果与混杂；按 T1／T2／T3 分层；优先重复自对照但不称 N-of-1 因果效应；输出固定警示，不允许自动产生目标因果、行动、拒绝或 permit 主张。

### 17.17 机制没有真正分歧，proper score 只奖励基础率套话

风险：`压力通常不好`、`合作通常有益` 等高基础率常识可让所有机制给出近似分布。即使某机制 Brier 略低，也没有类比诊断价值。

缓解：要求 \(H_F/H_R\) 在同一冻结探针上达到最小分布分离；同时越过相对 rival 门与绝对基率门；超过 70% 候选 modal outcome 相同则停止当前结果轴；加入 action shuffle、方向翻转和无机制 query。

### 17.18 参数记忆、共享模型与系统性共病

风险：公开事件可能已存在于模型参数中；同一模型家族的多 seed 共享训练分布和偏差，不能当作独立见证。提议者、映射者和评分者即使角色分离，也可能共同复现同一种捷径。

缓解：确认集优先使用模型未见过的经授权私人新情节或前瞻材料；按来源事件去重；至少两个架构／训练路线明显不同的模型家族端到端复现，锁定确认再加入第三家族；分家族报告共同假阳性，不能用多数投票掩盖系统性错误。

### 17.19 高精度来自覆盖率塌缩

风险：严格门天然会降低覆盖率。系统可以通过只发布极少数容易案例获得高精度，却对真实用户几乎无用；旧固定空槽最大损失又可能反向奖励用低信息均匀分布填满槽位。

缓解：在 calibration 前冻结最低 coverage \(c^*\)，确认阶段未达到即不合格；同时报告完整 risk–coverage、AURC、空结果、失败率和真实开放库覆盖。合理 abstention 不记最大损失，但也不能靠全空赢得主终点。

### 17.20 发现 seed 自证与“灵感被协议消灭”的双重风险

若机制必须在 seed 出现前冻结，系统无法提出真正的新机制；若 seed 同时负责生成和验证，结果盲封仍不能排除高自由度单事件拟合。缓解是 DSR 的硬隔离：seed 可生成、不能确认；确认只用独立 event clusters。任何同事件 prefix、近重复、跨桶副本或 supersedes 链进入 validation，都使候选确认无效。

### 17.21 目标 future shadow 变成 post-treatment observation

一旦候选、预测或诊断被展示给当前模型或用户，它就可能改变后续行为与记录；此时未来结果不能再解释为自然迁移复制。真实 shadow 必须 `never_shown`。可见臂只能用于产品效用实验，不能与自然 shadow 合并；当前调用永远不能用尚未出现的结果决定本次输出。

### 17.22 失访、删失与观察窗漂移

未来结果缺失往往不是随机的：失败经历可能不再记录，成功经历可能被更多讨论，观察窗也会随任务变化。必须预冻结 censoring rule，单独报告 `target_unscoreable`、失访率与缺失敏感性分析；缺失不能记作零、失败或被从分母静默删除。无法界定选择偏差时停止目标结论。

### 17.23 合成四元组制造伪捷径

LLM 生成的 foil、role swap 或 outcome swap 可能带有文体、长度、否定词或模板痕迹，使 selector 学会识别生成器而不是机制。合成样本只允许 `metamorphic_only`；真实通过证据必须来自 source-hash 可追溯的自然事件，并由看不到结果与模型胜负的独立审核者分配 cell。若只有合成对照才有效，核心假设失败。硬负例制作者的自检不能替代盲检；若评审或系统能够仅凭自然度、文体或明显编辑痕迹识别哪条是负例，则由这些样本得到的“高精度”一律按 shortcut 处理，当前实验无效，不能归因为结构识别。

### 17.24 选择后校准被名称替代

Conformal、maxT、e-BH 或 Bayes factor 不是给任意 LLM 分数贴标签就成立。自适应检索、个人时间序列和多候选选择通常破坏交换性；LLM 自报概率不是有效 e-value。必须明确 discovery／calibration／confirmation 三分、nonconformity score、cluster 单位和漂移假设；不满足时只报告经验 risk–coverage，不宣称有限样本 FDR／FCR／coverage 保证。

### 17.25 独立实现彩票与同模型共病

同模型不同 seed 共享训练分布与系统性捷径，不能算独立复制。至少使用跨家族提议者／映射者、确定性 scorer、独立 span 审核和新时间块角色交换，并报告 leave-one-implementation-out winner reversal。若方法赢家依赖某一实现或跨家族方向反转，结论降级为实现特定，不进入产品。

### 17.26 多切点伪独立与主动揭封过拟合

同一事件多个切点高度相关，不能把样本量从 1 人为膨胀到 \(m\)。切点、窗口和汇总函数必须结果盲冻结，并按事件簇联合评分。若主动选择揭封 probe，必须保留 untouched final holdout 并做选择后校正；边揭边改机制或用同一 probe 同时选择与确认均无效。

## 18. 分阶段实施建议

本文即使获得研究方向批准，也不直接授权完整实现。当前可讨论的首轮工作只到阶段 0D-A；任何临时防火墙 harness、真实结果揭封、production shadow 或产品接入都必须由上一门禁通过并由项目所有者再次批准。新顺序先验证“真实个人记忆能否支持 discovery／validation 隔离和自然反证复制”，再决定是否投入训练、主动层析或 WIT。

### 阶段 0D-A：只读 DSR 数据与召回可行性（当前唯一首轮）

- 对现有 60 query 只读建立 event-cluster、cutoff、BlindSourceLedger 可行性清单；
- 统计 discovery seed、独立 analogue／bridge／foil／null、真实多切点 probe 和历史 rolling-origin endpoint 密度；
- 在不读取封存结果的冻结全库快照上审计 `Recall@16/32/64`、Need-Path 增量、无关候选增量和 event-cluster diversity；
- 随机 15–20 条真实记忆人工检查文本能否支撑角色、轴、边界和原文 witness；
- 只产生聚合统计与临时研究记录，不训练、不揭封、不写 vault、不增加环境变量或工具。

若 16.0D.1 任一数据／召回门失败，停止上层 selector，只允许继续证据形或召回研究。

### 阶段 0D-B：危险基线与一次性防火墙 harness（另行批准）

- 先建立关系级 Spark-Gold；折叠重复事件簇后不足 12 个严格正例时立即停在数据门，不写比较器；
- 只有 Spark-Gold 数据门通过，才制作单轴硬负例并完成自然度盲检与单轴审查；有效案例再次少于 12 时停止；
- 先运行中性下游任务的最小 `no-memory / strict-positive / matched-hard-negative` CMI；以逐案例方向一致性为主，p 值只作探索性描述；
- CMI 未达到第 15.10A 节的方向门时停止当前比较器投入；通过也只允许继续下面的基线和防火墙开发，不能直接进入产品；
- 忠实复现／适配 BM25／dense、Green–Armstrong structured analogies、Creative Analogy Machine-style validation、TCA-SIR、PGR、CANA、MUSE、CMI、simple CBR、旧 SOS-PAR 和 `PGR+TCA+simple CBR` 便宜复合臂；
- 建立独立 BlindSourceLedger builder、OutcomeStore ACL、canary、taint DAG 和 outcome-permutation invariant harness；
- 比较静态 BlindCEF 与 `BlindSourceLedger+WitnessedTCA`，直召回与 Need-Path；
- 冻结候选 vault、调用预算、模型家族、prompt、renderer、标注手册和 split；
- 仍不接生产、不写真实记忆、不做可见输出。

> 2026-08-03 实测只推进到本阶段第二个门：关系级模型筛选 roster 达到 18，但 Hard-Negative 自然度盲检 `0/8` 通过，最终有效 H=0。因此后续 CMI、危险基线、防火墙 harness 与比较器均未启动；这不是对 Spark 下游效果的判定。

> 后续获准的 HN-F0 authored-benchmark 批次没有回收上述失败样本，而是另从冻结 ARN v1 来源重新建立 26 个严格开发正例和 208 条 H 边。singleton 语言门仅留下 45 条边、11 个唯一 T；T 唯一匹配上界 `11<12`，故在单轴语义评审之前再次停止。最新状态仍是：单轴调用 0、CMI 调用 0、防火墙 harness 与比较器未启动；本轮 `natural_observed=0`。

简单基线已经达到目标或防火墙无法证明零泄漏时停止。

### 阶段 1D：来源侧 DSR-CT 最小 2×2

- 执行 `same-seed / independent-event × singleton / natural contrast panel`；
- discovery 模型可提出新机制，validation 模型看不到 seed 结果；
- 同一 run 的全部候选／panel／binding／概率先全局冻结，再对去重 validation outcomes 并集唯一揭封；按事件簇 paired regret 与固定覆盖率风险分析；
- synthetic perturbation 只作 metamorphic test；
- 不启用 active reveal、完整 WIT、CUT 或训练 selector；
- 只有跨事件 contrast 主臂通过第 16.0D 的来源门，才进入下一阶段。

### 阶段 2D：历史 rolling-origin DSPT

- 在每个时点 \(t\) 只读 \(<t\) 数据并冻结 TargetObservationContract；
- 再揭示 \(>t\) 的自然记录，与 strongest target baseline／rivals 比较 proper score；
- 按 owner、event cluster、time block 外切分并报告 censoring／drift；
- 来源侧事件外验证不预测目标结果则停止真实迁移主张；
- 此阶段仍不向当前模型或用户显示 Spark。

### 阶段 3D：训练无关确认与独立实现

- 在新的 confirmation block 冻结 \(c^*\)、risk threshold、MechanismCard schema、rival set 和全部代码／模型 hash；
- 至少两个模型家族分担发现／验证，在新 block 交换角色；
- 确定性 scorer 与独立人工 span／cell 审核；
- 按预注册功效／精度模拟运行所需确认样本；30 个实际发布单元仅为 feasibility floor，不能替代风险界所需规模；另建独立 matched-null pools；
- 只运行一次；任何修改都需要新确认集。

### 阶段 4D：完全不可见的真实 future shadow（另行批准）

阶段 4D 与 5D 是阶段 3D 通过后的两个分别审批分支，不是同一批单元的串行漏斗。4D 回答 prospective transport，5D 回答“显示后是否有帮助”；任何 query×endpoint 一旦进入 5D 可见臂，就永久失去进入 4D natural shadow 的资格。

- 只对 `never_shown` 候选建立 target contract；
- 候选、预测与诊断不进入当前 prompt、Dashboard、vault、共激活边或用户界面；
- 观察窗结束后一次揭封，缺失／删失单独报告；
- 只能更新后续 transport reliability，不回改本次调用；
- 通过只为 prospective predictive transport 提供前瞻证据，不说明因果、独立复制或可见后有帮助。

### 阶段 5D：可见灵感效用盲评

- `no-memory / equal-length distractor / DSR candidate` 三臂隔离；
- 相同模型、token、renderer 与延迟预算；
- 人工盲评 appropriateness、nonredundancy、usefulness，novelty 次级；
- 可见候选的后续结果标记 post-treatment，不进入自然 shadow；
- 任何输出仍只是可忽略诊断问题，不生成行动、拒绝或 permit。

### 阶段 6D：外部复制与产品决策

- 新 owner／时间块／模型家族的授权独立复制；
- 复查系统文献、引用链、非英文数据库和专利分类；
- 完整报告失败、空结果、成本、漂移和实现反转；
- 只有第 16.0D 四门及安全 veto 全部通过，才讨论 `inspiration=true` 的最小产品接入；
- WIT／CUT 只在独立消融证明剩余增量时进入产品，否则删除。

### 旧阶段 0S：SOS-PAR 来源单元与结果防火墙可行性（冻结基线参考）

从本标题到旧阶段 6 的路线仅用于追溯旧版决策和忠实复现，不再表示当前实施顺序；当前顺序只按阶段 0D-A 至 6D。

阶段 0S 分成两个授权不同的子阶段，不能把“只读数据审计”与“实现防火墙服务”混成一件事。

#### 阶段 0S-A：只读数据可行性

- 不接入 MCP、不改真实 vault、不写回任何记忆；只在经授权的只读副本、独立测试 vault 或人工构造的脱敏材料上工作；
- 在现有 60 个 query 上只评估能否形成严格 \((X,A,Y,\Xi)\) 单元，不估计方法优效；冻结抽样框后完整报告 eligible、ineligible、unknown 和缺失原因；
- 由 A 组只看 \(X,A\) 标 transport，B 组只看结果 span 标 \(Y\) 与 `outcome_scoreable`，两组提交后才能由 C 组审计；
- 分别冻结行动轴／结果轴及其 \(\delta_A/\delta_O\) 零变化带、T1／T2／T3 证据级别、同主体与事件去重规则及字段级 provenance；
- 按第 16.0.1 节审查 `≥18/60` 严格单元、人工精确率、一致性和 provenance；不达标就停止，不进入任何算法 harness；
- 输出仅是可行性审计、脱敏样例和冻结数据字典，不包含 top-k、预测器、ACL 服务或效果数字。

#### 阶段 0S-B：一次性最小防火墙 harness（0S-A 通过后另行批准）

- 只在临时目录／独立测试 vault 构建可丢弃 harness，不改生产存储、MCP 或真实调用路径；
- 冻结 `top_k=8`、一个焦点机制、一个最近 rival、一个探针、三分类概率、先验混合、强结果盲基线和选择感知复合损失；
- 从结果前原文独立重建 BlindIndex；全文摘要、标题、标签、全文 embedding、后续文本和由结果驱动的共激活元数据全部进入 taint 清单；
- 建立测试级字段 ACL／进程隔离、append-only commitment、一次揭封接口、canary 和访问日志；
- 运行 outcome permutation、错误主体、时间反转、角色交换、action shuffle、重复事件和无机制 query；置换结果后 pool／top-k／mapping／prediction hash 必须逐字节不变；
- 预揭封访问、canary、hash 或 commitment 任一失败都使 harness 审计失败；通过也只说明隔离可实现，不说明算法有效；
- 0S-A 与 0S-B 均通过后，项目所有者才决定是否批准后续基线与最小效果实验。

### 旧阶段 1S：SOS-PAR 最小效果实验与组件 2×2（冻结基线）

- 进入条件是 0S-A 数据门与 0S-B 防火墙硬门全部通过；
- 先只实现结果盲 BM25／dense／RRF、case-based prediction／CDH、OB label-free CMI、一个冻结且不含主动层析的 `WIT-min consensus` 和最强普通概率基线；这不授权完整 WIT solver／checker／版本空间工程；
- 现有 60 例继续只作开发／构造数据，另建最小 60-query signal 数据：12 个冻结协议，48 个保持结果封存；另建至少 72 个独立 null query pool 估计 5% 假发布上界；
- 所有有效方法只读取结果前字段，固定候选全集、top-8、模型、token、调用、失败预算和 renderer；各方法可产生不同 top-8，但在 query 级成对比较；
- 运行“同一基础事件的单情节／同主体纵向 × WIT-min 共识／命名 rival proper score”的 2×2；结果可见条件只作 `invalid_for_claim` 泄漏正控；
- legacy 复现终点是第 15.0.3 节 query 级 selection-aware \(\Delta_{selection}\)；它不参与新版 go／no-go。同时报告共同 scoreable 面板上的纯 Brier、Recall@8、PairAUC、precision–coverage／AURC、matched-null false release、空候选 abstain 和 permutation invariance；
- 所有失败、unknown、超时和零结果进入预注册分母；揭封后不得换候选、改映射、换 rival、补探针或重试；
- 按第 16.0.2–16.0.3 节执行积极门、no-go 和组件降级；两项组件增量未分别通过时，不进入完整联合开发；交互 \(\Gamma_{SOS}^{int}\) 未通过时不宣称协同；
- 1S 通过只说明来源结果盲封的筛选信号值得继续，不说明目标机制成立，也不授权 shadow 或产品接入。

### WIT 子协议 0A（仅在 1S 通过且 WIT-min 有独立增量时）：冻结语言、证据承诺与证据筛查协议

- 冻结张力、Need Frame、有限 claim lattice、固定 claim 尝试顺序／最低 specificity／最低 Need 覆盖、证据轴、候选记忆职责类型、完整未过滤分层抽样框和 WIT-core 定义；Need／claim 编译必须记录 candidate-blind provenance、候选预暴露清单与进程访问日志，预暴露样本只能进入单列弱条件；
- 冻结查询无关 CEF schema、required/optional 规则、原文 span 与 source hash 绑定、排除运行元数据的规范内容摘要 `cef_payload_digest`、包含该摘要与时间戳／provenance 的 `cef_commitment_hash`、抽取器／prompt／模型／seed／解码参数／依赖版本、缓存失效、query-canary 输入隔离测试和无查询重复稳定性测试；
- 冻结 Transport DSL 的合法／禁用操作、最大程序长度和逐操作 witness 类型；冻结一个有限合理代码本族 \(\mathcal K\) 及各自操作成本，禁止只选一个“敏感性代码本”或看结果后择优；
- 冻结逐代码本目标 \(J_k\)、\(\epsilon\) 合理区间／主值／审计网格／最大包络并集、预测等价类去重、schema-bounded 有限 completion 空间 \(\Omega_Q\times\Omega_M\)、所有 codebook×completion membership 覆盖、最大 solver optimality gap、超时／不可证最优时的 `unknown` 规则；
- 冻结 `internal_fit / internal_audit_heldout / external_challenge` 的生成、隔离、哈希、揭封和同源泄漏规则；外部挑战独立持有、只揭封一次，锁定 signed 结论逐方向、mixed 逐 block 给出结果，任一缺失／失败都强制方向 unknown；shadow 阶段可保持 pending，但不得冒充产品通过；
- 冻结结构 rival 与 eligible 方向 rival 搜索、结构／方向各自的 \(S_q\) 与 matched-null、`candidate × claim_type` 联合 maxT 或层级 closed-testing 的唯一 multiplicity 方案、强 candidate-level 与 `complete_null_pool_only` 两种声明；后者只能形成池级审计，不能定位任何候选。并冻结表层远／机制近非互偿门、结构／方向嵌套发布规则、独立 `wit-check` 不变量和 Witness Bundle 最小字段；
- 固定阶段 0B 为 18 个单一 episode、每个完整度层 6 个，并冻结完整未过滤抽样框、抽样种子、纳入／排除、聚类、缺层、零适用轴处理和替补禁止规则；不得在看过轴标签后按“适用轴资格”预删 episode；
- 填入 \(c^{text}_{min}\)、\(k^{axis}_{min}\)、\(k^{text}_{min}\)、\(k^{usable}_{min}\)、AxisApplicableRate、证据精确率、SupportedFieldRecall、UnknownRecall、response-direction 混淆率、共同假阳性率和最小有效分母的绝对门限；明确 0B 的 CI 只作描述；
- 冻结阶段 0B 自动抽取所用的至少两个模型家族、精确版本、各自重复数和分家族报告规则；同家族多 seed 不算第二个家族；
- 冻结表层 × 机制四联体标注规则、目标内配对主终点、一般 selector 的 StructuralPairAUC／四不可互偿分项／零候选 abstain／自然开放库／同覆盖率门、2×2 结构归因模型和静态结构基线；同时冻结 `CEF × version-space` 与“双承诺”两个 2×2 的四臂、主效应、交互、完整臂对单臂差值和删模块动作，但正式比较的资源参数先保留为待 0D 填写；
- 在看到 0B／0C 结果前冻结方向类别、符号坐标规范、有限 mixed 分组库与最大块数、受控方向测试集，以及 `tau_pure / known_coverage_min / directional_coverage_min / mismatch_max / n_nonredundant_min / n_internal_holdout_min`、mixed 每块对应门、权重归一化／同源去重／等号／tie／零分母规则、\(\eta^{orientation}_{min}\)、逐类 recall、SignedOutputCoverage／abstention 和全部必要变形错误率门；这些值不得由 `external_challenge` 设定；
- 冻结 `CommonBlindOutput`、人工 blind gold、neutral proposer、native／共同 proposer 双轨、N/A／技术失败、matched coverage 和 target／基础事件／机制组聚类规则；冻结 `PostHocAtomDrift`、`PostHocNeedDrift`、`SurfaceFalseFriendAcceptanceAtMatchedCoverage`、`UnsupportedCrossDomainBindingRate`、`StructuralRivalEscapeRate` 的单位、分母、规范化、阈值和同时推断层级；
- 同时冻结 \(\delta^{WIT}_{min}\)、\(\delta^{CUT}_{min}\)、\(\gamma_{min}\)、\(\gamma^{attr}_{min}\)、\(s_{min}\) 与四个 \(s^{pair}_{a,b,min}\)、P1／P2 各项 \(m_k\)、\(\varepsilon_C\)、\(\varepsilon_{cost}\)、\(c^{pair}_{min}\)、\(c^{reject}_{min}\)、\(c^{output}_{min}\)、\(b_{min}\)、\(\epsilon_{preserve}\)、\(\psi_{min}\)、\(\kappa_{min}\)、\(i^{struct}_{min}\)、\(i^{dir}_{min}\)、\(c^{dir}_{min}\)、\(c^{dsl}_{min}\)、\(u^{cef}_{max}\)、\(p^{struct}_{min}\)、\(p^{dir}_{min}\)、\(a^{struct}_{max}\)、\(a^{dir}_{max}\)、\(n^{struct}_{max}\)、\(n^{dir}_{max}\)、\(f^{code}_{max}\)、\(v_{min}\)、\(w_{min}\)、\(\delta^{future}_{min}\)、方向机制、claim-level null multiplicity、claim／\(\epsilon\)／代码本、零候选／开放库／同覆盖率、安全／下游／在线与离线摊销成本实质界；query-canary 违例计数固定为 0。若部分界依赖资源测量，先冻结唯一决策函数；
- 冻结 CMI、RootMem、GSW／Panini、YARN／结构抽象映射、自动过程类比检索、Akifuji–Tsuji／Fanizzi DiVS-style version-space 类比适配、Claim-Selective／SURE-RAG／可适配 MedRAGChecker 语义验证、PCC-style 独立 deterministic checker、GED／partial GW、SparseCL 式矛盾检索和最强普通 reranker 的论文／代码版本、oracle 与 label-free 防火墙、适配数据边界和禁止读取的 gold／标签；Relish 既作版本空间／CEGIS 计算骨架核对，也与 DiVS 共同形成自然文本适配臂。另冻结 `GSW/Panini + DiVS/Relish + 最强语义 verifier + PCC-style checker` 最近邻复合对手及 leave-one-out，其中必须含 flat／post-hoc 单层 gate 对嵌套发布的替代；最强语义 verifier 只能在独立适配集按冻结同覆盖安全指标选择，无充分适配集则全部保留并校正，MedRAGChecker 资源不匹配记 N/A，不得正式揭封后换组合；
- 冻结 CEF 构建、图编译、solver、checker、缓存与人工审计的离线成本记录方式、折旧窗口和每次请求摊销规则；不能只比较在线 token／延迟而隐藏 WIT 的预处理成本；
- 确认私人记忆只读审计的数据范围、脱敏方式与可使用的模型 API；
- 未完成上述清单时不得运行阶段 0B；任何会使用 OB 记忆、四联体或 heldout 的基线适配必须等待 0B 冻结开发期允许范围。

### WIT 子协议 0B：真实记忆证据充分性门禁

- 不修改真实 vault，不接入 MCP；
- 对预注册抽出的 18 条分层真实记忆做只读 `supported / unknown / not_applicable` 审计；
- 人工 gold 先在完整未过滤抽样框报告 AxisApplicableRate，再计算 TextEvidenceCoverage 和 UsableEpisodeRate；文本内在门通过后才运行自动抽取；
- 至少两个冻结模型家族分别报告非 unknown 证据精确率、UnknownRecall、SupportedFieldRecall、response-direction 混淆率和共同假阳性，不能先聚合；
- 对严格预提交 CEF 做 paired canary：保持源文本、seed、模型和 extractor 可见输入一致，仅改变隔离通道中的假查询，任何 CEF 变化计入 `CEFInputLeakViolationCount` \(N_{canary}\)，且必须恰为 0；另在不提供任何 query 时按冻结重复测 CEF span、节点、边与 required/optional 的 `CEFNoQueryInstability`，两者分开报告，不能把随机不稳定性冒充 query effect；
- 阶段 0B 只使用冻结绝对计数／点估计门，CI 仅描述；两个模型家族都分别通过全部人工与自动抽取门，才算该预注册分层在开发期可用；任一家族失败或有效分母不足即不通过；
- 完整事件组低于 \(k^{usable}_{min}\) 或查询无关 CEF 门失败时停止完整 WIT；可另外保留不声称机制方向的普通检索基线；
- 0B 只形成开发期范围假设。至少一个预注册分层通过时，立即冻结阶段 1W 允许范围；0C、0D 和阶段 1W 不得扩展。正式产品范围仍需更大独立样本确认。

### WIT 子协议 0C：危险相邻基线的忠实复现与 OB 无标签适配

- 作者基准上的忠实复现可在 0A 完成后与 0B 并行；任何使用 OB 记忆类型、候选池或适配数据的工作，必须等 0B 至少一个分层通过并冻结开发期允许范围；
- 先复核 CMI 论文、公开代码、任务定义和版本；CMI 是最先做的低成本危险基线；
- 先在作者 Causal-LoCoMo、冻结代码提交和尽可能一致的模型／配置上复现 `no-memory / with-candidate / perturbed-same-candidate`、gold target scorer 与 label-aware risk filter；分别记录 selector 可见标签和只用于评估的标签，再与作者结果比较；
- 论文复现完成后，才在 0B 冻结范围内构造 OB label-free intervention reranker，禁止读取 gold answer、useful/irrelevant/harmful 标签、gold 候选角色标签和外部挑战标签；
- 在同一候选池上实现最小但忠实的自动过程类比检索、YARN 式结构抽象映射、RootMem 式隐式逻辑／根记忆检索、GSW／Panini native 结构记忆及 OB 安全只读 sidecar、Akifuji–Tsuji／Fanizzi DiVS-style version-space 类比适配、Relish-style 关系版本空间适配、Claim-Selective／SURE-RAG／资源可匹配时的 MedRAGChecker 语义 evidence verifier、PCC-style 独立 deterministic checker、GED／partial GW 结构匹配、SparseCL 式矛盾检索和普通 cross-encoder／LLM reranker；再按冻结接口组成 `GSW/Panini + DiVS/Relish + 最强语义 verifier + PCC-style checker` 最近邻复合对手。PCC 只检查确定性 artifacts；MedRAGChecker 若依赖不可匹配 KG／蒸馏资源则只作 N/A／资源上界。GED／GW 使用各自原生图表示，不强迫先吃 WIT 的 gold-like CEF；不能给 WIT 更多原文、模型调用、token 或人工标签；
- 对每个相邻基线区分“论文原任务复现”“OB 标签无关适配”和“人工 oracle 上界”，不能把不同信息权限的结果放在同一排名；
- 代理 utility 与 RRF 调整默认使用和阶段 1W 目标完全分离的适配集。若数据不足，只允许预注册按 target、vault 与机制组隔离的外层 cross-fitting；每个阶段 1W 目标必须由未见过同组数据的适配器评分，区间估计纳入外层 fold，不能在同一 60 例上调好后报告名义未调参区间；
- 明确其代理 utility、评分范围、候选缺失、失败调用、在线调用成本、离线构建成本及摊销口径和排序适配；RA-RFT 等需要任务奖励或训练标签的路线若无法匹配信息权限，只列为资源不匹配的扩展上界，不能与 label-free 方法混排；
- 0C 只验证各基线可运行并暴露适配约束，不冻结不公平的共同预算，也不能在未运行 WIT-core 时仅凭某个不同构念的单项结果淘汰 WIT。

### WIT 子协议 0D：基线适配后冻结正式比较与求解协议

- 依据已实现的 label-free 基线和 WIT-core 最小定义，冻结共同候选池、强制四联体输入、answer model、renderer、token、延迟、调用和失败预算；
- 冻结每个方法如何产生全预序、seed 聚合、合法 tie、abstain、缺失和失败的目标级记分；同时冻结 `CommonBlindOutput` 适配器、neutral proposer 的唯一版本、native／共同 proposer 两条轨道、方法原生 N/A 与技术失败的区别，以及 matched coverage 阈值；
- 按 0A 冻结的选择规则，只用与正式比较隔离的适配数据确定非 WIT 的最强自动基线 \(auto^\star\)；若数据不足以无偏选择，则把一组危险基线共同放入层级检验，不能看正式结果后挑对 WIT 最有利者；
- 冻结 `CMI+WIT-core` 输入的两组排名、RRF 常数和 tie 规则，并冻结 `CMI+ordinary-reranker`、`CMI+structure-ablated-WIT`、`CMI+permuted-WIT-ranking` 三个同预算归因对照；若比较多个组合算子，必须作为独立实验臂并校正多重性；
- 冻结 exact／symbolic solver、版本、随机性、硬件预算、最优性间隙、分支下界、枚举完整性证书与超时记分；近似求解器只能作为另列消融；
- 在不看标签的微型合成图上穷举所有合法 program–query completion–memory completion–codebook 成员，与 solver 的逐 codebook／completion 最优值、\(\mathcal V_{release}\)、原始 completion／codebook 覆盖和 rival search 逐例核对；
- 原样带入阶段 0A 的主终点、\(\Delta_{CUT}\)、\(\Gamma\) 与三项 \(\Gamma^{attr}\)、P1／P2、两个核心 2×2、一般 selector、结构／方向可识别性与覆盖、CEF、heldout／rival、claim／\(\epsilon\)／代码本、方向类别与变形、Need／future、null、安全、下游和在线／离线成本全部实质界；只能向 0A 的冻结决策函数代入 0C 资源常数，禁止使用排序准确率或下游效果改界；
- 按已冻结实质界做功效模拟并确定样本量；冻结一次分析时间点、所有无结论门共享的唯一一次扩样、检验层级和最大预算；
- 未完成并签署 0D 清单时不得运行阶段 1W 正式对比。

### 阶段 1W（仅在 1S 通过且 WIT 有独立增量时）：WIT-VS 最小核心的可识别性与反自证实验

- 进入条件是：1S 的 SOS-PAR 主终点和必要门通过，`WIT-min consensus` 组件臂在 matched-SOS 候选内达到预注册的 Top-1／AUPRC 非冗余界；随后 WIT 子协议 0A–0D 完成，其中 0B 至少一个预注册分层通过联合证据门、0C 的 OB label-free CMI 可运行、0D 的共同 renderer 与数值协议冻结；
- 不接入 MCP，不修改真实 vault；
- 现有 60 例仅作为开发集；
- 先在可穷举的微型合成事件图上实现查询无关 CEF、候选盲 Need／claim commitment、冻结 Transport DSL、codebook×completion×\(\epsilon_{max}\) 并集版本空间、盲封留出、rival search、两种强度明确区分的 matched null、独立 `wit-check` 和 Witness Bundle；solver／checker 与穷举不一致或“扩类反而增信”即停止；
- 再在冻结范围的真实文本上只实现最小 WIT-core：检索前盲封、角色／轴方向冻结、版本空间有符号比较器、`internal_audit_heldout`、正控制、负控制和 `unknown`；
- 先运行 15.2 的受控方向机制层；若方向门失败，终止当前 signed 版本并把所有方向输出强制为 unknown，只能在独立命名的 unsigned structural-retrieval baseline 中继续记录召回；
- 必须运行查询替换、同源 CEF 稳定性、映射 tie、相反 rival、删证据、结构破坏、表层换皮和 matched-null 测试；
- 必须按第 15.6 节运行 `CEF × version-space` 与“双承诺”两个 2×2、P1／P2、最近邻复合对手和逐项 leave-one-out；所有指标使用共同盲接口、冻结 neutral proposer、独立 gold、相同 coverage／预算和预注册失败规则；
- 暂不实现在线主动信息增益、完整响应张量、神经求解近似或产品校准；只做保证版本空间完整性所必需的反例搜索；
- 公平比较 `向量／普通 reranker / SparseCL式矛盾检索 / 静态结构映射 / 自动过程类比 / YARN式抽象映射 / RootMem式隐式逻辑检索 / GSW或Panini结构记忆 / DiVS或Relish-style version-space类比 / Claim-Selective或SURE-RAG或可适配MedRAGChecker语义verifier / PCC-style独立checker / 最近邻复合对手 / GED或partial-GW / OB label-free CMI / Spark-CUT / WIT-core / CMI+WIT-core`；
- 以 15.7 的目标内配对辨别率 \(\Delta_{WIT}\) 作为**该条件 WIT 分支**的唯一终点；不得把它写成 SOS-PAR 首轮主终点；
- 同时把 CEF 输入隔离违例／无查询不稳定、checker 通过率、claim／\(\epsilon\)／代码本敏感性、结构版本空间可识别与 eligible 方向可识别、DSL／方向 eligible 覆盖、分层盲封精确率、结构／方向 rival 残余接受率、matched-null 家族错误和 solver 证书完整率报告为不可省略的机制门；
- 只有上述 WIT 机制门、适用时的方向机制门、主终点、\(\Delta_{CUT}\)、P1／P2 及相应因子门、一般 selector 的 StructuralPairAUC／四分项／零候选／开放库／同覆盖率门、PairResponseCoverage、RejectionEvidenceCoverage、OutputCoverage、结构破坏敏感性、结构保持非劣性、Need 增量、成本、安全以及预注册层级中本轮主张所需的 \(\Gamma\)／\(\Gamma^{attr}\) 门均通过，才可申请阶段 2；
- \(\Delta_{WIT}\) 实质失败、必要机制／安全／成本门失败，或一次共享扩样后仍无结论，分别按“算法实质失败”“产品伤害／不可行”或“证据不足而不继续投入”停止；
- 若 \(U_{\Delta_{CUT}}<\delta^{CUT}_{min}\)，说明版本空间与见证门尚未兑现预注册的最小设计价值，停止当前 WIT 扩展并保留 CUT；若区间无结论，只允许与所有其他无结论门共享一次预注册扩样，不能凭点估计继续；
- 只有 15.7 定义的“CMI 联合占优且 \(U_\Gamma<\gamma_{min}\)”全部成立时，才能**以 CMI 全面替代为理由**停止当前 WIT；这不是其他停止理由的必要条件。

### 阶段 2：条件性的胜出组件离线开发与 WIT 结构归因

从这里开始只保留不可遗忘的进入条件和安全边界，不构成可执行规格、排期或模块承诺。阶段 1S 拿到冻结数字前，不再细化实现接口、参数表或工作量；任何细化都需要新的自包含协议和再次批准。

进入条件：阶段 1S 的 SOS-PAR 主终点、泄漏门、召回、风险—覆盖与 matched-null 门通过；项目所有者再次批准一个**唯一结构审计策略**。若 1W 证明 WIT 具有 matched-SOS 非冗余增量，可批准 `wit_slim_locked`／后续完整 WIT；若 WIT 被判重，只能批准 `flat_integrity`。两条分支不得在看阶段 2 结果后择优，也不能写成“已经证明真实文本或算法普遍有效”。

- 保留 1S 通过的来源证据单元和裁决组件，删除未达到门槛的组件；
- 只有 1W 通过时才加入版本空间内主动机制层析、算法内部审计探针和两类预算内反例审计；
- 若 WIT 未通过独立增量门，阶段 2 只验证 `flat_transport_integrity_verifier` 的 span／主体／时间／冻结 binding／禁止字段／hash 检查；所有方向固定为 unknown。flat 的 transport precision、unsupported-binding、成本或安全门任一实质失败时，停止可见输出路线，只保留 SOS-PAR 离线研究；
- 运行完整的表层近／远 × 机制近／远四联体，检验 WIT 是否真正依赖机制可迁移，而不是表层或主题近似；
- 对每个自动入选与拒绝候选保存 Witness Bundle，并由看不到方法名和得分的人工审计证据 span、竞争程序和带搜索边界的反例记录；
- 在严格时间切分且不回填的子集上提交预测，再用自然后续事件做前瞻见证；该结果只作预测证据，不冒充因果证明；
- 计算自动方法相对 matched distractor 恢复人工 oracle 增益的 \(\rho_{oracle}\)，并始终并列报告三个原始效应；
- 至少使用两个预冻结模型家族分别端到端运行；
- 加入跨家族共同假阳性与交叉角色消融；
- 使用独立校准集或开发集内 cross-fitting 研究风险门槛；
- 用预注册消融建立 CEF 承诺、候选盲 Need／claim、版本空间、独立 checker、heldout、rival search、matched null、CUT 子层与 Need Frame utility 的开发期增量信号，不因完整方案已经写好而默认全部保留；
- 任一模块没有达到最小增益或成本超限，应删除该模块而不是继续堆叠。

### 阶段 3：条件性不公开 shadow

进入条件：阶段 1S 的最小胜出方法已冻结；阶段 2 已对唯一的 `wit_slim_locked` 或 `flat_integrity` 策略完成锁定门验证，其必要模块具有独立增益或预注册安全必要性、成本可接受；项目所有者再次批准。没有通过的结构审计策略时不得进入 shadow。

- 使用独立空测试 vault；
- 真实调用路径只记录 shadow 结果，不注入模型上下文；
- 验证性能、并发、失败路径、提示注入和隐私；
- 不影响 Spark 关闭或 `inspiration=false` 时的现有行为；
- 只产生 `shadow_hypothesis`、带原因的 `shadow_unknown` 或 `shadow_none`；不得写回桶、条目或边。

### 阶段 4：条件性锁定确认

进入条件：shadow 安全门通过、系统性共病可接受，并由项目所有者再次批准。

- 冻结代码提交、BlindIndex schema、结果防火墙、一次揭封协议、来源单元版本；若 WIT 保留，再冻结 CEF／DSL／代码本、solver；同时冻结配置、提示、三个模型家族、seed 列表和聚合规则；
- 冻结校准器、阈值、主要终点、分析代码和数据哈希；
- 运行全新确认集，不在确认集上调参；
- 完成人工盲评、CMI 对照、随机对照、renderer 隔离和结构破坏对照。

### 阶段 5：条件性独立复制

进入条件：全新确认集达到预注册的实质效应和安全标准。

- 由不同人员或来源构建数据；
- 使用独立测试／合成 vault，或逐 vault 获得数据主体明确同意的只读材料；访问隔离、不汇集可识别原文、不调用未经该数据方批准的外部 API；
- 复制集自身达到预注册的最小可接受效应；
- 分层报告三个模型家族及跨家族共同错误。

### 阶段 6：条件性产品决策

只有确认与独立复制均支持后，才重新审查哲学边界并决定：

- 是否接入现有调用参数；
- 若接入，只扩展现有请求参数（例如 `inspiration=true`），不新增第 16 个 MCP 工具；
- 是否对用户可见以及默认是否关闭；
- 是否需要热更新、版本号和正式文档；
- 环境变量清单和部署迁移；
- 是否值得形成论文或公开 benchmark。

## 19. 配置与环境变量原则

研究阶段不建议将大量权重暴露为环境变量。

原因：

- 未校准参数会形成不可维护的部署表面；
- 用户难以理解结构阈值；
- 不同模型的分数不可直接共享；
- 环境变量无法解决算法层面的抽象和反事实问题。

实施前参数应先进入类型化内部实验配置，例如：

- BlindIndex 结果前字段白名单、切分协议、taint DAG、canary 和 outcome-permutation 不变量；
- BlindSourceLedger、WitnessedTCA、discovery seed／validation cluster exclusion、自然 contrast cell 与一次揭封协议；
- 变量长度 discovery／validation recall 预算、MechanismCard、material rival set、真实多切点联合签名、交叉拟合强 \(p_{base}\)、逐机制 proper score、固定 coverage \(c^*\) 与 risk–coverage 门；
- TargetObservationContract、`never_shown` exposure receipt、rolling-origin／censoring／missingness、append-only commitment、模型／prompt／seed／implementation hash 与全部失败分母；
- CEF schema、required/optional 规则、源哈希算法与抽取器版本；
- Transport DSL、禁用操作、逐操作 witness schema、有限代码本族与最大程序长度；
- \(J_k\) 各项、有限 completion 宇宙、\(\epsilon\) 区间／最大包络、claim lattice、预测等价类规则与 solver／checker gap；
- fit／heldout／external challenge 的分割哈希、response-fit 符号等变规则、\(\mathcal D^{fit}\cap\mathcal D^{hold}\) 聚合、claim-level matched-null／multiplicity 和 rival-search 预算；
- 候选预算；
- 探针预算；
- 模型或 seed 数；
- 允许的抽象梯度；
- 最低证据覆盖；
- candidate-blind provenance、实际 CEF payload commitment、输入隔离违例／无查询不稳定、结构／eligible 方向版本空间可识别、盲封共识、竞争程序、结构／方向 matched null、Need Frame 增量与自然后续见证门；
- 有符号坐标规范版本、有限 mixed 分组库版本、逐类 recall／SignedOutputCoverage 门和全部必要变形错误上限；
- 最大输出数；
- shadow 开关。

只有稳定、确有运维意义的开关才考虑成为环境变量。新增或修改环境变量前必须先更新 docs/ENVIRONMENT_VARIABLES.md，并保留兼容、弃用和迁移说明。

用户层面的 inspiration 是请求参数，不应由全局环境变量替代。部署方可以关闭 Spark，但不能通过环境变量让普通请求在不知情时自动注入灵感。

## 20. 实施前待项目所有者确认

以下问题应在任何代码改动前确认：

新版优先确认项：

1. 是否批准以 DSR-CT 作为新版 Spark 的主研究假设：discovery seed 可以提出新机制，但不能为自己提供确认性证据；SOS-PAR 只作独立 validation memories 的结果防火墙／预序评分组件；
2. 是否批准阶段 0D-A 只读审计，并指定哪些经授权记忆可用于 60-query discovery／validation／rolling-origin 可行性；本文不自动批准 0D-B harness、揭封或真实 shadow；
3. 是否接受 event-cluster 不相交是硬门：同一对话切片、近重复、跨桶副本和 supersedes 链不能跨 discovery／validation／confirmation；这只建立数据隔离，不自动构成独立复制；
4. 是否接受 `BlindSourceLedger + 临时 WitnessedTCA` 双视图、Need-Path 只生成信息需要、不生成行动／意图／计划，且所有派生对象均不写回真源；
5. 谁分别持有 discovery gold、validation cell gold、outcome gold、盲封结果服务、append-only commitment、canary／访问日志和一次揭封权限；
6. 是否同意废止“固定 top-8 + 空槽最大损失 + 混合 \(p_{SOS}\)”作为新版主终点，改用预冻结 coverage \(c^*\) 上相对预冻结基线的 \(\Delta_{PF}\) 唯一主效应，并由功效／精度模拟决定样本量（30 只作 feasibility floor）；
7. 是否批准第 15.0D 与 16.0D 的主终点、四个核心门、no-go、简单基线优先和组件删除规则；
8. 是否批准 DSPT 先做历史 rolling-origin；真实 future shadow 必须另行授权、`never_shown`、不回流当前调用，并明确谁负责 endpoint、censoring、失访与隐私治理；
9. 是否接受可见输出只能说“独立历史对照更支持该机制，可自行检查某边界”，不能宣称目标因果、决定正反行为或触发拒绝／permit；
10. 是否同意现有 60 例只用于可行性／开发，锁定确认必须使用新的封存 query package、跨模型独立实现和独立 matched-null；
11. 是否同意 `inspiration=false` 完全旁路、不新增 MCP 工具、不写回记忆真源、认知画像、关系类型或共激活权重；
12. 是否接受本轮新颖性只表述为“截至检索日，本轮非系统检索覆盖的一手来源中尚未定位到所述两层完整协议”，不宣称世界首创、因果证明或专利结论。

以下原 WIT 项仅在阶段 1D 来源事件外验证通过、且 WIT 被消融证明对 DSR-CT 有独立增量时继续确认：

1. 若 WIT 被 1S 保留，是否批准以 WIT-VS 作为条件性结构审计算法名，并把 Spark-CUT 仅保留为其有符号扰动验证子层；不得再把 WIT-VS 写成新版 Spark 总基础；
2. 是否批准“候选盲 Need／claim commitment + 查询前实际 payload CEF commitment + 逐操作 witness 的有限迁移语法 + codebook×completion×\(\epsilon_{max}\) 并集版本空间 + 结构／方向嵌套发布 + 独立 checker”作为研究核心；
3. 是否同意现有 60 例只作为开发集；
4. tension 是否必须由调用方显式提供，还是允许证据约束下自动派生；
5. 已定：DSR 不产生可注入的 `speculative`；legacy `speculative` 永远只作 audit-only，未来若需展示必须另立非 DSR 协议；
6. 是否接受跨类比确认只作为加分项而非硬门；
7. 若阶段 1D 来源事件外验证通过，是否同意 WIT 子协议 0A–1W 仍需单独批准，阶段 2D–6D 更不得由本文自动授权；
8. 是否需要把正式研究规格放进仓库，还是继续保留为仓库外审阅文件；
9. 是否计划后续以论文或公开 benchmark 为目标；
10. 哪些模型和 API 可以用于私人记忆上的派生分析；
11. 可接受的单次调用成本与延迟上限；
12. 何时冻结确认集，谁负责持有盲态标签；
13. 允许对现有 60 例进行多少轮设计修改；
14. 阶段 0A 应填入怎样的 \(k^{axis}_{min}\)、\(c^{text}_{min}\)、\(k^{text}_{min}\)、\(k^{usable}_{min}\)、证据精确率／召回率、共同假阳性率和混淆率门；
15. 已定：`speculative` 永不注入，只保留审计信息；本项不再作为可调产品选项；
16. 哪些普通情节记忆可进入固定 18 条的只读证据充分性审计，哪些职责类型必须保持排除；
17. CMI 使用哪个提交、模型和评测配置作为论文忠实／oracle 复现，OB label-free 代理 utility 如何定义；
18. 阶段 2 的两个模型家族与锁定确认的第三个模型家族分别是什么；
19. 是否同意只有 CMI 对 WIT 的结构／下游／安全非劣与成本优势联合成立，且 \(U_\Gamma<\gamma_{min}\) 时，才把“CMI 全面占优且组合无增益”作为停止理由；方向模块失败只停止 signed 主张，不冒充 CMI 全面占优；
20. 谁负责在任何结果可见前冻结数值协议、数据哈希和一次扩样规则；
21. 阶段 0A 应冻结怎样的 \(\delta^{WIT}_{min}\)、\(\delta^{CUT}_{min}\)、\(\gamma_{min}\)、\(\gamma^{attr}_{min}\)、P1／P2 各项 \(m_k\)、\(\psi_{min}\)、\(\kappa_{min}\)、一般 selector、结构／方向识别与 null、\(c^{pair}_{min}\)、\(c^{reject}_{min}\)、\(c^{output}_{min}\)、\(b_{min}\)、\(\epsilon_{preserve}\) 以及安全、下游、在线／离线摊销成本边界或资源映射函数；阶段 0D 允许代入哪些资源常数；
22. 是否接受默认用冻结 RRF 组合 CMI 与 WIT；如需比较其他组合算子，是否愿意把它们预注册为独立实验臂并校正多重性；
23. 若 WIT 被保留，是否批准“查询局部、证据受限的有符号扰动—响应对应”作为 Spark-CUT 子层的方向判据，并仅把 WIT-VS 的证据传输与版本空间发布门用于 **DSR-CT 事件外来源审计和共同风险校准均已通过**的条件审计；DSR adapter 不依赖旧 `localized_HF/HR`。新版 Spark 的总基础是 discovery／validation 隔离与自然对照验证，不得由 WIT 或 SOS-PAR 自验证反向取代；
24. 谁负责在候选响应可见前冻结角色映射、干预轴与结果轴的正向端点、\(\sigma_U\)、\(\sigma_O\)、有限 mixed 分组库和 `internal_audit_heldout`，谁独立持有一次揭封的 `external_challenge`；谁签署锁定 signed 输出逐方向、mixed 逐 block 的外部结果，并保证缺失／失败时产品策略强制方向 unknown；
25. 阶段 0A 应冻结怎样的响应到类别参数（`tau_pure`、两类 coverage、mismatch、非冗余／内部留出数量、mixed 每块门、权重／去重／tie／零分母规则）、SignedOrientationGate 下界 \(\eta^{orientation}_{min}\)、逐类 recall、SignedOutputCoverage／abstention 门，以及全部坐标不变性、真实响应翻转、混合塌缩、结构破坏和删证据不转 unknown 错误率上限；
26. 是否同意 `opposed` 先作为独立 shadow 假设验证，只有方向机制门与锁定确认均通过后才讨论产品可见性。
27. 谁冻结 CEF schema、span 证据规范、required/optional 规则、source hash、实际 payload 规范序列化、seed／解码／时间戳链和查询输入隔离审计，谁持有真正先于 query 的 CEF 抽取流水线；
28. 是否批准冻结 Transport DSL canonical enum 仅含 `bind_role / bind_axis / bind_condition / align_phase / coarsen_type / coarsen_phase / drop_optional / open_boundary_port / restrict_scope`，并禁止 `invent_edge / invent_role / drop_required / rewrite_evidence / flip_response / posthoc_axis / posthoc_mixed_partition / probe_specific_orientation`；
29. 有限合理代码本族、逐 \(k,\omega\) 的 \(J^*_{k,\omega}\)、\(\epsilon\) 区间／最大包络、最大 solver optimality gap 与超时 `unknown` 规则分别取何值，谁在揭封前签署；
30. 是否接受“未找到 rival”不等于“证明类内不存在 rival”，只有枚举／界证明覆盖全部 codebook×completion×近优预测等价类时才允许发布相应结构或方向 claim；
31. 是否接受结构距离和表层距离不可互相补偿：只有表层达到远距带且机制 transport 达到近距带，才称远类比；
32. 2×2 四联体与自然后续见证由谁构建和盲持，哪些真实时间序列有合法、明确的数据授权；
33. 是否批准用户产品仍只通过现有调用参数显式触发灵感，不新增 MCP 工具，且 `inspiration=false` 必须完全旁路；
34. 是否接受本文的创新表述只限于“截至 2026-08-02，本轮非系统检索覆盖的一手来源中尚未定位到所述两层完整协议”，不宣称世界首创或完成专利自由实施分析；
35. claim-level null 采用 `candidate × claim_type` 联合 max-stat，还是结构先行、方向后行的 closed-testing／alpha 分配；谁签署且禁止结果后切换；
36. 谁实现并独立维护不调用 LLM 的 `wit-check`，如何证明它没有与 generator 共用会共同失效的判定路径；
37. 是否批准 response-fit 的全局极性翻转等变不变量，以及每成员必须用 \(\mathcal D^{fit}\cap\mathcal D^{hold}\) 才能发布方向；
38. 是否批准条件阶段 1W 的 `pre-query CEF × version-space unanimity` 2×2 核心联合消融及其最低主效应／交互动作规则；
39. 是否批准独立的“actual-payload CEF × candidate-blind Need／claim”双承诺 2×2、`CommonBlindOutput`／neutral proposer、P1／P2、最近邻复合对手和 leave-one-out（含以 flat／post-hoc 单层 gate 替代结构先行／方向嵌套发布），并接受任一部件没有非冗余增量时删去对应复杂度。

## 21. 审阅检查表

项目所有者审阅时可重点检查：

- discovery 与 validation 严格候选池是否都只由各自 cutoff 前的 BlindSourceLedger 及结果盲派生表示构建；
- discovery seed 及其同 event cluster 的切片、近重复、跨桶副本和 supersedes 链是否全部排除出 confirmation panel；
- seed 是否可以提出新 \(H_C\)，但 MechanismCard、轴、边界、material rivals、panel 检索规则和概率是否在 validation outcomes 可见前冻结；
- analogue／bridge／foil／null 是否来自自然、source-hash 可追溯事件；合成扰动是否只标 `metamorphic_only`；
- 标题、摘要、标签、全文 embedding、后续文本、跨桶结果副本和参数记忆风险是否全部进入 taint 审计；
- outcome permutation 后 pool／panel／mapping／prediction hash 是否逐字节不变，canary 预揭封读取是否为 0；
- 是否一次揭封冻结 validation panel，并按 event cluster 联合评分多切点，不把相关切点伪装成独立样本；
- 是否逐一比较 \(H_C\)、每个 material rival 与强 \(p_{base}\)，而不是用混合 \(p_{SOS}\) 或揭封后 best-of-many 代表焦点机制成立；
- 是否在固定 coverage \(c^*\) 报告 precision／选择风险、AURC、空结果和失败率，既不惩罚合理 abstention，也不允许全空获胜；
- DSPT 是否先做历史 rolling-origin；真实 shadow 是否 `never_shown`、不回流当前调用，并把失访／censoring 单独报告；
- 是否把来源事件外验证或 target shadow 误写成目标因果、独立复制、可见后效用或行为建议；
- WIT／CUT 是否只能收窄 DSR-CT 通过候选，不能救活独立复制失败；
- 结果可见、seed self-validation 与全文 embedding 臂是否只标记为泄漏／拟合正控，不参与有效方法排名；
- 是否仍然尊重“记忆不能替代思考”；
- 是否有任何字段会被误当作新真源；
- 是否偷偷给共激活边增加了关系类型；
- 是否存在不明确请求也自动触发 Spark 的路径；
- 是否把模型推断写成了真实因果；
- 是否允许 unknown 和空结果；
- 是否保证候选 CEF 在 query 到达前已同时提交排除运行元数据的 `cef_payload_digest` 与包含它的完整 `cef_commitment_hash`，Need／claim 在读取候选前提交，并能由原文 span、source hash、模型／prompt／seed／解码／时间戳链重建；
- 是否禁止 Transport program 发明原文不存在的边、删除 required 证据、翻转响应或事后添加隐变量；
- 是否保存所有预注册 codebook×合法 completion×\(\epsilon_{max}\) 的并集版本空间，而不是把单一 argmin 或 LLM 首答当成唯一映射；
- 是否先在全部版本成员上独立发布结构 claim，再仅对 signed-eligible 样本要求每成员 \(\mathcal D^{fit}\cap\mathcal D^{hold}\) 为同一已知 singleton；方向未知是否仍能保留已通过的 unsigned 结构；
- 是否用同预算 flat／post-hoc 单层 gate 做 leave-one-out，并分别报告 unsigned／signed coverage 与 error，以检验嵌套发布而不是把它写进“完整联合”却不消融；
- 是否主动搜索最强竞争程序并把带搜索边界的反例审计、solver gap 和未决分歧放入 Witness Bundle；
- 是否把“未找到反例”和“证明无反例”严格区分；
- 是否分别使用结构／方向 claim-level matched null，冻结跨候选与 claim type 的 multiplicity，并严格保证 complete-null pool gate 不定位任何具体候选；只有有效 candidate-level 控制才允许 bundle passed；
- 是否由不调用 LLM、代码路径分离的 `wit-check` 重放 hash、witness、completion／codebook 覆盖、solver、claim、方向交集与 null 证书，并通过故障注入；
- 扩大合理 DSL／代码本／completion／\(\epsilon\) 后是否只能保持或削弱 claim，绝不能反而增信；
- 是否把表层远与机制近设为不可互偿双门，而不是新的加权总分；
- 是否用 Need Frame 增量价值验证候选对当前问题有用，而不把通用结构相似误当灵感；
- 是否明确向量检索只负责召回、去重和多样性，而不单独决定同向、逆向、矛盾或无对应；
- 是否在候选响应可见前冻结轴的正向端点、方向签名、有限 mixed 分组库和 `internal_audit_heldout`，并把一次揭封的 `external_challenge` 独立持有；
- 是否明确区分逆向、逻辑矛盾、无支持对应、情感正负和负控制；
- 是否在符号不稳定、非单调或证据被删时返回 unknown，而不是强迫二分类；
- 是否把有符号方向机制门与候选选择主终点分别检验，并报告完整方向混淆矩阵；
- 是否保证所有方向标签只对当前查询有效，不回写桶、条目或共激活边；
- 是否把置信度误用于自主拒绝；
- 是否对稀有单例灵感过度否决；
- 是否能用最小实验独立验证核心创新；
- 是否在 WIT-core 前完成固定 18 条的真实文本证据充分性与查询无关 CEF 审计，并把危险相邻论文的忠实／oracle 复现与 OB label-free 基线分开；
- 是否已经冻结强制四联体的目标内配对主终点、StructuralPairAUC／四分项／零候选与开放库门、\(\Delta_{CUT}\)、CMI 组合及归因对照、失败记分和全部分层数值门禁；
- 是否明确区分 TextEvidenceCoverage、UsableEpisodeRate、PairResponseCoverage、RejectionEvidenceCoverage 和 OutputCoverage；
- 是否有明确失败和停止标准；
- 是否在确认集前冻结方法；
- 是否把算法内部审计与外部挑战探针真正隔离；
- 是否明确现有 60 例、校准集、确认集和复制集的不同职责；
- 是否把多 seed 只当重复测量，并报告跨家族共同假阳性；
- 是否把阶段 2–6 明确标为再次批准后才存在的条件路线；
- 是否用 2×2 表层 × 机制四联体检验效应是否与结构因子一致，而不是只由主题、情绪、叙事流畅度或提示迎合解释；
- 是否以严格时间切分的自然后续事件做反自证见证，并明确它仍不是因果证明；
- 是否避免把已有的 MDL、图匹配、GW、因果干预或选择性预测单项包装成首创；
- 是否保留当前模型最终决定权。

## 22. 参考文献

以下按与本方案的关系列出，不表示所有论文具有相同证据等级。`[同行评审]` 表示正式期刊或会议论文，`[技术报告]` 与 `[预印本]` 不能按同等证据强度使用；2026 年工作尤其需要在实施前再次核验版本和发表状态。

### 22.1 经典类比检索与认知研究

1. `[同行评审]` Gentner, D., Rattermann, M. J., & Forbus, K. D. (1993). The Roles of Similarity in Transfer: Separating Retrievability From Inferential Soundness.  
   https://doi.org/10.1006/cogp.1993.1013

2. `[同行评审]` Forbus, K. D., Gentner, D., & Law, K. (1995). MAC/FAC: A Model of Similarity-Based Retrieval.  
   https://doi.org/10.1207/s15516709cog1902_1

3. `[同行评审]` Falkenhainer, B., Forbus, K. D., & Gentner, D. (1989). The Structure-Mapping Engine: Algorithm and Examples.  
   https://doi.org/10.1016/0004-3702(89)90077-5

4. `[同行评审]` Gentner, D., Loewenstein, J., Thompson, L., & Forbus, K. D. (2009). Reviving Inert Knowledge: Analogical Abstraction Supports Relational Retrieval of Past Events.  
   https://doi.org/10.1111/j.1551-6709.2009.01070.x

5. `[技术报告]` Finlayson, M. A., & Winston, P. H. (2006). Analogical Retrieval via Intermediate Features: The Goldilocks Hypothesis. MIT-CSAIL-TR-2006-071.  
   https://courses.csail.mit.edu/6.803/pdf/finlayson.pdf

6. `[同行评审]` Raynal, L. (2025). ADAPTER: A Conceptual Model of Category-Driven Analogical Retrieval.  
   https://doi.org/10.1002/wcs.70005

7. `[同行评审]` Lee, H. S., & Holyoak, K. J. (2008). The Role of Causal Models in Analogical Inference. Journal of Experimental Psychology: Learning, Memory, and Cognition, 34(5), 1111–1122.  
   https://doi.org/10.1037/a0012581

### 22.2 LLM 类比与叙事基准

8. `[同行评审]` Cheng et al. (2023). StoryAnalogy: Deriving Story-level Analogies from Large Language Models to Unlock Analogical Understanding.  
   https://aclanthology.org/2023.emnlp-main.706/

9. `[同行评审]` Sourati et al. (2024). ARN: Analogical Reasoning on Narratives.  
   https://aclanthology.org/2024.tacl-1.59/

9a. `[公开数据记录／CC-BY-4.0]` Sourati et al. (2024). ARN v1 dataset record. [Zenodo 11044026](https://zenodo.org/records/11044026)

9b. `[同行评审／来源数据论文]` Ghosh & Srivastava (2022). [ePiC: Employing Proverbs in Context as a Benchmark for Abstract Language Understanding](https://aclanthology.org/2022.acl-long.276/)

10. `[同行评审]` Ye et al. (2024). AnaloBench: Benchmarking the Identification of Abstract and Long-context Analogies.  
    https://aclanthology.org/2024.emnlp-main.725/

11. `[同行评审]` Qin et al. (2025). Relevant or Random: Can LLMs Truly Perform Analogical Reasoning?  
    https://aclanthology.org/2025.findings-acl.1230/

12. `[同行评审]` Zhang & Lyu (2025). Can Language Models Serve as Analogy Annotators?  
    https://aclanthology.org/2025.findings-acl.819/

13. `[同行评审／任务论文]` Hatzel et al. (2026). SemEval-2026 Task 4: Narrative Story Similarity and Narrative Representation Learning.  
    https://aclanthology.org/2026.semeval-1.429/

14. `[同行评审／系统论文]` Erana et al. (2026). COGNAC at SemEval-2026 Task 4: Evaluating Narrative Components with LLMs for Hard Story Similarity Cases.  
    https://aclanthology.org/2026.semeval-1.290/

15. `[同行评审]` Barakat & Kochmar (2026). Teaching Through Analogies: A Modular Pipeline for Educational Analogy Generation.  
    https://aclanthology.org/2026.bea-1.59/

16. `[预印本]` Khojasteh et al. (2026). Enhancing Structural Mapping with LLM-derived Abstractions for Analogical Reasoning in Narratives.  
    https://arxiv.org/abs/2603.29997

17. `[预印本]` Chen et al. (2026). Analogical Deep Research: Retrieving and Integrating Historical Analogies for Foresight Analysis.  
    https://arxiv.org/abs/2607.13602

### 22.3 相邻方法基础

18. `[同行评审]` Qin et al. (2019). Counterfactual Story Reasoning and Generation. TIMETRAVEL provides controlled counterfactual story rewrites.  
    https://aclanthology.org/D19-1509/

19. `[同行评审]` Veitch et al. (2021). Counterfactual Invariance to Spurious Correlations in Text Classification.  
    https://proceedings.neurips.cc/paper/2021/hash/8710ef761bbb29a6f9d12e4ef8e4379c-Abstract.html

20. `[同行评审]` Geiger et al. (2025). Causal Abstraction: A Theoretical Foundation for Mechanistic Interpretability.  
    https://www.jmlr.org/papers/v26/23-0058.html

21. `[同行评审]` D’Acunto et al. (2025). Causal Abstraction Learning based on the Semantic Embedding Principle.  
    https://proceedings.mlr.press/v267/d-acunto25a.html

22. `[同行评审]` Solar-Lezama (2013). Program Sketching. This paper describes counterexample-guided inductive synthesis in program synthesis.  
    https://doi.org/10.1007/s10009-012-0249-7

补充的直接程序综合近邻（不参与全文连续编号）：

- `[同行评审]` Wang, Y., Wang, X., & Dillig, I. (2018). Relational Program Synthesis（Relish）. 该工作已经联合关系版本空间学习、领域 DSL 与 CEGIS；WIT 不主张这套计算骨架本身为原创。  
  https://doi.org/10.1145/3276525
- `[预印本]` Kan, S. (2026). Claim-Selective Certification for High-Risk Medical Retrieval-Augmented Generation. 该工作已经把回复分成可核查 claims，并产生 full／partial／conflict／abstain 的证据链接选择；WIT 不主张 claim-selective certification 本身原创。  
  https://arxiv.org/abs/2605.21949
- `[预印本]` Qiu, J., Han, Z., & Huang, C. (2026). SURE-RAG: Sufficiency and Uncertainty-Aware Evidence Verification for Selective Retrieval-Augmented Generation. 该工作已把 claim–passage 局部关系聚合为集合级 evidence sufficiency、conflict、uncertainty 与风险—覆盖选择性决定；它是 WIT 语义证据门的强直接近邻。  
  https://arxiv.org/abs/2605.03534
- `[预印本]` Ji, Y., Kwak, M. G., Zhang, H., Wu, X., Li, C., & Wang, Y. (2026). MedRAGChecker: Claim-Level Verification for Biomedical Retrieval-Augmented Generation. 该工作已联合原子 claim、证据 NLI 与生物医学 KG 一致性做支持／矛盾诊断；其 KG／蒸馏资源若不能忠实迁移到 OB，应记资源不匹配而不是失败。  
  https://arxiv.org/abs/2601.06519
- `[预印本]` Koomullil, G. (2026). Proof-Carrying Certificates for LLM Pipelines: A Trust-Boundary Architecture. 该工作已把 LLM 周边的确定性计算置于独立信任边界，并提出 per-call certificate、emission gate 与 Maximal Certifiable Residue；`wit-check` 和“降级到最大可认证残留”都不能单独构成 WIT 的新颖性。  
  https://arxiv.org/abs/2605.16407

23. `[预印本]` Scherrer et al. (2021). Learning Neural Causal Models with Active Interventions.  
    https://arxiv.org/abs/2109.02429

24. `[预印本]` Srivastava (2026). Causal Intervention-Based Memory Selection for Long-Horizon LLM Agents. This is the closest direct memory-selection neighbor and must receive dedicated comparison.  
    https://arxiv.org/abs/2605.17641

25. `[预印本]` An et al. (2026). Cycle-Consistent Search: Question Reconstructability as a Proxy Reward for Search Agent Training. This is adjacent search-agent reconstruction work, not direct analogical retrieval.  
    https://arxiv.org/abs/2604.12967

26. `[同行评审]` Lu et al. (2026). Structured Episodic Event Memory.  
    https://aclanthology.org/2026.acl-long.277/

27. `[预印本]` Shu et al. (2026). REMem: Reasoning with Episodic Memory in Language Agent.  
    https://arxiv.org/abs/2602.13530

### 22.4 向量关系与方向判定边界

28. `[同行评审]` Mrkšić et al. (2016). Counter-fitting Word Vectors to Linguistic Constraints.<br>
    https://aclanthology.org/N16-1018/

29. `[同行评审／教程]` Glavaś, Ponti, & Vulić (2019). Semantic Specialization of Distributional Word Vectors.<br>
    https://aclanthology.org/D19-2007/

30. `[同行评审]` Finley, Farmer, & Pakhomov (2017). What Analogies Reveal about Word Vectors and their Compositionality.<br>
    https://aclanthology.org/S17-1001/

补充的方向／矛盾检索基线（不参与全文连续编号）：

- `[同行评审]` Xu, H., Lin, Z., Chang, K.-W., Sun, Y., & Indyk, P. (2025). Contradiction Retrieval via Contrastive Learning with Sparsity（SparseCL）. 该工作说明专门训练的向量检索可以保留细微矛盾，因此 WIT/CUT 必须与其公平比较。  
  https://proceedings.mlr.press/v267/xu25s.html

### 22.5 结构迁移、图匹配与可适配性检索

31. `[早期论文]` Cornuéjols, A. (1996). Analogy as Minimization of Description Length. 该工作已经把类比表述为有限表示下的变换压缩，因此 WIT 不主张“用 MDL 选类比”本身为创新。  
    https://antoinecornuejols.github.io/publications/PUBLIES/1996-analogy-chap.pdf

32. `[同行评审]` Smyth, B., & Keane, M. T. (1998). Adaptation-guided retrieval: questioning the similarity assumption in reasoning. 该工作直接指出最相似案例不必是最可复用案例。  
    https://www.sciencedirect.com/science/article/pii/S0004370298000599

33. `[同行评审]` Sultan, O., & Shahaf, D. (2022). Life is a Circus and We are the Clowns: Automatically Finding Analogies between Situations and Processes.  
    https://aclanthology.org/2022.emnlp-main.232/

34. `[同行评审]` Chen et al. (2020). Graph Optimal Transport for Cross-Domain Alignment.  
    https://proceedings.mlr.press/v119/chen20e.html

35. `[同行评审]` Xu et al. (2019). Gromov-Wasserstein Learning for Graph Matching and Node Embedding.  
    https://proceedings.mlr.press/v97/xu19b.html

36. `[预印本]` Bai et al. (2025). Fused Partial Gromov-Wasserstein for Structured Objects. 部分质量传输是 WIT 的危险数学近邻之一。  
    https://arxiv.org/abs/2502.09934

37. `[同行评审]` Verma et al. (2025). GRAIL: Graph Edit Distance and Node Alignment using LLM-Generated Code. 该工作表明“LLM 生成可解释图匹配程序”也不是 WIT 可单独主张的创新。  
    https://proceedings.mlr.press/v267/verma25a.html

38. `[同行评审]` Ryu et al. (2025). Cross-modality Matching and Prediction of Perturbation Responses with Labeled Gromov-Wasserstein Optimal Transport. 该工作是“结构传输 + 干预响应预测”的重要相邻证据，但领域和发布门不同。  
    https://proceedings.mlr.press/v258/ryu25a.html

39. `[同行评审]` Beckers, S., & Halpern, J. Y. (2020). Approximate Causal Abstractions.  
    https://proceedings.mlr.press/v115/beckers20a.html

40. `[预印本]` Liu et al. (2026). Beyond Semantic Relevance: Counterfactual Risk Minimization for Robust Retrieval-Augmented Generation.  
    https://arxiv.org/abs/2605.01302

41. `[预印本]` Xiao et al. (2026). Learning to Reason by Analogy via Retrieval-Augmented Reinforcement Fine-Tuning.  
    https://arxiv.org/abs/2606.13680

42. `[预印本]` Ding et al. (2026). Towards Root Memories: Benchmarking and Enhancing Implicit Logical Memory Retrieval for Personalized LLMs.  
    https://arxiv.org/abs/2606.23283

#### 22.5.1 补充的直接近邻（不参与全文连续编号）

- `[同行评审]` Akifuji, Y., & Tsuji, S. (1995). Application of Version-Space Method for Case Retrieval and Indexing. 该工作已用 version space 表示“可直接复用／可能可解”的案例条件并随成功失败更新索引；WIT 不主张 version-space case retrieval 或增量索引本身原创。  
  https://ipsj.ixsq.nii.ac.jp/records/14021
- `[同行评审]` Fanizzi, N., d’Amato, C., & Esposito, F. (2007). Instance-based Retrieval by Analogy. 该工作已把 disjunctive version space 与 semantic difference 用于实例式类比检索和类成员预测；WIT 必须以 DiVS-style 自然文本 reconstruction／adaptation arm 正面比较，不能把“version space 用于类比”当作差异。  
  https://doi.org/10.1145/1244002.1244303
- `[同行评审]` Rajesh, S., Holur, P., Duan, C., Chong, D., & Roychowdhury, V. (2026). Beyond Fact Retrieval: Episodic Memory for RAG with Generative Semantic Workspaces. GSW 已经构建可解释、时空与逻辑一致的情节工作区；WIT 不主张结构化 episodic workspace 本身原创。  
  https://ojs.aaai.org/index.php/AAAI/article/view/40557
- `[预印本]` Rajesh, S., Holur, P., Turali, M. Y., Duan, C., & Roychowdhury, V. (2026). Panini: Continual Learning in Token Space via Structured Memory. Panini 已把写时 GSW 整合与查询时 inference-chain retrieval 连成完整外部语义记忆；它是 WIT 双承诺与只读边界最危险的表示／检索基线之一。  
  https://arxiv.org/abs/2602.15156
- `[同行评审]` Tan, C. H.-M., Subagdja, B., & Tan, A.-H. (2026). ARTEM: Enhancing Large Language Model Agents with Spatial-Temporal Episodic Memory. ARTEM 已经覆盖时空事件抽取、向量化存储与 partial-cue retrieval；WIT 不能把事件结构化与向量 sidecar 当作新意。  
  https://ojs.aaai.org/index.php/AAAI/article/view/39773
- `[同行评审]` Gou, Q., Dong, Y., & Ke, Q. (2024). SynthoMinds: Bridging human programming intuition with retrieval, analogy, and reasoning in program synthesis. 该工作已组合 retrieval、analogy 与 program synthesis；WIT 的研究主张必须窄化到个人情节跨域 transport 的具体承诺和发布协议。  
  https://www.sciencedirect.com/science/article/abs/pii/S0164121224001857
- `[预印本]` Choraria et al. (2026). Context-Gated Associative Retrieval: From Theory to Transformers. 它支持“上下文改变召回能量景观”的价值，因此 context／Need gating 本身不是 WIT 独占创新。  
  https://arxiv.org/abs/2605.10970
- `[同行评审]` Li et al. (2025). Past Meets Present: Creating Historical Analogy with Large Language Models.  
  https://aclanthology.org/2025.acl-long.200/
- `[同行评审]` Xia & Bareinboim (2025). Causal Abstraction Inference under Lossy Representations.  
  https://proceedings.mlr.press/v267/xia25a.html
- `[预印本]` Nagy et al. (2025). Analogy making as amortised model construction. 这项工作进一步说明“类比作为模型构造／部分同态”的视角已有直接先例。  
  https://arxiv.org/abs/2507.16511
- `[预印本]` Coupette & Vreeken (2021). Graph Similarity Description: How Are These Graphs Similar? 该工作与“输出可解释结构相似描述”直接相邻。  
  https://arxiv.org/abs/2105.14364

### 22.6 弃权、反事实稳健性与反自证

43. `[同行评审]` Gangrade, Kag, & Saligrama (2021). Selective Classification via One-Sided Prediction. `unknown`／abstain 的准确率—覆盖率权衡具有成熟先例。  
    https://proceedings.mlr.press/v130/gangrade21a.html

44. `[同行评审]` Bao et al. (2025). CAP: A General Algorithm for Online Selective Conformal Prediction with FCR Control.  
    https://www.jmlr.org/papers/v26/24-0452.html

45. `[同行评审]` Chen et al. (2024). Controlling Risk of Retrieval-augmented Generation: A Counterfactual Prompting Framework.  
    https://aclanthology.org/2024.findings-emnlp.133/

46. `[同行评审]` Wang et al. (2024). Backtracing: Retrieving the Cause of the Query.  
    https://aclanthology.org/2024.findings-eacl.48/

47. `[同行评审]` Yang et al. (2026). Quantifying and Improving the Robustness of Retrieval-Augmented Language Models Against Spurious Features in Grounding Data.  
    https://aclanthology.org/2026.acl-long.1545/

48. `[同行评审／统计方法]` Grünwald, de Heide, & Koolen (2024). Safe Testing. 其 anytime-valid／安全检验思想用于提醒本方案预注册序贯规则，并不是 WIT 的类比机制依据。  
    https://academic.oup.com/jrsssb/article/86/5/1091/7623686

补充的测试方法近邻（不参与全文连续编号）：

- `[同行评审]` Hyun, Guo, & Babar (2024). METAL: Metamorphic Testing Framework for Analyzing Large-Language Model Qualities. WIT 的文本变形测试属于已有 metamorphic-testing 思路的领域化使用。  
  https://arxiv.org/abs/2312.06056

### 22.7 WIT-VS 原方案的首创性检索边界（保留作条件审计）

截至 2026-08-02，本轮检索覆盖 ACL Anthology、PMLR／JMLR、arXiv、AAAI／主要出版商页面以及关键词级 Google Patents 初筛。检索到的组成项先例包括：结构映射、过程类比自动抽取、LLM 派生抽象、MDL 变换、GED、GW／partial-GW、干预响应对齐、因果记忆选择、结构化情节工作区、写时语义记忆整合、推理链检索、version-space case retrieval、disjunctive version-space analogy、semantic-difference retrieval、RAG 反事实风险控制、claim-selective／set-level evidence sufficiency、proof-carrying checker、选择性预测与弃权。

在本轮非系统检索所覆盖的这些公开一手来源中，尚未定位到把**个人情节记忆的远类比筛选**具体操作化为以下完整联合协议的同构方法：查询前 actual-payload CEF 与候选盲 Need／有限 claim lattice 的**双时序承诺**；逐操作双侧 witness 的有限跨域迁移语法；对所有预注册 codebook×证据 completion×最大合理 \(\epsilon\) 包络取并集版本空间；结构 claim 先在盲封留出上全体一致，方向 claim 再由独立响应证据与 CUT 嵌套发布；类内 rival／unknown 决定是否只能降级。candidate-level null、matched-null、确定性 checker 与 Witness Bundle 是必要的发布／验证门，不是算法原创点，也不能被纳入“首创联合”来人为增加独特性。

这是一项**关于完整联合之非冗余增量的、截至检索日的可证伪新颖性假设**，不是已经识别全部高阶交互。Akifuji–Tsuji 与 Fanizzi 等已把 version space／disjunctive version space 直接用于案例或类比检索；Relish 已有关系版本空间／DSL／CEGIS；RootMem、GSW、Panini 与 ARTEM 已覆盖结构化或隐式逻辑记忆表示与检索；CMI 已直接研究记忆干预选择；Claim-Selective Certification、SURE-RAG 与 MedRAGChecker 已覆盖逐 claim／集合充分性语义证据门，Proof-Carrying Certificates 已覆盖独立 deterministic checker、per-call 证书和主张降级；SynthoMinds、YARN、过程类比、GED／GW 等已覆盖“检索＋类比／程序综合”或结构映射的重要部分。因此唯一可辩护的对象只是“双承诺 × 逐操作跨域 witness transport × 并集版本空间盲封一致 × 结构／方向嵌套候选级发布”这一联合的**非冗余增量假设**，且必须由第 15.6 节两个近邻区分性预测、两个局部 2×2、包含 flat gate 的 leave-one-out 以及最近邻复合对手支持；这些实验不能被概括成完整联合的全部交互效应。上述结论不是系统综述结论、世界首创证明、专利新颖性意见或自由实施分析。正式投稿、公开宣传或知识产权动作前必须再次做引用链、关键词与专利系统检索，并由独立专业人员复核。

### 22.8 SOS-PAR 的直接先例与组件新颖性边界

以下来源直接约束新版主张：

1. `[同行评审／会议论文]` Johnson, H. M., & Seifert, C. M. (1990). [Predictive Utility in Case-Based Memory Retrieval](https://escholarship.org/uc/item/6nk4c45v). 研究包含结果预测线索、但不含结果的部分情节模式能否触发召回；它不是对已存储来源结果实施 ACL 隔离。

2. `[同行评审／AAAI]` Simoudis, E., & Miller, J. S. (1990). [Validated Retrieval in Case-Based Reasoning](https://cdn.aaai.org/AAAI/1990/AAAI90-048.pdf). 已提出先检索再验证候选，否定“首个检索后验证门”主张。

3. `[同行评审／综述]` Badra, F., & Lesot, M.-J. (2023). [Case-based Prediction – A Survey](https://doi.org/10.1016/j.ijar.2023.108920). 说明从历史情境—结果案例预测新情境本身是成熟 CBR 任务。

4. `[工作坊论文／预印本]` Ye, X., Zhao, Z., Leake, D., Wang, X., & Crandall, D. (2021). [Applying the Case Difference Heuristic to Learn Adaptations from Deep Network Features](https://arxiv.org/abs/2107.07095). 从问题差异预测方案／解差异，是算法近邻，但不直接预测个人叙事结果。

5. `[同行评审／会议论文]` Hanney, K., & Keane, M. T. (1996). [Learning Adaptation Rules from a Case-Base](https://doi.org/10.1007/BFb0020610). 是 Case Difference Heuristic 的原始强先例之一。

6. `[同行评审]` Lee, H. S., & Holyoak, K. J. (2008). [The Role of Causal Models in Analogical Inference](https://doi.org/10.1037/a0012581). 已说明角色映射后的因果模型运行会影响跨域推断。

7. `[预印本]` Chen et al. (2026). [Analogical Deep Research: Retrieving and Integrating Historical Analogies for Foresight Analysis](https://arxiv.org/abs/2607.13602)（框架名：Causal Analogical Researcher，CANA）. 已把机制分解、结构对齐与多类比确认用于历史类比前瞻；它读取完整来源结果，没有“同一来源结果 ACL 盲封—竞争概率承诺—一次揭封评分”门。

8. `[同行评审]` Dawid, A. P. (1984). [The Prequential Approach](https://doi.org/10.2307/2981683). “先预测、后揭示、再评分”不是 SOS-PAR 发明。

9. `[同行评审／统计方法]` Rubin, D. B. (2008). [For Objective Causal Inference, Design Trumps Analysis](https://doi.org/10.1214/08-AOAS187). 观察研究设计阶段不看结果是直接方法学先例。

10. `[博士论文]` Sigweni, B. B. (2016). [An Investigation of Feature Weighting Algorithms and Validation Techniques Using Blind Analysis for Analogy-Based Estimation](https://bura.brunel.ac.uk/handle/2438/12797). 说明 blind analysis 与 analogy-based estimation 的组合已有相邻先例。

11. `[同行评审／统计方法]` Gneiting, T., & Raftery, A. E. (2007). [Strictly Proper Scoring Rules, Prediction, and Estimation](https://doi.org/10.1198/016214506000001437). 为概率承诺、Brier／log score 与校准提供规范基础。

12. `[同行评审／统计方法]` Xu, T., Chen, Y., Zeng, D., & Wang, Y. (2022). [Self-matched Learning to Construct Treatment Decision Rules from Electronic Health Records](https://doi.org/10.1002/sim.9426). 支持纵向自匹配设计的分析价值，同时提醒其依赖观察性假设。

13. `[预印本]` Asemota, A., & Hooker, G. (2024). [Longitudinal Counterfactuals: Constraints and Opportunities](https://arxiv.org/abs/2403.00105). 提供纵向反事实表示的相邻路线，但不等于个人叙事可直接识别因果。

14. `[同行评审／会议论文]` Keane, M. T., & Smyth, B. (2020). [Good Counterfactuals and Where to Find Them](https://arxiv.org/abs/2005.13997). 约束反事实质量与可行动性主张。

15. `[预印本]` [DCPM](https://arxiv.org/html/2606.09483v1) (2026). 已研究个人跨域模式发现，说明“个人记忆中的跨域模式”不是 SOS-PAR 可单独主张的新意。

16. `[同行评审／ACL]` [Experience-Following](https://aclanthology.org/2026.acl-long.27/) (2026). 已使用未来任务结果评价历史经验质量，是“结果评价记忆”的危险近邻。

17. `[预印本]` [Causal Memory Intervention（CMI）](https://arxiv.org/abs/2605.17641) (2026). 已直接研究候选记忆干预与下游效用，必须作为简单强基线。

18. `[预印本]` Ning et al. (2026). [One Run Is Not an Idea](https://arxiv.org/abs/2607.26587). 已使用冻结候选卡、结果盲忠实度审查和产物重跑，说明“冻结规范＋盲审忠实度”本身不是新意；它不涉及个人记忆或来源结果封存。

基于这些先例，SOS-PAR 只保留以下组件级陈述：

> SOS-PAR 提供一种面向个人自然语言记忆的来源结果访问隔离、竞争概率预提交与一次揭封评分协议；本轮不再把“同一来源结果验证自身映射”当作新版 Spark 的充分确认基础。

如果严格 WIT 或任一更简单基线已经全程遮蔽同一来源结果并达到相同性能，SOS-PAR 只算工程完整性条款。其价值必须由 DSR-CT 独立验证 panel 上的增量证明，不能由命名或流程复杂度推出。

### 22.9 DSR-CT／DSPT 本轮新增的直接近邻与方法依据

1. `[同行评审／International Journal of Forecasting]` Green & Armstrong (2007). [Structured analogies for forecasting](https://doi.org/10.1016/j.ijforecast.2007.05.005). 已采用多类比、相似度判断、来源 outcome→目标 outcome 映射和机械聚合；46% vs 无辅助 32%，特定多类比直接经验子组 60%（23 个 forecasts）。它是 DSPT／多类比预测的必做基线。

2. `[同行评审／Strategy Science]` Sen, Workiewicz, & Puranam (2026). [Can LLMs Aid Analogical Reasoning for Strategic Decisions? A Comparative Study](https://doi.org/10.1287/stsc.2025.0426). LLM 高 recall、低 precision且易受表层相似误导，人类则相反；直接支持把宽召回与独立适切性评价拆开。

3. `[同行评审／ICCC]` O'Donoghue & Keane (2012). [A Creative Analogy Machine: Results and Challenges](https://mural.maynoothuniversity.ie/id/eprint/3891/). 已把 creative analogy 明确拆成 retrieval、mapping 与 inference validation，并从背景记忆检查迁移推断；因此“发现后验证”不是 DSR 的新贡献。

4. `[预印本／最直接性能近邻，尚未同行评审或独立复现]` Suzuki & Banaei-Kashani (2026). [TCA-SIR: Learning Target-Conditioned Abstractions for Scientific Inspiration Retrieval](https://arxiv.org/abs/2607.28498). 针对目标抽取可迁移抽象原则并学习 graded transferability；直接约束 WitnessedTCA 的新颖性，且要求作为性能基线。

5. `[同行评审／基准]` [ResearchBench](https://aclanthology.org/2026.findings-acl.644/) (2026). 提供科学启发检索评测背景；公共科学语料不能替代 OB 个人记忆分布的锁定确认。

6. `[预印本／Microsoft Research]` [Thinking Ahead: Prospection-Guided Retrieval of Memory with Language Models](https://arxiv.org/abs/2605.14177) (2026). PGR-TOT 在 MemoryQuest 报告 Recall `0.723`，最强基线 Mem0 为 `0.256`；OB 只借用非行动性 informational-need probes，probe 上限须另做预算消融。

7. `[预印本]` Yin & Tang (2026). [DeferMem: Query-Time Evidence Distillation via Reinforcement Learning for Long-Term Memory QA](https://arxiv.org/abs/2605.22411). 高召回后做目标条件化证据蒸馏，约束双视图的新颖性。

8. `[预印本]` Wang & Dong (2026). [MGRetrieval: Memory-Guided Reflective Retrieval for Long-Term Dialogue Agents](https://arxiv.org/abs/2605.27437). 说明让已取回记忆迭代引导检索已有直接先例；DSR 首版不允许揭结果后的自适应找证据。

9. `[同行评审／TACL 综述]` Petersen, Stevenson, & van der Plas (2026). [Modelling Analogies and Analogical Reasoning: Connecting Cognitive Science Theory and NLP Research](https://aclanthology.org/2026.tacl-1.32/). 系统连接结构映射、召回、映射与迁移，强调关系理解不能退化为实体相似。

10. `[同行评审／ACL]` Garikaparthi et al. (2025). [MIR: Methodology Inspiration Retrieval](https://aclanthology.org/2025.acl-long.1390/). 方法论启发检索及图谱先验已有，不能把“inspiration retrieval”本身当首创。

11. `[同行评审／EMNLP]` [Finding your MUSE](https://aclanthology.org/2025.emnlp-main.1547/) (2025). 功能概念、问题抽象路径与跨域机制搜索已有直接近邻。

12. `[预印本／机制确认近邻]` Chen et al. (2026). [Analogical Deep Research: Retrieving and Integrating Historical Analogies for Foresight Analysis](https://arxiv.org/abs/2607.13602)（框架名：CANA）. 机制分解、结构反馈与 cross-analogy confirmation 不是 DSR 发明；DSR 的窄差异假设必须由结果盲的事件外自然验证检验，prospective target shadow 只检验外部迁移。

13. `[同行评审／EMNLP]` Zhang et al. (2020). [Analogous Process Structure Induction for Sub-event Sequence Prediction](https://aclanthology.org/2020.emnlp-main.119/). 利用相似过程预测缺失子事件，否定“masked transition prediction”单独的新颖性。

14. `[KDD 2026 accepted／arXiv]` [When Hard Negatives Hurt: Bridging the Generative-Discriminative Gap in Hard Negative Synthesis for Retrieval（CausalNeg）](https://arxiv.org/abs/2606.01304). 揭示生成 hard negatives 的 shortcut 与 generative–discriminative gap；支持 natural-first、synthetic-diagnostic-only 规则。

15. `[同行评审／EACL]` Jiang & Merlo (2026). [Analogical Structure, Minimal Contextual Cues and Contrastive Distractors](https://aclanthology.org/2026.eacl-long.22/). 这是受控语言规则诱导中的相邻对比证据，不能当个人自然记忆四元验证的直接先例或直接效力证明。

16. `[同行评审／PMLR]` Angelopoulos et al. (2023). [Recommendation Systems with Distribution-Free Reliability Guarantees](https://proceedings.mlr.press/v204/angelopoulos23a.html). 从候选排序到用户可见集合的 FDR 控制是选择发布的强统计近邻。

17. `[同行评审／JMLR]` Jin & Candès (2023). [Selection by Prediction with Conformal p-values](https://www.jmlr.org/papers/v24/22-1176.html). 预测筛选后的 false selection 控制已有；需要真实交换性与有效分数。

18. `[同行评审／ICML]` Bai et al. (2025). [Multivariate Conformal Selection](https://proceedings.mlr.press/v267/bai25d.html). 多变量候选选择与 FDR 控制已有；DSR 的多门不可直接冒充 mCS 保证。

19. `[同行评审／JMLR]` Bao et al. (2025). [CAP: Online Selective Conformal Prediction with FCR Control](https://www.jmlr.org/papers/v26/24-0452.html). 约束自适应在线选择与分布漂移下的校准表述。

20. `[同行评审／AISTATS]` Bickford Smith et al. (2023). [Prediction-Oriented Bayesian Active Learning](https://proceedings.mlr.press/v206/bickfordsmith23a.html). EPIG 是主动选择与未来预测相关证据的直接方法学依据；不是 DSR 的新贡献。

21. `[同行评审／JMLR]` Saengkyongam et al. (2024). [Effect-Invariant Mechanisms for Policy Generalization](https://jmlr.org/papers/v25/23-0802.html). 跨环境机制／效应不变性已有严格形式；自然文本响应签名不能借用其因果保证。

22. `[同行评审／PMLR]` Felekis et al. (2024). [Causal Optimal Transport of Abstractions](https://proceedings.mlr.press/v236/felekis24a.html). 干预约束下的抽象映射与传输已有；DSR 只有观察性 predictive transport。

23. `[同行评审／JMLR]` Gibbs & Candès (2024). [Conformal Inference for Online Prediction with Arbitrary Distribution Shifts](https://www.jmlr.org/papers/v25/22-1218.html). 说明时间漂移可专门处理，也提醒静态交换性保证不能直接搬到个人长期记忆。

24. `[同行评审／ACL Workshop]` [Can LLMs Recognize Their Own Analogical Hallucinations?](https://aclanthology.org/2025.knowllm-1.8/) (2025). 类比迁移阶段的不确定性集中，且模型自评不可靠，支持跨实现稳健性检查与确定性 scorer。

25. `[同行评审／INLG]` [From Prototypical to Relational: How LLMs Navigate Complex Analogies](https://aclanthology.org/2025.inlg-main.28/) (2025). 当前模型在精细关系映射上仍有限，反对把一个 LLM judge 当真值。

26. `[同行评审／Findings EMNLP]` Thakur et al. (2025). [Hard Negatives, Hard Lessons](https://aclanthology.org/2025.findings-emnlp.481/). 说明错误负标签会损害 retriever，要求自然 foil 的独立盲审和未判定敏感性分析。

27. `[预印本／自动研究可靠性]` Ning et al. (2026). [One Run Is Not an Idea](https://arxiv.org/abs/2607.26587). 支持跨实现、结果盲和重放审计；单次实现或多 seed 不能替代新数据、独立实现与新 confirmation block 的复制。

28. `[预印本／选择性记忆干预近邻]` Wu et al. (2026). [Remember When It Matters: Proactive Memory Agent for Long-Horizon Agents](https://arxiv.org/abs/2607.08716). 独立 memory agent 选择注入 grounded reminder 或沉默，并报告相对 always-on／普通 retrieval 的增益；因此“选择性注入”与“记忆提醒改善任务”都不是 DSR 新意。其行动代理与持续 memory update 不适合作为 OB 的认知／规范层。

29. `[预印本／决策相关选择近邻]` Guan, Zhao, & Deng (2026). [Decision-Aware Memory Cards: Counterfactual-Inspired Context Selection and Compression for Tool-Using LLM Agents](https://arxiv.org/abs/2606.08151). CICL 按预期行动影响、效用与负迁移风险排序 context，并在小型代码检索实验中报告 hit@1 0.58→0.78；DSR 必须把 action-shift 与方法中立 useful-far gold 分开，且不能用行动影响替模型做决定。

30. `[同行评审／ACL System Demonstration]` Latimer et al. (2026). [Hindsight: Structured Agent Memory that Retains, Recalls, and Reflects](https://aclanthology.org/2026.acl-demo.27/). 多网络结构化记忆、向量／关键词／图／时间并行召回与 reflect 已有系统实现；只能作为强 retrieval 系统近邻，不能把 opinion／behavior profile 设计带入 OB。

31. `[同行评审／EACL]` Zhou et al. (2026). [Amory: Building Coherent Narrative-Driven Agent Memory through Agentic Reasoning](https://aclanthology.org/2026.eacl-long.183/). 已用叙事结构、一致性驱动检索和离线 consolidation 改善长程记忆；对 OB 的公平适配必须保持原文可追溯、只读且不成为第二真源。

32. `[同行评审／ACL]` Yan et al. (2026). [Memory-R1: Enhancing Large Language Model Agents to Manage and Utilize Memories via Reinforcement Learning](https://aclanthology.org/2026.acl-long.583/). 已学习 ADD／UPDATE／DELETE／NOOP 与检索策略；只能在隔离测试库作学习式 memory-policy 上界，真实 OB 记忆不可因该基线被删除或改写真源。

#### 22.9.1 本轮检索结论

已有工作分别覆盖了 structured analogical forecasting、innovation by analogy、analogy finder、case-based outcome prediction、target-conditioned abstraction、prospection-guided recall、cross-analogy confirmation、decision-aware／selective memory injection、结构化长程记忆、masked process prediction、hard negatives、prequential evaluation、historical backtesting、live shadow、selective prediction 和 conformal selection。因此 DSR-CT 不能靠罗列或重命名这些模块声称首创。

当前分成两个待证伪层级。**DSR-CT 核心方法假设**是：discovery event 永不确认自身；全 run 候选在 validation outcome 的真实 ACL 揭封前冻结，并由不使用 human gold 的 runtime-equivalent 自动链在事件簇外自然记忆上接受检验。**DSPT 外部验证扩展**是：来源与历史时间外验证通过后，再以 never-shown prospective target outcome 做只影响后续版本的计分。本轮非系统检索覆盖的一手来源中未识别到同时明确具备两层完整协议的方案，但这不是世界首创、专利新颖性或独立复制结论；即使 DSPT 未执行，也只能评价 DSR 核心，不能借设计中的未来步骤抬高结果。正式投稿前仍需系统数据库、引用链、非英文文献、CPC／IPC 专利分类和独立专业复核。

## 23. 最终建议

当前最合理的决策不是立即实现完整 Spark，也不是继续寻找一个更聪明的向量距离，而是批准或否决一个更硬、也更符合“灵感”的研究问题：**一条记忆提出的新机制，能否在完全不同、从未参与发现、结果仍被盲封的自然记忆上经受事件外验证；这种来源侧信号能否在历史时间外重放中保留，并最终在不影响当前模型与用户的 prospective shadow 中再次被检验？**

建议按以下顺序执行：

截至 2026-08-03，上一版“建立全新、结果不可见、先保证 T/H 语言质量的独立成文批次”已经由 HN-F0 实际执行，而不再只是下一步建议。历史上的 AnaloBench 前瞻补批仍保留为第二停止门：开发 roster 18、官方自然 foil 0、8 个合成候选自然度盲检 `0/8`、有效 H=0。新的 HN-F0 没有回收这些样本，而是从 ARN v1 的冻结 authored-benchmark 池重新建立 26 个严格开发正例、674 个候选 H 节点和 208 条承诺边；但 singleton 语言门只留下 T `11/26`、H `71/172`、边 `45/208`。这 45 条边只覆盖 11 个唯一 T 与 42 个唯一 H，因此 T/H 唯一匹配上界为 `11<12`，触发 `STOP_HNF0_LANGUAGE_ELIGIBLE_T_UPPER_BOUND_LT_12`。单轴结构评审调用数为 0，CMI 调用数为 0，没有 N/T/H 下游回答、p 值或效应方向。

因此当前不得实现比较器、不得在同一 ARN 池中放宽语言门或用第 9 个 H／换轴／换 T 救回样本，也不得把供给门停止写成 Spark 或 CMI 的效果失败。若项目所有者以后批准继续，新的数据工作必须再次结果不可见，并把 singleton 语言质量放在关系与单轴判断之前；优先解决 `natural_observed=0` 和跨模型／人类 gold 缺失，而不是重复利用当前 authored-benchmark 开发批次。只有新的独立供给门先证明至少 12 个语言合格、T/H 唯一且四轴各不少于 2 的可用三元组，才有资格另写单轴／CMI 协议；当前状态不授权 CMI。该下一步现已具体化为桌面的 [HN-F1 自然来源与语言优先供给协议](./OB_HN-F1_自然来源与语言优先供给协议_2026-08-03.md)，但仍须项目所有者填写来源、隐私、评审与预算字段后另行批准，不能把“写完协议”解释为“获准执行”。

1. 把 **DSR-CT** 设为新版 Spark 自动筛选的主研究假设：发现 seed 可以提出新 \(H_C\)，但该 seed 及同事件切片永不计入确认；来源证据只能来自 event-cluster 外的结果盲自然记忆；
2. 把 **SOS-PAR** 降为 validation memory 的结果防火墙与预序评分组件；把 **DSPT** 设为来源核心通过后的外部迁移验证扩展；WIT-VS／Spark-CUT 继续只是有独立增量时的结构／方向审计；
3. 首先只批准阶段 0D-A 的 60-query 只读可行性，不建 harness、不揭结果、不训练、不改生产、不接 MCP、不写真实 vault；
4. 严格索引只由 cutoff 前原文的独立 builder 生成；标题、结果摘要、全文 embedding、后续文本、结果驱动标签、跨桶副本、缓存和模型会话全部进入 taint 审计；
5. 使用 `BlindSourceLedger + 临时 WitnessedTCA`：前者查询无关、不可变，后者目标条件化但只能读盲态 ledger、每条主张回链 span、揭封前 hash 冻结且不持久化；
6. 允许不超过预冻结 \(K_{probe}\) 个非行动性 Need-Path probes 改善低重叠召回，\(K_{probe}\) 由独立 calibration 的 recall—噪声—成本消融决定；与直召回并行消融，不得预测用户下一步、生成计划、拒绝或 permit；
7. discovery seed 产生 MechanismCard 后立即冻结角色／变化映射、方向轴、边界、最不利 rivals、验证策略和概率；seed 及同事件切片永不进入确认统计；
8. validation retrieval 优先寻找自然 `analogue / bridge / foil / null`，合成扰动只作 `metamorphic_only`；多切点按事件簇联合计分，不能伪独立；
9. 同一 query 的全部候选、panel、binding、rivals、概率、排序和候选数必须先形成 run-level manifest，再对去重 outcome 并集统一揭封；任何逐候选揭封流程均无效；
10. 对 \(H_C\)、每个 material rival、强 query-free baseline 和 query-aware-no-discovery control 分别提交概率；analogue+bridge 与 bridge-only 分别使用不可互偿的 proper-loss advantage，foil／null 用结果盲 applicability、分 subtype 边界签名与假发布率，不再使用方向错误的混合特异性分数；
11. 唯一自动筛选主效应是冻结 \(c^*\) 上相对预冻结同预算 \(base^*\) 的 query-block paired \(\Delta_{PF}\)；绝对 precision≥0.80 是安全 floor。自动选择指标 \(I_{qm}\) 必须来自不接触 human gold 的 runtime-equivalent 自动链，方法盲人工 gold 只产生标签 \(Y_{qm}\)，不得参与候选选择、排序、panel pass、证书或 replay winner；分母区分 release precision 与 eligible-query coverage；样本量由功效／精度模拟决定，30 只作 feasibility floor；
12. 先忠实复现 Green–Armstrong structured analogies、Creative Analogy Machine-style validation、TCA-SIR、PGR、CANA、MUSE、CMI、Remember When It Matters、CICL、Hindsight／Amory read-only、simple CBR、BM25／dense、旧 SOS-PAR，以及 `PGR+TCA+simple CBR` 便宜复合臂；简单方法没有预注册实质劣势就删除 DSR 的无增量复杂度；
13. 来源侧事件外验证通过后先做历史 rolling-origin；历史目标预测没有剩余 proper-score 技能就停止产品主张；通过后先做新 confirmation block，再分别审批完全不可见的真实 future shadow 与可见三臂效用盲评；二者使用不同目标单元，互不污染；
14. 真实 shadow 必须 `never_shown`，未来结果只更新严格晚于揭封时间的版本；任一 Spark 内容可见后，当前 query×endpoint 的全部候选后续均视为可能 post-treatment；
15. 多模型不同 seed 不算独立复制；跨模型家族只能称 robustness check，因为仍可能共享语料、标签、代码和 evaluator。真正复制还需要新数据／时间块、独立实现或团队和冻结 confirmation；
16. 输出最多是一条“来源侧预提交预测优于冻结 rival；这不说明当前情境成立，可自行检查边界 B”的可忽略问题；不得写成目标因果、事实真源、行动建议、拒绝、permit、计划或认知画像；
17. 产品仍只通过现有调用的显式 `inspiration=true` 触发，不新增 MCP 工具；`inspiration=false` 完全旁路；`run_mode` 参数不构成授权，必须验证绑定 stage／protocol／model／renderer／vault 的 `approved_product_receipt`；不写记忆真源、关系类型或共激活权重；
18. 第 16.0D 的自动筛选、观察性对照特异性预测、假发布、历史迁移与目标价值门，以及全部安全 veto、外部复制都通过后，才讨论代码版本、热更新、公开 benchmark 或论文。

对效果的现实预期应写清：DSR-CT 主要针对两个当前痛点——**表层近的假朋友**与**表层远但存在事件外机制见证的候选**。目标条件化见证透镜和 Need-Path 预计改善 recall，事件外自然对照和 material-rival tournament 预计改善 precision，rolling-origin／prospective shadow 则分别检验历史与前瞻目标迁移。代价是更高数据需求、延迟、成本与 abstention。任何一项预期都必须由数字验证，不能由设计精巧推出。

如果真实 OB 文本没有足够的事件簇外自然材料、全盲后召回回到 null、事件外验证信号消失、目标 rolling-origin 没有剩余技能，或简单基线等效，这不是继续堆 WIT／CUT／主动探针的理由，而是说明当前自动 Spark 的证据基础尚未成立。

一句话总结：

> 新 Spark 不再先问“两段文本像不像”，也不允许启发自己的故事证明自己；它先让一条结果盲记忆提出新机制，再把同一 run 的全部候选整体冻结，用事件簇外的自然反例与远机制见证做结果盲验证，随后以历史时间外重放和真正 never-shown 的 prospective 第二封存分别检验迁移。只有方法版本先通过共同 gold 的锁定确认，当前候选再由不接触 human gold 的 runtime-equivalent 自动链通过固定覆盖率风险、历史目标价值与授权门，它才可能作为一个可忽略的核查问题出现；它始终不替模型思考、判断或行动。
