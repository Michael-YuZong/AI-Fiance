# 外审经验沉淀

这份文件不是泛泛的“复盘记录”，而是当前已经被采纳、并要求持续内化到生成系统里的外审经验。

原则：

1. 外审不是长期兜底手段，而是收敛系统的反馈回路。
2. 只要某条外审意见被认定值得长期采纳，就不能只修当前稿。
3. 这类意见必须至少落到两处之一：
   - 代码门禁 / 评分逻辑 / 渲染逻辑 / 数据链路
   - 本文件的 lesson registry，用于后续继续编码成门禁
4. 目标是让未来报告在生成时就天然避开这些问题，而不是每次都靠外审返工。

## 当前已固化 lessons

| Lesson ID | 主题 | 作用层 |
| --- | --- | --- |
| `L001` | 终稿不得暴露内部过程 | `release_check` |
| `L002` | 终稿必须是解释版 | `release_check` |
| `L003` | 重复模板理由视为解释不合格 | `release_check` |
| `L004` | 盘中/竞价语言必须有执行层证据 | `release_check` |
| `L005` | 基金画像缺失不允许过稿 | `release_check` |
| `L006` | 商品/期货 ETF 不能套股票估值框架 | `opportunity_engine` |
| `L007` | ADX 不能脱离 DI 方向解释 | `opportunity_engine` |
| `L008` | ETF/基金前瞻催化不能混入无关公司财报 | `opportunity_engine` |
| `L009` | final 导出前必须有独立外审 PASS | `report_guard` |
| `L010` | 催化面必须结构化事件优先，没新闻不等于没催化 | `opportunity_engine` |
| `L011` | 新闻/事件源降级不能把推荐系统打成假阴性 | `opportunity_engine` / `release_check` |
| `L012` | 市场级筹码数据不能伪装成个股级筹码优势 | `opportunity_engine` / `external_review` |
| `L013` | 合并稿必须披露数据完整度 | `release_check` / `renderer` |
| `L014` | 催化证据要能在成稿中直接复核 | `renderer` / `external_review` |
| `L015` | A股结构化公司事件应优先使用 Tushare 而不是新闻标题猜测 | `opportunity_engine` / `external_review` |
| `L016` | A股资本结构与执行层风险应优先使用 Tushare 结构化数据 | `opportunity_engine` / `external_review` |
| `L017` | 历史相似样本置信度必须说明样本边界并严控数据置信度 | `release_check` / `external_review` |
| `L018` | 正式推荐的过线逻辑与边界案例必须对客户讲清楚 | `external_review` / `renderer` |
| `L019` | 历史相似样本只能作为辅助验证，不能冒充严格回测 | `external_review` / `renderer` |
| `L020` | 跨市场覆盖率不均时必须明确不同市场的参考强弱 | `external_review` / `renderer` |
| `L021` | 同一事件若出现数值冲突，成稿必须统一口径或显式解释差异 | `external_review` / `renderer` |
| `L022` | 结构化事件必须做新鲜度控制，陈旧事件不能按满额催化计分 | `opportunity_engine` / `external_review` |
| `L023` | 样本置信度必须和总推荐置信度分开命名 | `renderer` / `external_review` |
| `L024` | 覆盖率与覆盖源分数必须说明分母和阈值 | `renderer` / `external_review` |
| `L025` | 结论文案必须拆开估值、质量和信息不足，不能混成一句模板话 | `renderer` / `external_review` |
| `L026` | 相关性/分散度基准映射必须可解释 | `external_review` / `renderer` |
| `L027` | 中期宏观判断必须拆开景气、价格与信用指标角色 | `external_review` / `renderer` |
| `L028` | 商品/期货 ETF 必须显式披露展期与期限结构风险 | `external_review` / `renderer` |
| `L029` | 客户稿不得暴露原始异常字符串 | `release_check` / `renderer` |
| `L030` | 历史样本验证必须披露非重叠样本、置信区间和样本质量 | `release_check` / `renderer` |
| `L031` | Pick 覆盖率分母必须对应完整分析样本 | `release_check` |
| `L032` | 单候选说明不能擅自改写交付等级 | `release_check` |
| `L033` | 覆盖率过低的 ETF/基金不应继续输出完整终稿模板，应退化为摘要观察稿 | `workflow` / `release_check` / `external_review` |
| `L034` | `回避 / 观察` 的 ETF/基金稿不能无差别保留完整交易动作表 | `renderer` / `external_review` |
| `L035` | ETF/基金“基本面”若主要来自产品结构和代理映射，必须显式降格或改名，不能冒充真实行业基本面 | `opportunity_engine` / `renderer` / `external_review` |
| `L036` | 长期缺失的维度不能只写“未纳入”，必须做补权、归一化或降格为信息项 | `opportunity_engine` / `external_review` |
| `L037` | 观察类终稿必须按信息增量裁剪篇幅，重复 playbook 句式视为结构性解释不合格 | `renderer` / `release_check` / `external_review` |
| `L038` | 外审必须包含“rich prompt 审稿 + 零提示发散审”双层结果，不能只跑模板化单层审稿 | `workflow` / `report_guard` / `review_audit` / `external_review` |
| `L039` | pick 稿里的代理信号必须同时披露置信度、限制和降级影响，manifest / review audit 也必须能追踪这层合同 | `renderer` / `workflow` / `review_audit` / `external_review` |

## 处理规则

以后每次外审，如果出现新的有效意见：

1. 先判断是不是 `P0/P1` 或重复出现的 `P2`
2. 如果是，就必须做根因处理
3. 根因处理后，至少要满足下面之一：
   - 已新增代码门禁 / 逻辑修复 / 渲染修复 / 数据修复
   - 已在这里登记为新的 `lesson id`
4. 只有“当前稿修了”和“系统以后也更不容易再犯”同时成立，才算真正关闭

## 使用方式

- `release_check`：负责把已经能文本化/结构化检查的 lesson 做成硬校验
- `report_guard`：负责把外审 PASS 变成 final 导出的门禁
- `opportunity_engine / collectors / renderers`：负责把已经明确的逻辑性问题彻底修进模型和输出层
- `external_financial_reviewer.md`：负责继续发现新的系统性问题
- `report_revision_loop.md`：负责要求主执行者把外审意见内化，而不是只修当前稿
