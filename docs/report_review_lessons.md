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
