# Tushare 10000 分接入计划

这份文件只回答一件事：

`10000 分 Tushare 到位后，哪些新数据已经接进 repo，哪些还没接完，以及新主链覆盖到的旧接口该怎么退。`

## 目标

不是把能用的接口都堆进来，而是优先提升这几条 mature 主链：

- `briefing`
- `scan`
- `stock_analysis`
- `stock_pick`
- `etf_pick`
- `fund_pick`
- `risk / portfolio / compare`

核心原则：

1. 优先接能显著提升研究质量的高价值数据
2. 优先替换旧的低配或不稳定接口
3. 每层都保留 `source / as_of / fallback / disclosure`
4. 先共享 collector / processor / renderer 合同，再做 today final

当前 ETF 范围默认只做非实时分析主链：

- 先做 `ETF 基本信息 / ETF 基准指数 / ETF 日线 / ETF 复权 / ETF 份额规模`
- 先把 `etf_pick / scan / compare / portfolio / briefing` 的研究质量做稳
- `ETF 实时日线 / 实时分钟 / 历史分钟 / 实时参考` 暂不进入当前主线，后续如需盘中监控再单开专题

## 当前口径

这份计划里的“已接入”只按研究主链来算，不按“collector 里有个 stub”来算。

- `已接入`
  至少已经进入 `collector + processor/排序/证据 + renderer/guard` 其中两层，不是只落一个接口包装。
- `收口`
  新主链已经能稳定接住主要职责，可以把被覆盖的旧 `AKShare` 路径降成 fallback 或直接退掉。
- `未完成`
  要么还没接接口，要么只停在 collector/metadata，没有真正进入 mature 报告主链。

## 进度总览

| 域 | 当前状态 | 已接入 | 当前剩余缺口 |
| --- | --- | --- | --- |
| 股票 | 第一阶段已收口，第二阶段继续扩可见性 | `ths_index / ths_daily / ths_hot / ths_member / st / stk_high_shock / stk_alert / cyq_perf / cyq_chips / moneyflow / margin_detail / top_list / top_inst / stk_auction / stk_limit / limit_list_d / broker_recommend / 上证e互动 / 深证互动易 / stk_factor_pro / stk_surv` | 继续把已覆盖的旧 AKShare 退掉，并把 `stk_factor_pro / stk_surv` 的成熟稿可见性继续前置 |
| 指数/行业 | 第一阶段已收口 | `index_basic / index_daily / index_dailybasic / index_weight / idx_factor_pro / index_global / 申万/中信行业框架 / index_weekly / index_monthly` | 主要剩余是可见性、口径统一和最终收口 |
| ETF | 第二阶段已收口 | `etf_basic / etf_index / etf_share_size / fund_daily / fund_adj / fund_factor_pro` | 继续把被覆盖旧 `AKShare` 退掉，并补 `compare / portfolio` 的长期收口 |
| 市场结构/宏观 | 第二阶段进行中 | `daily_info / sz_daily_info / fx_basic / fx_daily / sge_basic / sge_daily` | 继续把 `daily_info` 交易结构、`fx` 外汇口径和黄金现货锚定前置到成熟稿判断层 |
| 跨市场/转债/基金渠道 | 第二阶段已进入可见层 | `stk_ah_comparison / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / cb_issue / cb_share` | `briefing / compare / fund_pick` 已可见，下一步是继续统一到更多 mature 首屏和长期对比链，并视需要单开转债主线 |
| 板块/港股短线专题 | 第二阶段新收口 | `tdx_index / tdx_member / tdx_daily / ggt_top10 / ccass_hold_detail / hm_detail / dc_index / dc_daily / moneyflow_mkt_dc / report_rc` | 继续把 `TDX 结构专题 / DC 结构专题 / 港股辅助层 / 研报辅助层 / 转债辅助层` 稳定前置到 mature 首页和比较链 |

## 第二阶段 backlog（已接入与继续收口）

下面这批接口原来都在“已解锁、未接、也未排期”的列表里；其中第一批已经开始开发，剩余部分继续按研究价值排序。

### 已进入第二阶段开发

- `daily_info / sz_daily_info`
  - 当前状态：已接到 `market_overview` 共享 collector，并开始进入 `briefing / market_analysis`
  - 下一步：继续统一成熟稿里的交易结构表达
- `stk_factor_pro`
  - 当前状态：已接到 `valuation + opportunity_engine + factor_meta`，开始影响股票技术面维度
  - 下一步：继续把这层证据往 `stock_analysis / stock_pick` 的正文前置
- `stk_surv`
  - 当前状态：已接到 `news` structured intelligence 主链，并已放进 `stock_pick / briefing` fast profile 白名单
  - 下一步：继续把命中的机构调研证据更稳定前置到成熟稿首页
- `fx_basic / fx_daily`
  - 当前状态：已接到 `macro_cn`，并开始进入 `china_macro snapshot / macro_lines`
  - 下一步：继续把人民币/外汇方向写进 `briefing / portfolio` 的宏观判断

### 股票补充层
- `stk_ah_comparison`
  - 用途：A/H 比价与港股映射
  - 更适合：`briefing / stock_analysis / compare`
  - 当前状态：已接到 `market_cn` 共享 collector，并已进入 `opportunity_engine` 的 `跨市场比价` 因子；A/H 标的成熟稿现在会显式披露 `premium_rate / latest_date / disclosure`

### 宏观与跨资产

- `sge_basic / sge_daily`
  - 用途：黄金现货基础与日线
  - 更适合：`briefing / compare / 黄金链 ETF`
  - 当前状态：`commodity` 已切到 `Tushare sge_daily` 主链；黄金链 ETF/基金的 `基本面` 维度会显示 `现货锚定`

### 基金与财富管理

- `fund_sales_ratio`
  - 用途：基金销售保有和渠道结构
  - 更适合：`fund_pick / compare / briefing`
  - 当前状态：`fund_profile` 已沉淀 `sales_ratio_snapshot`，`client_report / opportunity_report` 已能渲染 `公募渠道环境`

### 可转债

- `cb_basic / cb_daily / cb_factor_pro`
  - 用途：可转债基础、日线、技术因子
  - 更适合：后续单开可转债研究主线
  - 当前状态：`market_cn` 已接 collector，`opportunity_engine` 已把它下沉成 A 股个股 `可转债映射` 辅助因子
- `cb_issue / cb_share`
  - 用途：可转债发行与转股结果
  - 更适合：`stock_analysis / stock_pick / compare`
  - 当前状态：`market_cn` 已接共享 collector，`opportunity_engine` 的 `市场事件行 / 转债辅助层` 已开始消费

### 板块 / 港股短线专题

- `tdx_index / tdx_member / tdx_daily`
  - 用途：板块/风格/地区专题链
  - 更适合：`briefing / stock_analysis / stock_pick / scan / etf_pick`
  - 当前状态：`market_drivers` 已接共享 collector，`opportunity_engine` 已开始下沉 `TDX结构专题`
- `dc_index / dc_daily`
  - 用途：东方财富概念/板块专题链
  - 更适合：`briefing / stock_analysis / stock_pick / scan / etf_pick`
  - 当前状态：`market_drivers` 已接共享 collector，`opportunity_engine` 已开始下沉 `DC结构专题`
- `moneyflow_mkt_dc`
  - 用途：东财大盘资金流视角
  - 更适合：`briefing / market_overview / market_analysis`
  - 当前状态：`market_drivers` 已接共享 collector，市场结构/大盘资金流会优先尝试这条主链；权限、频控或 IP 限制按缺失披露，不伪装成 fresh
- `report_rc`
  - 用途：卖方盈利预测、目标价和一致预期辅助层
  - 更适合：`stock_analysis / stock_pick / compare`
  - 当前状态：`market_drivers` 已接共享 collector，`opportunity_engine / client_report / opportunity_report` 已开始下沉 `研报辅助层`
- `ggt_top10 / ccass_hold_detail / hm_detail`
  - 用途：港股通活跃、CCASS 持仓、游资交易明细
  - 更适合：`briefing / stock_analysis / scan / compare`
  - 当前状态：`market_cn` 已接共享 collector，`opportunity_engine` 已开始下沉 `港股辅助层`

## 当前主线

现在这条计划不再以“广撒接口”为主，而是按下面顺序收口：

1. 把已被 Tushare 覆盖到的旧 `AKShare` 主链系统性退掉
2. 补齐剩余未接的 10000 分高价值接口
3. 让 `index_weekly / index_monthly` 进入 mature 主链消费并统一周/月线口径
4. 收拢可见性、最终收口和 client-safe 披露

## 当前进度

- `2026-04-02`：股票链第一批已把 `ths_member + st / stk_high_shock / stk_alert` 接进共享 collector 合同，并下沉到 `scan / stock_analysis / stock_pick / briefing`。
- 当前这批新字段默认都带 `source / as_of / fallback / disclosure`；当 `ths_member / stock_st / stk_high_shock / stk_alert` 权限失败、空表或频控时，统一按缺失/阻塞处理，不再把空结果写成 fresh 命中或“已通过”。
- `ths_member` 当前优先服务主题成员归因与 `briefing` 观察池情报回填；若板块索引侧拿不到干净主题名，则降成空结果，不把 `nan` / 伪主题名下沉到客户稿。
- `st / stk_high_shock / stk_alert` 当前已进入个股 `硬性检查 + 风险维度 + 风险提示`。
- `2026-04-02`：股票链第二批已把 `cyq_perf / cyq_chips` 接进共享筹码快照合同，并下沉到 `scan / stock_analysis / stock_pick / briefing` 的 `排序 / 推荐理由 / 风险提示 / 关键证据`。
- `cyq_perf / cyq_chips` 当前默认走共享 cache + normalize；若只拿到旧日期筹码，则显式标成 `stale / 非当期`，不把旧筹码写成 same-day fresh 命中，也不把它直接当成当天资金确认。
- `2026-04-02`：股票链第三批已把 `moneyflow / margin_detail / top_list / top_inst / stk_auction / stk_limit / limit_list_d` 接进共享 collector 合同，并通过 `ths_member + moneyflow_ind_ths / moneyflow_cnt_ths` 做个股主题资金流代理。
- 这批股票 P1 数据现在会真实影响 `scan / stock_analysis / stock_pick / briefing` 的 `排序 / 推荐理由 / 风险提示 / 关键证据 / 事件消化`：个股级 `moneyflow` 优先覆盖行业代理，`margin_detail` 进入两融拥挤与风险提示，`龙虎榜 / 竞价 / 涨跌停边界` 进入短线确认与情绪风险。
- `moneyflow / margin_detail / 龙虎榜-竞价-涨跌停边界` 当前默认都带 `source / as_of / latest_date / fallback / disclosure`；权限失败、空表、频控或非当期明细会显式降成 `缺失 / stale / 观察`，不把旧数据或空专题写成 same-day fresh 命中。
- `2026-04-02`：股票链第四批已把 `broker_recommend` 接进共享 `market_drivers` collector，并下沉到 `scan / stock_analysis / stock_pick / briefing` 的 `market_event_rows / 催化面 / 风险维度 / 推荐理由 / 观察点`。
- `broker_recommend` 当前默认带 `source / as_of / latest_date / fallback / disclosure`；只有当月券商月度金股名单命中时才按 fresh 共识处理，历史月份名单显式降成 `stale / 观察`，权限失败或频控不会回退成假 fresh 卖方升温。
- `2026-04-02`：股票链第五批已把 `上证e互动 / 深证互动易` 接进共享 `news` collector；`scan / stock_analysis / stock_pick / briefing` 现在都会真实消费 `互动平台问答 / 投资者关系口径`，把它写进 `结构化事件 / 关键证据 / 事件消化 / What Changed`。
- `e互动` 当前默认带 `source / published_at / configured_source / link / note / fallback`；即使 `stock_pick_fast` 把全局新闻压成 `proxy/empty`，个股级 `e互动` 仍会继续拉取，但只按“补充证据/方向确认”处理，不会把管理层口径写成正式公告或 same-day 强催化。
- `2026-04-02`：指数/行业标准链第一批已把 `index_classify + index_member_all + sw_daily` 接进共享 `industry_index` collector，`ci_index_member + ci_daily` 同步接成补充框架；`market_drivers.industry_spot` 现在优先走 `申万行业指数 -> 中信行业指数 -> ths 行业盘面 -> AKShare board`，不再让 `AKShare board spot` 充当默认行业主链。
- `2026-04-02`：`scan / stock_analysis / stock_pick / etf_pick / briefing` 现在都会消费 `申万/中信行业框架`；个股与 ETF 的 `market_event_rows`、正文叙事和 `briefing` 首页盘面都能直接写出标准行业/指数归属，不再只靠 `sector / chain_nodes / 板块名字符串` 模糊匹配。
- `2026-04-02`：指数专题主链第二批已把 `index_basic / index_dailybasic / index_weight / index_daily / idx_factor_pro / index_global` 接进共享 `index_topic` collector；`valuation / market_cn / market_overview` 已改成先吃这条主链，再把结果下沉到 `scan / stock_analysis / stock_pick / etf_pick / briefing`。
- `2026-04-02`：这批指数主链现在会真实影响 `ETF/指数` 路径的 `估值 / 跟踪指数框架 / 指数技术状态 / 成分权重结构 / 相对强弱`；`market_overview` 的国际指数也不再把 `yfinance` 写成默认主来源。
- `2026-04-02`：ETF 主链已继续收口：`fund_profile` 现在会优先按 `etf_basic / etf_index / etf_share_size` 构建 `etf_snapshot`，`market_cn.get_etf_daily()` 也已改成只走 `Tushare fund_daily + fund_adj` 主链；只要 ETF 持仓已有 `Tushare fund_portfolio + stock_basic`，就不再默认调用 `AKShare` 持仓补名。`etf_pick` 的排序和“为什么先看它”现在也会真实消费 `跟踪指数结构 + 指数成分权重 + 份额快照`，而不是只把这些字段挂在信息表里。
- `2026-04-04`：`fund_profile` 的持仓主路进一步退场：`cn_fund` 现在也优先吃 `Tushare fund_portfolio + stock_basic`，只有 Tushare 持仓缺失时才允许 AKShare holdings 兜底；`valuation` 也已不再保留任何实质 AKShare helper，Tushare 主链之外只剩明确的 realtime / side 路。
- `2026-04-02`：ETF 当前已不再让 `AKShare` 充当默认主链：ETF 概况、ETF 日线、ETF 持仓补名三条被覆盖路径都已退到非主路径；当时主要剩余缺口已经收窄到 `etf_share_size` 的两点变化和 `跟踪指数 + 份额变化 + 外部情报` 的最终融合。
- `2026-04-03`：`fund_factor_pro` 已从共享 collector 合同真正下沉到 ETF 主链：`fund_profile` 会补 `fund_factor_snapshot`，`opportunity_engine` 会产出 `场内基金技术状态` 因子，`etf_pick / scan` 的 `基金画像 / 推荐理由 / 因子解释` 都会消费它；客户稿默认写出 `趋势 / 动能 + 日期`，缺失时显式按信息项披露，不会把 ETF 产品层技术状态伪装成已确认。这块已不再是主缺口。
- `2026-04-03`：ETF phase 2 现已收口：`etf_share_size` 默认会拉近 7 个开盘日做两点变化，`etf_pick` 的“为什么先看它”和 `compare` 的 `ETF产品层对比` 会一起写出 `跟踪指数 + 份额变化 + 场内基金技术状态 + 外部情报状态`；`config/config.etf_pick_fast.yaml` 也改成默认保留 `light fund_profile`，不再通过 `skip_fund_profile` 把 ETF 产品层静默关掉。
- `2026-04-03`：`broker_recommend` 和 `上证e互动 / 深证互动易` 也已经进入共享 collector + processor + renderer + guard 路径，当前主要是稳定披露和例外处理，不再是剩余主缺口。
- `2026-04-03`：`index_weekly / index_monthly` 已从 helper 层进一步下沉到 mature 稿面；`briefing / scan / stock_analysis / etf_pick` 正文里都会显式出现 `周月节奏`，并把周线、月线和方向性结论写进首页判断、关键证据或趋势节奏，不再只停在 collector / event rows。
- `2026-04-03`：这轮又继续退掉一批已被 Tushare 覆盖的旧路径：`valuation.get_cn_etf_nav_history()` 不再回退 AKShare ETF 净值，`valuation.get_cn_etf_scale()` 改成 `etf_share_size` 主链，`valuation.get_cn_stock_financial_proxy()` 也不再回退 AKShare 财务代理；当前保留的 AKShare 只剩实时/分钟或尚未被 10000 分一对一覆盖的侧路。
- `2026-04-04`：第二阶段 backlog 里第一批新接口开始真正接入 mature 主链：`market_overview` 已接 `daily_info / sz_daily_info` 的市场结构快照；`valuation + opportunity_engine + factor_meta` 已接 `stk_factor_pro`；`news` 结构化情报主链已接 `stk_surv`，`stock_pick / briefing` 的 fast runtime 白名单也已放行机构调研；`macro_cn + context` 已接 `fx_basic / fx_daily` 并开始把 `USDCNH` 写进宏观快照和 `macro_lines`。
- `2026-04-04`：第二阶段 backlog 又向前收了一批：`stk_ah_comparison / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / sge_basic / sge_daily` 不再只是待排期清单，而是已经进入 `market_cn / fund_profile / commodity / opportunity_engine / renderer` 的共享主链。
- `2026-04-04`：第二阶段第二批的可见层也开始收口：`briefing` 首屏已前置 `daily_info / sz_daily_info` 形成的市场结构快照，`compare` 新增 `第二阶段信号快照`，`fund_pick` 的“为什么先看它/为什么推荐它”现在会显式前置 `公募渠道环境 / 黄金现货锚定`。当前真实剩余项不再是“有没有接口”，而是把这批信号继续统一到更多 mature 首屏和长期对比链。
- `2026-04-05`：第二阶段又收了一批原本未接的值得接接口：`tdx_index / tdx_member / tdx_daily / ggt_top10 / ccass_hold_detail / hm_detail / cb_issue / cb_share / dc_index / dc_daily / moneyflow_mkt_dc / report_rc` 已进入 `market_drivers / market_cn / opportunity_engine / renderer` 的共享主链，并开始下沉 `TDX结构专题 / DC结构专题 / 港股辅助层 / 研报辅助层 / 转债辅助层`。
- `2026-04-05`：`fx_daily` 在权限、频控或 IP 限制下已改成按缺失快照降级；`briefing / research / regime` 这类依赖宏观上下文的命令不会再因为外汇链异常直接崩溃。
- `2026-04-05`：这批新接口已补齐对应 collector / processor / renderer 窄测；当前剩余项进一步收窄到“把新信号前置到更多 mature 首页/比较链”和“继续只清理仍可安全退场的旧 realtime/side fallback”，而不是再找主缺口接口。

## 当前收口项

1. `index_weekly / index_monthly` 的成熟消费与周/月线口径统一
- 这两条已经进入主链，也已经进入 mature 稿面，不再算“未接接口”。
- 当前重点已经从“有没有消费”收窄到“周/月线结论写法、来源披露和最终收口一致”。

2. `AKShare` 旧 ETF/指数路径系统性退场
- ETF / 指数主链已经切到 Tushare 优先，旧 AKShare 现在只应保留明确兜底。
- 当前重点不再是补新接口，而是把已覆盖旧链进一步压到非主路径，并同步文档 / guard / 测试 / 可见性。

3. 第二阶段 backlog 正式排期
- 已进入主链：`daily_info / sz_daily_info / stk_factor_pro / stk_surv / fx_basic / fx_daily / stk_ah_comparison / fund_sales_ratio / cb_basic / cb_daily / cb_factor_pro / cb_issue / cb_share / sge_basic / sge_daily / tdx_index / tdx_member / tdx_daily / ggt_top10 / ccass_hold_detail / hm_detail`
- 当前重点：把这批数据的首屏可见性继续从 `briefing / compare / fund_pick` 扩到更多 mature 首页/长期对比链，而不是再停留在 collector 或辅助因子层

## 优先级

### P0 当前立即收口

1. `已被覆盖的 AKShare 旧链退场`
- 用途：
  - 不再长期双轨混跑
  - 质量提升和速度提升一起收
- 主要落点：
  - `market_drivers`
  - `fund_profile`
  - `market_cn`
  - `valuation / market_overview`
- 替换方向：
  - 只退已经被 Tushare 覆盖到的旧实现，不做超范围大扫除

2. `index_weekly / index_monthly`
- 用途：
  - 周/月线研究口径
- 主要落点：
  - `scan / stock_analysis / briefing / etf_pick`
- 替换方向：
  - 从“已接 helper”升级到“成熟消费 + 统一披露 + 最终收口”

3. 第二阶段 backlog
- 用途：
  - 继续把 `10000` 分的边际价值吃满，不再只盯股票/ETF/指数/行业第一阶段
- 主要落点：
  - `briefing / market_overview / stock_analysis / stock_pick / fund_pick / compare / portfolio`
- 替换方向：
  - 先排研究价值高的，再进入新主线

### P1 已完成主块，继续维护与深化

4. `ths_index / ths_daily / ths_hot / ths_member`
- 用途：
  - A 股主题/行业主线识别
  - 今日热股/热主题
  - 主题成分股映射
- 主要落点：
  - `briefing`
  - `market_drivers`
  - `theme_playbook`
  - `scan / stock_analysis / stock_pick`
- 替换方向：
  - `AKShare board spot / hot rank` 不再做默认主链，只保留兜底

5. `etf_basic / etf_index / etf_share_size / fund_daily / fund_adj / fund_factor_pro`
- 用途：
  - ETF 专用基础信息
  - ETF 跟踪指数
  - ETF 每日份额/规模变化
  - ETF 日线与复权
  - ETF 产品层趋势/动能状态
- 主要落点：
  - `etf_pick`
  - `scan` 里的 ETF 路径
  - `briefing`
  - `compare / portfolio`
- 替换方向：
  - `fund_basic(market='E')` 不再充当 ETF 主 metadata
  - 已覆盖的 ETF `AKShare` 概况/日线/持仓补名路径退到非主路径

6. `st / stk_high_shock / stk_alert / cyq_perf / cyq_chips / moneyflow / margin_detail / 打板专题`
- 用途：
  - 个股风险、筹码、资金流和短线边界
- 主要落点：
  - `scan / stock_analysis / stock_pick / briefing`
- 替换方向：
  - 已从“计划中”进入稳定主链维护状态

7. `index_basic / index_daily / index_dailybasic / index_weight / idx_factor_pro / index_global / 申万/中信行业框架`
- 用途：
  - 标准行业/指数框架
  - 指数技术状态
  - 海外指数联动
- 主要落点：
  - `briefing / scan / stock_analysis / stock_pick / etf_pick`
- 替换方向：
  - 逐步替换旧的模糊板块表达、`AKShare` 板块快照和低配指数路径

## 替换矩阵

| 当前低配/旧路径 | 新主路径 | 当前状态 | 影响命令 |
| --- | --- | --- | --- |
| `AKShare board spot / hot rank` | `ths_index + ths_daily + ths_hot + ths_member` | 已改成非默认主链 | `briefing / scan / stock_analysis / stock_pick` |
| `行业关键词 / 板块名字符串 / sector 模糊匹配` | `index_classify + index_member_all + sw_daily + ci_index_member + ci_daily` | 已改成标准行业框架优先 | `briefing / scan / stock_analysis / stock_pick / etf_pick` |
| `fund_basic(E)` 充当 ETF 主身份 | `etf_basic + etf_index` | 已替换 | `etf_pick / scan / compare / portfolio` |
| 仅价格判断 ETF 资金变化 | `etf_share_size` | 已接入并进入两点变化主链 | `briefing / etf_pick / scan / compare` |
| ETF / 开放式基金持仓补名默认走 AKShare | `etf_basic + etf_index + fund_daily + fund_adj + fund_portfolio + stock_basic` | 已从默认主路径退场；cn_fund 仅在 Tushare 持仓缺失时才允许 AKShare 兜底 | `etf_pick / scan / compare / portfolio` |
| 粗粒度筹码描述 | `cyq_perf / cyq_chips` | 已替换主描述层 | `stock_analysis / stock_pick / scan` |
| 只靠行业/板块代理判断个股资金、拥挤和打板节奏 | `moneyflow + margin_detail + top_list/top_inst + stk_auction + stk_limit + limit_list_d + ths_member/moneyflow_ind_ths/moneyflow_cnt_ths` | 已进入主链 | `briefing / scan / stock_analysis / stock_pick` |
| 简化 ETF/指数技术判断 | `fund_factor_pro / idx_factor_pro` | 两者都已接入主链；`fund_factor_pro` 当前已进入 `fund_profile / opportunity_engine / etf_pick / scan / client_report / opportunity_report` | `etf_pick / scan / 指数/ETF 客户稿` |
| `valuation.index_all_cni / stock_zh_index_value_csindex` | `index_basic + index_dailybasic` | 已替换主路径 | `scan / stock_analysis / stock_pick / etf_pick` |
| `market_cn.index_zh_a_hist` 的指数主路径部分 | `index_daily + index_weekly + index_monthly` | 已替换主路径，周/月线已进入正文可见判断 | `scan / stock_analysis / etf_pick / briefing` |
| `valuation.get_cn_etf_nav_history` 的 AKShare ETF 净值回退 | `fund_nav` | 已退掉 AKShare 主路径 | `etf_pick / scan / compare / portfolio` |
| `valuation.get_cn_etf_scale` 的 AKShare 规模快照 | `etf_share_size` | 已替换主路径 | `etf_pick / scan / compare / portfolio` |
| `valuation.get_cn_stock_financial_proxy` 的 AKShare THS/EM 回退 | `daily_basic + fina_indicator` | 已退掉 AKShare 主路径 | `scan / stock_analysis / stock_pick / etf_pick` |
| `valuation.index_stock_cons_weight_csindex` | `index_weight` | 已替换主路径 | `etf_pick / scan / stock_analysis / stock_pick` |
| `market_overview` 海外指数 `yfinance` | `index_global` | 已替换主路径 | `briefing` |

## 执行顺序

1. 先补 `index_weekly / index_monthly` 的成熟消费
2. 再把已被覆盖的旧 `AKShare` 主链系统性退掉
3. 每一组先 narrow tests + spot check
4. family-level 后再重刷 today final

## 当前明确不在本计划内

这些不是 `10000` 分自动解锁的主任务，不要混进来：

- `公告信息` 独立权限
- `新闻资讯` 独立权限
- `分钟数据` 独立权限
- ETF 实时/分钟链路

## 验收标准

做到以下几点，才算这条计划收口：

1. `briefing` 能稳定写出今天 A 股在交易什么主题、什么热股、什么风险提示
2. `etf_pick / scan` 不再把 ETF 当普通基金处理，并把 `跟踪指数 + 份额变化 + 外部情报` 真写进排序和理由
3. `stock_analysis / stock_pick` 的筹码、资金流和风险提示明显更实
4. 已被 Tushare 覆盖到的旧 `AKShare` 主链不再长期双轨混跑
5. `report_guard / release_check / client_report` 承认这些成熟字段，并不会把 `fund_factor_pro / broker_recommend / e互动` 继续写成主缺口
6. 对外文案里有明确 `source / as_of / fallback / disclosure`，且周/月线口径与最终收口一致
