# AI-Finance

本地优先的 CLI 投研工作台。

它不是 Web 产品，不是自动交易系统，也不是“给个代码就替你买卖”的黑盒。它做的是把研究、推荐、风险、组合、日报、复盘、外审、交付串成一套可运行、可测试、可持续迭代的本地工具链。

## 现在能做什么

- 单标的分析：`scan`、`stock_analysis`
- 推荐产出：`stock_pick`、`etf_pick`、`fund_pick`
- 市场简报：`briefing daily / weekly / noon / evening / market`
- 研究问答：`research`、`assistant`、`lookup`
- 组合与风险：`portfolio`、`risk`
- 回测与学习：`strategy replay / validate / attribute / experiment`
- 正式交付：`client_export`、`release_check`、`report_guard`
- 外审治理：`review_ledger`、`review_audit`

## 最近这版最重要的更新

### 1. 推荐链和客户成稿链已经收口

- `scan / stock_analysis / stock_pick / etf_pick / fund_pick / briefing` 都能走正式客户交付链。
- final 产物不再只是 Markdown，正式会导出 `md + html + pdf`。
- final 之前会经过 `release_check` 和 `report_guard`，并要求配套 `external review + manifest`。
- HTML 导出已支持把本地图片嵌成 `data:` URI，单文件 HTML 可以直接转发。
- PDF 导出已修复 Edge 偶发“文件已写出但进程慢退出”的收尾问题。

### 2. 强因子 v1 已从“会加分”变成“会加分也会扣分”

当前已经成型的主因子族：

- `J-1 技术结构`
  - `MACD 金叉`、`ADX`、`KDJ`、`OBV`、`RSI 位置`
  - `量价/动量背离`
  - `K线形态`
  - `假突破识别`
  - `支撑结构`
  - `压力位`
  - `压缩启动`
  - `量价结构`
  - `波动压缩`
- `J-2 季节/日历`
  - `月度胜率`
  - `旺季前置`
  - `财报窗口`
  - `指数调整`
  - `节假日窗口`
  - `商品季节性`
  - `政策事件窗`
  - `分红窗口`
- `J-3 相对强弱 / 宽度 / 筹码`
  - `超额拐点`
  - `板块扩散`
  - `行业宽度`
  - `龙头确认`
  - `北向/南向`
  - `公募/热度代理`
  - `机构资金承接`
  - `机构集中度代理`
  - `拥挤度风险`
- `J-4 基本面`
  - `估值代理`
  - `盈利增速`
  - `ROE`
  - `毛利率`
  - `PEG 代理`
  - `现金流质量`
  - `杠杆压力`
  - `盈利动量`
- `M-1 宏观`
  - `敏感度向量`
  - `景气方向`
  - `价格链条`
  - `信用脉冲`
  - `当前 regime`
- `J-5 ETF / 基金专属`
  - `ETF 折溢价`
  - `ETF 份额申赎`
  - `跟踪误差`
  - `成分集中度`
  - `主题纯度`
  - `业绩基准披露`
  - `风格漂移评估`
  - `经理稳定性`
  - `费率结构`

这些因子现在不是只在细节里展示，顺风和逆风都能进入：

- 评分
- 正文解释
- 强因子拆解
- final 客户稿和 review manifest

### 3. `briefing` 已经变成正式的市场工作台入口

- 新增 `briefing market`，可以直接生成“全市场行情简报”。
- 输出已拆成：
  - `背景框架`
  - `交易主线候选`
  - `次主线候选`
- 主线 taxonomy 不再只剩几句粗模板，新增并区分：
  - `黄金避险`
  - `红利/银行防守`
  - `宽基修复`
  - `电网/公用事业`
- A 股观察池不再只是附录，会把行业分布回灌到主线评分。

### 4. 速度已经做过一轮实改

- `stock_pick` 默认市场已改成 `cn`，不再默认拖上 `HK/US`。
- `discover_stock_opportunities()` 支持共享 context，`drivers / news / pulse / proxy` 不再按股票重复拉。
- ETF discover 链已经做了并行预热和有界并发，7 只 ETF 小样本大约从 `62s` 压到 `40s` 左右。
- 目前剩下最明显的慢点，是 `briefing market` 里的 A 股观察池深扫还会重复做一遍上游取数。

## 输出长什么样

### internal / preview

用于内部观察、调试、当天快照，不等于正式对外交付。

### final

正式交付产物，默认包括：

- `markdown`
- `html`
- `pdf`

并且需要同时存在：

- `external_review.md`
- `release_manifest.json`

如果 `release_check` 或 `report_guard` 没过，final 不会落盘。

## 最常用命令

### 单标的

```bash
python -m src.commands.scan 300308
python -m src.commands.stock_analysis 300308
python -m src.commands.compare 510210 510300 510500
python -m src.commands.research 300308 现在还能不能买
```

### 推荐

```bash
python -m src.commands.stock_pick
python -m src.commands.stock_pick --client-final
python -m src.commands.etf_pick
python -m src.commands.fund_pick
```

### 市场简报

```bash
python -m src.commands.briefing daily
python -m src.commands.briefing market
```

### 组合与风险

```bash
python -m src.commands.portfolio status
python -m src.commands.portfolio whatif buy 300308 580 20000
python -m src.commands.risk report
```

### strategy

```bash
python -m src.commands.strategy replay 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
python -m src.commands.strategy validate --symbol 600519 --limit 20 --preview
python -m src.commands.strategy attribute --symbol 600519 --limit 20 --preview
python -m src.commands.strategy experiment 600519 --start 2024-01-01 --end 2024-12-31 --max-samples 6
```

### 外审与治理

```bash
python -m src.commands.review_ledger
python -m src.commands.review_audit
```

## 默认怎么读这个仓库

不要一上来扫完整个仓库。

建议顺序：

1. 先读这份 [README.md](./README.md)
2. 再读 [AGENTS.md](./AGENTS.md)
3. 再读 [docs/context_map.md](./docs/context_map.md)
4. 最后只打开你要改的 command / processor / renderer / test

配置相关再读 [config/README.md](./config/README.md)。  
`strategy` 相关再读 [docs/plans/strategy.md](./docs/plans/strategy.md)。  
更细状态再读 [docs/status_snapshot.md](./docs/status_snapshot.md)。

## 当前成熟度

成熟区：

- `scan`
- `stock_analysis`
- `stock_pick`
- `fund_pick`
- `etf_pick`
- `research`
- `risk`
- `portfolio`
- `compare`
- `briefing`
- `lookup`
- `assistant`

可用但仍在迭代：

- `discover`
- `policy`
- `strategy`
- `decision_review`
- `scheduler`

弱或占位：

- `collectors/policy.py`
- `collectors/social_sentiment.py`
- `collectors/global_flow.py`
- scheduler 的持久化和运维可见性层

## 最近里程碑日志

### 2026-03-19

- `briefing market` 落地
- 主线 taxonomy 扩容，并把 A 股观察池行业分布回灌到主线评分
- `stock_pick / etf_pick / scan / stock_analysis` 的 today final、review、manifest 已补齐
- PDF 导出 slow shutdown 问题已修

### 2026-03-18

- ETF / fund discover 开始做性能收口
- `build_market_context` 并行预热
- ETF/fund analyze 改成有界并发
- 基金画像共享缓存补齐

### 2026-03-17

- 强因子链从单向加分扩展到双向打分
- `催化 / 相对强弱 / 筹码结构 / 季节日历 / 基本面 / 宏观` 都开始显式纳入逆风项
- `基本面底线`、`行业宽度`、`机构集中度代理`、`龙头确认` 等合同继续收口

### 2026-03-16

- 客户 final 改成 HTML-first 成稿主题
- 图片和图表进入客户稿与 PDF
- trade handoff 合同统一
- `review_audit` 对 `structured-round` 审计为 `0 active findings`

## 接下来最值得做什么

1. `briefing market` 提速
   - 复用 `market_context`
   - 不再让 A 股观察池重复拉一轮 `drivers / pulse / news`
2. `strategy` fixture 和治理层
   - lag / visibility / overlap / benchmark fixture
   - champion-challenger promotion / rollback gate
3. proxy 信号合同继续下沉
   - 把代理置信度和降级影响更完整地传到 pick 输出、release guard、review guard
4. `policy` v2
   - 继续提升重 PDF / OFD 的抽取和 taxonomy
5. `scheduler` v2
   - 做持久化 run history、失败可见性和运维状态

## 相关入口

- 任务地图：[docs/context_map.md](./docs/context_map.md)
- YAML 地图：[config/README.md](./config/README.md)
- 状态快照：[docs/status_snapshot.md](./docs/status_snapshot.md)
- 路线图：[plan.md](./plan.md)
- `strategy` 专题：[docs/plans/strategy.md](./docs/plans/strategy.md)
- 外审 kit：[docs/review_kit/README.md](./docs/review_kit/README.md)

## 历史文档说明

- [docs/architecture_v2.md](./docs/architecture_v2.md) 是历史架构草案，不是当前主合同。
- `reports/`、`tmp/` 下的大多数文件都是生成产物，不应作为开工前默认上下文。
