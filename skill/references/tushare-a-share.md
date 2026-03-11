## Tushare A股参考

优先目标：A 股数据先用 `Tushare Pro`，不要默认直接切到 `AKShare`。

### 先按“要什么数据”选接口

| 场景 | Tushare接口 | 备注 |
| --- | --- | --- |
| 股票静态信息 | `stock_basic` | 名称、行业、市场、上市日期、状态 |
| 日线行情 | `daily` | `open/high/low/close/vol/amount` |
| 复权 | `adj_factor` | 和 `daily` 拼表，不要指望 `daily_basic` 给复权行情 |
| 每日估值/换手/市值 | `daily_basic` | PE/PB/PS、换手率、市值；不是完整行情快照 |
| 财务指标 | `fina_indicator` | ROE、毛利率、营收/利润同比等优先从这里拿 |
| 三大表原始财务 | `income` / `balancesheet` / `cashflow` | `fina_indicator` 不够时再补 |
| 个股资金流 | `moneyflow` | 大单/中单/小单等 |
| 沪深港通 | `moneyflow_hsgt` / `hsgt_top10` | 北向南向总量、活跃股 |
| 指数日线 | `index_daily` | A股指数 |
| 指数成分和权重 | `index_weight` | 主题/宽基成分股 |
| ETF/场内基金列表 | `fund_basic` | `market=E` 常用于 ETF |
| ETF日线 | `fund_daily` | 场内基金行情 |
| 场外基金净值 | `fund_nav` | 开放式基金净值 |
| 交易日历 | `trade_cal` | 先确认 `trade_date` 是否是交易日 |

### 常见拼表规则

1. A 股“快照/选股池”
   - 先用 `daily_basic`
   - 补 `stock_basic` 的 `name/industry`
   - 补 `daily` 的 `amount`

2. A 股历史 K 线
   - `daily`
   - 如需复权，再拼 `adj_factor`

3. 个股基本面
   - 先 `fina_indicator`
   - 再用 `daily_basic` 补 PE/PB/换手率/市值
   - 必要时再下钻 `income/balancesheet/cashflow`

### 先查字段，再判定“没数据”

当 Tushare 返回空表、缺列、或数值明显不对时，先按这个顺序检查：

1. `ts_code` 是否正确
   - A 股股票通常是 `000001.SZ` / `600000.SH`
   - A 股指数常见是 `000300.SH` / `000905.SH` / `399001.SZ`
   - 场外基金常见是 `022365.OF`，不要拿裸码直接查 `fund_nav`
   - 不要把裸代码直接喂给只接受 `ts_code` 的接口

2. 日期是否有效
   - `trade_date` 不是自然日
   - 收盘后接口常要用最近交易日，必要时先查 `trade_cal`

3. 接口是否选错
   - `daily_basic` 没有 `name/industry/amount`
   - `stock_basic` 是静态表，不是行情表
   - `daily` 有 `amount`，但没有 PE/PB

4. 字段名是否记错
   - `daily`: `vol`, `amount`
   - `daily_basic`: `turnover_rate`, `pe_ttm`, `total_mv`, `circ_mv`
   - `fina_indicator`: 常用 `roe`, `grossprofit_margin`, `or_yoy`, `op_yoy`, `q_profit_yoy`

5. 单位是否误读
   - `daily.amount`: `千元`
   - `daily.vol`: `手`
   - `daily_basic.total_mv/circ_mv`: `万元`
   - `daily_basic.total_share/float_share/free_share`: `万股`
   - `moneyflow_hsgt.hgt/sgt/north_money/south_money`: `百万元`
   - `hsgt_top10.amount/net_amount/buy/sell`: `元`
   - `margin.rzye/rzmre/rzche/rqye/rzrqye`: 默认按 `元` 处理

6. 权限/积分是否覆盖
   - 某些接口可调用，但返回空或历史范围受限，先排查积分和权限

7. 是否只是缓存旧了
   - 当天盘中、未收盘、节假日、或缓存 TTL 过长都可能造成“看起来像没数据”

只有以上都排查完，才降级到其他数据源。

### 当前仓库里要遵守的默认策略

- A 股默认 `Tushare first`
- Tushare 不完整时优先“补表/修字段/修单位”，不是立刻换源
- 降级到 `AKShare / efinance / Yahoo` 时，要在结果里明确这是 fallback
- 资金流和两融统一后，内部金额口径默认按 `元` 存储；只有展示时再换成 `亿`

### 官方文档

- [stock_basic](https://tushare.pro/document/2?doc_id=25)
- [trade_cal](https://tushare.pro/document/2?doc_id=26)
- [daily](https://tushare.pro/document/2?doc_id=27)
- [adj_factor](https://tushare.pro/document/2?doc_id=28)
- [daily_basic](https://tushare.pro/document/2?doc_id=32)
- [moneyflow](https://tushare.pro/document/2?doc_id=170)
- [fina_indicator](https://tushare.pro/document/2?doc_id=79)
- [income](https://tushare.pro/document/2?doc_id=33)
- [balancesheet](https://tushare.pro/document/2?doc_id=36)
- [cashflow](https://tushare.pro/document/2?doc_id=44)
- [index_daily](https://tushare.pro/document/2?doc_id=95)
- [index_weight](https://tushare.pro/document/2?doc_id=96)
- [moneyflow_hsgt](https://tushare.pro/document/2?doc_id=47)
- [hsgt_top10](https://tushare.pro/document/2?doc_id=48)
- [fund_basic](https://tushare.pro/document/1?doc_id=19)
- [fund_daily](https://tushare.pro/document/2?doc_id=127)
- [fund_nav](https://tushare.pro/document/2?doc_id=119)
