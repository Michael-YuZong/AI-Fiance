# 强因子工程化计划

## 1. 目标

这份计划解决的不是“再多补几个因子”，而是把现有八维分析里还偏弱、偏代理、偏解释层的部分，升级成：

- 金融意义明确
- 点时边界清楚
- 能真实改变评分、叙事和动作
- 能回流到 `pick_history / decision_review / strategy`
- 能被外审持续追问并收敛

这里的“强因子”不是“因子更多”，而是“因子更能支撑产品判断和后验校准”。

## 2. 当前状态

已经落地的第一批技术增强：

- `量价 / 动量背离`
- `K线形态` 从单根 K 升级到最近 `1-3` 根组合形态

它们已经进入：

- `scan / stock_analysis`
- `stock_pick / fund_pick / etf_pick`
- 技术叙事
- 介入条件

**2026-03-15 首次 family-level 收口（J-1 ~ J-5）：**

| 因子家族 | 状态 | 核心落地 |
|---|---|---|
| J-1 价量结构与 setup | ✅ production | `setup_analysis()` 三种 setup + 评分/叙事/介入 |
| J-2 季节/日历/事件窗 | ✅ scoring_supportive | 显式样本边界 + 降级 + 7 子因子 |
| J-3 breadth/chips | ✅ scoring_supportive | 行业宽度/龙头确认/拥挤度风险 |
| J-4 质量/盈利修正 | ✅ scoring_supportive | 现金流质量/杠杆压力 + observation_only 盈利动量 |
| J-5 ETF/基金专属 | ✅ scoring_supportive | 9 个子因子全部落地；ETF 份额申赎 + 真实跟踪误差已补入 |
| factor_meta.py | ✅ 共享合同（已消费） | FactorMeta + FACTOR_REGISTRY J-1~J-5；factor_id 已接入 J-5 _factor_row 输出 |

相关因子家族回归、产品链回归与治理回归已通过；具体数字以 [docs/status_snapshot.md](../status_snapshot.md) 的最新记录为准。

**2026-03-16 阶段 J 结案边界（v1）**

- 强因子工程按 `v1 已收口` 切出主开发主线，不再把“继续补新因子”当当前第一优先级。
- 当前结案判断不是“以后不再改”，而是：
  - J-1 ~ J-5 已进入产品链
  - 共享因子 metadata 合同已落地
  - family-level 产物和 review 记录已存在
  - `review_audit` 当前对 `structured-round` 协议审计为 `0 active findings`
- 后续同类问题进入常规 today final / 外审 / 校准节奏，不再单独算作阶段 J 未完成。

当前还明显偏弱的因子家族：

1. ~~价量结构剩余 setup~~ ✅
2. ~~季节 / 日历 / 事件窗~~ ✅
3. ~~breadth / chips~~ ✅
4. ~~质量 / 盈利修正 / 估值协同~~ ✅（基础层，EPS修正仍 observation_only）
5. ~~ETF / 基金专属因子~~ ✅

已迁出的长尾工作：

- J-4 EPS 修正：等待可靠 point-in-time 源接入后再升格，转入 `阶段 E / I`
- J-2 政策事件窗：lag/visibility fixture 完成后可升格，转入 `阶段 E / I`
- setup / breadth / 质量阈值再校准，转入 `阶段 F`

## 3. 为什么单独成专题

如果没有一条单独的强因子路线，后面会同时出现两个问题：

1. 产品层继续用偏弱因子解释结论，外审只能纠正文案，无法明显提升底层判断质量。
2. `strategy` 会在一组还没收口的弱因子上做验证和实验，最后把“代理问题”误判成“权重问题”。

所以这条线的顺序必须是：

1. 先把产品因子做强
2. 再把满足 point-in-time / lag 条件的子集送进 `strategy challenger`
3. 再讨论 promotion / rollback 治理

## 4. 因子家族顺序

### `J-1` 价量结构与 setup

目标：让技术面不只是“多空方向”，而是能回答当前处在哪种 setup。

优先内容：

- 多根 K 线组合形态
- 价格 vs `RSI / MACD / OBV` 背离
- 支撑 / 阻力与假突破、突破失败
- 放量突破 / 放量滞涨 / 放量下跌 / 缩量回调
- 波动压缩 / 扩张与启动前状态

当前状态：

- `背离` 和 `组合 K 线` 已经完成第一轮
- 下一步优先补：
  - 假突破 / 失败突破
  - 支撑失效后的 setup 分流
  - 压缩后放量启动 vs 情绪释放追价区分

### `J-2` 季节 / 日历 / 事件窗

目标：把当前偏规则化的“月度胜率 / 财报月 / 调样月”升级成更像事件窗因子。

优先内容：

- 财报前后窗口
- 指数调样 / 半年末 / 年末窗口
- 节假日消费与出行窗口
- 商品与能源的季节性窗口
- 政策会议、医保谈判、产业展会等主题事件窗

硬约束：

- 必须显式披露样本边界
- 不能把“历史常见窗口”写成“这次一定有效”
- 没有足够样本时默认降级成观察提示
- 没有完成 `lag / visibility fixture` 前，不允许直接进入主评分

### `J-3` breadth / chips

目标：把“这只票强不强”从单标涨跌，升级成“行业有没有扩散、龙头有没有确认、资金是不是太挤”。

优先内容：

- benchmark-relative 强弱
- 行业内上涨家数 / 创新高家数 / 扩散比例
- 龙头确认与二线跟随
- 北向 / ETF 份额 / 行业资金流 / 股东变化
- 拥挤度、热度和反身性风险

硬约束：

- 市场级 / 行业级 / 个股级 / 基金产品级信号必须分开
- 代理层级必须显式披露
- 不能把板块代理写成个股自身优势

### `J-4` 质量 / 盈利修正 / 估值协同

目标：把基本面从“价格位置 + 少量财务代理”升级成更能回答“增长是真是假、估值有没有被透支”的因子组。

优先内容：

- 盈利动量 / EPS 修正代理
- 业绩预告 / 快报 / surprise
- 经营现金流、资产负债率、杠杆与偿债压力
- 毛利率 / ROE / 利润率稳定性
- PEG / 增长-质量-估值协同

硬约束：

- 没有可靠 point-in-time 源的修正类因子，先停在实验台或辅助说明
- 不允许把财报后信息回填到预测时点
- 没有完成 `lag / visibility fixture` 的修正类因子，不允许进入主评分

### `J-5` ETF / 基金专属因子

目标：让 ETF / 场外基金不是只靠底层资产代理和八维通用框架来分析。

ETF 优先内容：

- 折溢价
- 份额申赎 / 资金流
- 跟踪偏离 / 跟踪误差
- 成分股集中度
- 主题纯度与跨市场暴露

场外基金优先内容：

- 业绩基准拟合度
- 风格漂移
- 基金经理稳定性
- 产品定位和持仓集中度
- 申赎友好度、费率和确认节奏

硬约束：

- 这些因子优先接进 `fund_pick / etf_pick / scan(cn_fund/cn_etf)`
- 不要先做成 `strategy` 变量，再回头补产品链

## 5. 产品接入顺序

### 产品层

- `technical.py / opportunity_engine.py`
  - 负责因子计算、评分、summary、action
- `scan / stock_analysis`
  - 负责把强因子直接展示成“为什么这么评”
- `stock_pick / fund_pick / etf_pick`
  - 负责把强因子写进推荐理由、备选淘汰理由和 rerun diff
- `briefing`
  - 对能形成 watchlist setup 的因子，进入日度观察池解释

### 校准层

- `pick_history`
  - 记录同日重跑时哪些因子变化改变了结论
- `decision_review / retrospect`
  - 用 setup bucket 复盘哪些因子组合更容易成功 / 失败
- `report_review_lessons`
  - 记录哪些因子最容易被过度解释

### `strategy` 层

- 不是所有产品因子都自动进入 `strategy`
- 只有满足：
  - point-in-time 可见性明确
  - lag 合同明确
  - 数据质量足够稳定
  的因子，才允许进入 `strategy_challenger`

## 6. 因子元数据合同

以后新因子默认要带一组最小元数据：

- `factor_id`
- `family`
- `source_type`
- `source_as_of`
- `visibility_class`
- `degraded`
- `proxy_level`
- `supports_scoring`
- `supports_strategy_candidate`

不是所有字段都必须立刻对客户可见，但至少在内部 payload 和验证层要能拿到。

默认要求：

- 新因子进入评分前，先接到统一的 factor metadata payload / helper
- `scan / pick / decision_review / strategy` 用同一套字段名
- 第一落点优先共享 contract/helper 层，再由 `pick_history / decision_review / strategy` 做适配消费
- 如果某个新因子还没有 metadata 合同，默认不能算“已进入产品层”

## 7. 因子状态机

新因子默认不再只有“上了/没上”两种状态，而要明确处在哪个阶段：

- `observation_only`
  - 只作为提示或观察线索，不进入主评分
- `scoring_supportive`
  - 可以进入产品评分，但还不是高权重主因子
- `production_factor`
  - 已进入主评分、叙事和动作链
- `strategy_challenger`
  - 已满足 point-in-time / lag 条件，允许进入 `strategy` 候选池验证
- `champion_candidate`
  - 已通过产品层与研究层验证，允许进入后续 promotion 讨论

默认要求：

- 没有完成 lag / visibility fixture 的因子，最高只能到 `scoring_supportive`
- 没有完成因子族级外审收敛的因子，不能标成 `production_factor`
- 没有完成 `strategy` 验证的因子，不能标成 `champion_candidate`

## 8. 外审节奏

### 分层 done

#### `patch-level done`

- 逻辑已经接进正确层级
- 相关测试通过
- 有至少一个真实样例验证
- 文档在需要时已同步

#### `factor-family-level done`

- 一个因子家族已经形成稳定合同
- today final 已重跑
- 已走外审循环并收敛
- 外审里的长期问题已沉淀

#### `stage-level done`

- 因子路线目标完成
- 输出合同清楚
- 测试和真实样例都具备
- 已走外审循环
- lesson / backlog / guard 已沉淀

### review 粒度

- `patch-level`
  - 单次实现改动或单个小因子 patch
  - 必须过 tests + 真实样例
  - 不要求每次都单独跑 today final 外审
- `factor-family-level`
  - 一个因子家族完成最小收口，例如 `J-1` 或 `J-2`
  - 必须重跑 today final + 外审收敛
- `stage-level`
  - 强因子路线的阶段性完成
  - 必须完成外审分流、lesson 回写、calibration / strategy 回流路径确认

### 最小 review target 矩阵

- `J-1 / J-2 / J-3`
  - 至少重跑：
    - 一条真实 `scan`
    - `stock_pick today final`
  - 如果该家族已经进入 `cn_fund / cn_etf` 评分或正文，再同步重跑受影响的 `fund_pick / etf_pick final`
- `J-4`
  - 至少重跑：
    - 一条真实 `scan`
    - `stock_pick today final`
  - 只有当 ETF / 基金的基本面代理或产品画像也被改动时，才扩到 `fund_pick / etf_pick final`
- `J-5`
  - 至少重跑：
    - `scan(cn_fund 或 cn_etf)`
    - 受影响的 `fund_pick / etf_pick final`
  - 默认不要求同步重跑 `stock_pick`

### 外审重点

每次因子家族外审默认重点追问：

- 这个因子是不是被写得比证据更强
- 这个因子到底是个股级、行业级还是市场级
- 这个因子是否受 point-in-time / lag 污染
- 这个因子改变了评分后，叙事和动作有没有同步改变
- 这个因子如果失效，最可能是信号错、数据错、还是 regime 错

要明确：

- 外审能抓“解释过头、边界不清、产品口径不一致”
- 外审不能替代“因子是否真有增量”的科学验证

## 9. 当前第一优先级

先锁 `J-1` 收口，不要把 `J-2` 混进同一轮 family 外审。

本轮子目标：

- 继续补强价格结构与量价共振
- 优先补：
  - 假突破 / 失败突破
  - 压缩后启动
  - 支撑失效后的 setup 分流
- 明确每个新因子的：
  - 金融含义
  - 点时边界
  - 降级逻辑
  - 代理层级

本轮验收：

- `J-1` 至少再新增一组真实强因子进入产品层
- 因子不仅出现在表里，也要进入叙事和动作
- 至少补一组 tests / fixtures
- 至少完成：
  - 一条真实 `scan`
  - 一条真实 `pick`
- 不把不确定因子硬升格成高置信主因子

本轮之后：

1. `J-2` 季节 / 日历 / 事件窗
2. `J-3` breadth / chips
3. `J-5` ETF / 基金专属因子
4. `J-4` 盈利修正 / 质量 / 估值协同
