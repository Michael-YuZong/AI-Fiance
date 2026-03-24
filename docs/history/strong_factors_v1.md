# 强因子工程 v1 归档

这份文件保存阶段 J 在进入维护模式前的较详细说明。

默认不要把它当开工入口。只有在需要回答下面这些问题时再读：

- 某个家族最初为什么这样拆
- 阶段 J 当时的完成边界是什么
- 某些历史约束为什么后来迁到 `strategy fixtures / point-in-time coverage`

## 原始目标

强因子工程解决的不是“多补几个因子”，而是把现有八维分析里偏弱、偏代理、偏解释层的部分升级成：

- 金融意义明确
- 点时边界清楚
- 能真实改变评分、叙事和动作
- 能回流到 `pick_history / decision_review / strategy`
- 能被外审持续追问并收敛

## 阶段 J v1 收口时的家族快照

| 家族 | 当时状态 | 核心落地 |
| --- | --- | --- |
| `J-1` 价量结构与 setup | `production_factor` | `setup_analysis()` 三种 setup + 评分 / 叙事 / 介入 |
| `J-2` 季节 / 日历 / 事件窗 | `scoring_supportive` | 显式样本边界 + 降级 + 事件窗子因子 |
| `J-3` breadth / chips | `scoring_supportive` | 行业宽度 / 龙头确认 / 拥挤度风险 |
| `J-4` 质量 / 盈利修正 | `scoring_supportive` | 现金流质量 / 杠杆压力 + `observation_only` 盈利动量 |
| `J-5` ETF / 基金专属 | `scoring_supportive` | 折溢价、份额申赎、跟踪误差、主题纯度、风格漂移等 |

## 当时为什么单独成专题

如果没有一条单独的强因子路线，会同时出现两个问题：

1. 产品层继续用偏弱因子解释结论，外审只能纠正文案，无法明显提升底层判断质量。
2. `strategy` 会在一组还没收口的弱因子上做验证和实验，最后把“代理问题”误判成“权重问题”。

当时的顺序要求是：

1. 先把产品因子做强
2. 再把满足 point-in-time / lag 条件的子集送进 `strategy challenger`
3. 再讨论 promotion / rollback 治理

## v1 时期的家族设计重点

### `J-1` 价量结构与 setup

目标是让技术面不只回答“多空方向”，而是回答“当前处在哪种 setup”。

当时重点包括：

- 多根 K 线组合形态
- 价格 vs `RSI / MACD / OBV` 背离
- 支撑 / 阻力与假突破、突破失败
- 放量突破 / 放量滞涨 / 放量下跌 / 缩量回调
- 波动压缩 / 扩张与启动前状态

### `J-2` 季节 / 日历 / 事件窗

目标是把“月度胜率 / 财报月 / 调样月”升级成更像事件窗因子。

当时重点包括：

- 财报前后窗口
- 指数调样 / 半年末 / 年末窗口
- 节假日消费与出行窗口
- 商品与能源的季节性窗口
- 政策会议、医保谈判、产业展会等主题事件窗

### `J-3` breadth / chips

目标是把“这只票强不强”升级成“行业有没有扩散、龙头有没有确认、资金是不是太挤”。

当时重点包括：

- benchmark-relative 强弱
- 行业内上涨家数 / 创新高家数 / 扩散比例
- 龙头确认与二线跟随
- 北向 / ETF 份额 / 行业资金流 / 股东变化
- 拥挤度、热度和反身性风险

### `J-4` 质量 / 盈利修正 / 估值协同

目标是把基本面从“价格位置 + 少量财务代理”升级成更能回答“增长是真是假、估值有没有被透支”的因子组。

当时重点包括：

- 盈利动量 / EPS 修正代理
- 业绩预告 / 快报 / surprise
- 经营现金流、资产负债率、杠杆与偿债压力
- 毛利率 / ROE / 利润率稳定性
- PEG / 增长-质量-估值协同

### `J-5` ETF / 基金专属

目标是让 ETF / 场外基金不再只靠底层资产代理和八维通用框架分析。

ETF 重点包括：

- 折溢价
- 份额申赎 / 资金流
- 跟踪偏离 / 跟踪误差
- 成分股集中度
- 主题纯度与跨市场暴露

场外基金重点包括：

- 业绩基准拟合度
- 风格漂移
- 基金经理稳定性
- 产品定位和持仓集中度
- 申赎友好度、费率和确认节奏

## 当时的硬约束

阶段 J v1 时强调过的边界：

- 市场级 / 行业级 / 个股级 / 基金产品级信号必须分开
- 代理层级必须显式披露
- 没有可靠 point-in-time 源的修正类因子，先停在实验台或辅助说明
- 没完成 `lag / visibility fixture` 的因子，不允许继续升格
- ETF / 基金专属因子优先接进产品链，不先做成 `strategy` 变量

## 产品接入顺序

v1 设计时要求的顺序是：

1. `technical.py / opportunity_engine.py`
2. `scan / stock_analysis`
3. `stock_pick / fund_pick / etf_pick`
4. `briefing`
5. `pick_history / decision_review / retrospect`
6. 最后才讨论 `strategy`

## 因子元数据与状态机

v1 阶段就已明确，新因子默认要带最小 metadata：

- `factor_id`
- `family`
- `source_type`
- `source_as_of`
- `visibility_class`
- `degraded`
- `proxy_level`
- `supports_scoring`
- `supports_strategy_candidate`

当时的状态机：

- `observation_only`
- `scoring_supportive`
- `production_factor`
- `strategy_challenger`
- `champion_candidate`

## 当时的外审与验收粒度

阶段 J v1 用三层 done：

- `patch-level`
- `factor-family-level`
- `stage-level`

并强调：

- patch 默认不要求每次都跑 today final 外审
- family-level 必须重跑 today final + 外审收敛
- stage-level 必须完成 lesson / backlog / guard 沉淀

## 为什么后来迁出

后续这几类问题不再归阶段 J 主开发：

- `J-4 EPS 修正`
  等可靠 point-in-time 源接入后再升格，迁到阶段 `E / I`
- `J-2 政策事件窗`
  `lag / visibility fixture` 完成后再讨论升格，迁到阶段 `E / I`
- setup / breadth / 质量阈值再校准
  迁到阶段 `F`
