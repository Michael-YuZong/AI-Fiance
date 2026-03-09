# 个人投资决策辅助 Agent 系统架构设计 V2

本文档依据需求说明整理，覆盖系统定位、七大业务模块、数据采集层、命令体系、技术栈和开发路线，作为仓库内实现参考。

## 系统定位

- 信息聚合
- 规则引擎
- 风险管理
- 交互式研究助手

原则：

- AI 负责信息效率和逻辑推演
- 人负责最终判断
- 不做预测
- 不输出置信度
- 不替人做决策

## 覆盖范围

- A 股行业 / 主题 ETF
- 港股科技 ETF
- 美股大盘 ETF
- 黄金
- 商品期货

## 七大业务模块

### 模块 1：主动发现

- 事件驱动扫描
- 信号驱动扫描
- 异动驱动扫描
- 产业链传导图谱推演
- 宏观 Regime 识别
- 全球资金流向追踪

### 模块 2：标的扫描

- 宏观环境
- 板块轮动与产业链逻辑
- 行业 / 板块基本面
- 资金与情绪
- 技术面
- 跨市场联动
- 盘中实时快照
- 标的横向对比

### 模块 3：组合管理

- 持仓、成本、仓位管理
- 风险敞口计算
- 目标配置偏离提醒
- 再平衡建议
- 分批建仓 / 止盈规则
- Thesis Tracker
- 操作日志与决策复盘

### 模块 4：情报简报

- 每日晨报
- 每周周报
- 事件快报
- 政策深度解读

### 模块 5：交互式研究

- 自由提问
- 联动前述模块数据
- 关注逻辑推演、相关性、组合影响和历史类比

### 模块 6：风险管理

- 最大回撤、波动率、夏普
- VaR / CVaR
- 持仓相关性矩阵
- 压力测试
- 风险限制规则

### 模块 7：简易回测

- 规则回测
- 因子有效性验证
- 与买入持有对比
- 分时段统计
- 明确不做参数优化和高频回测

## 数据采集层

主要数据源：

- AKShare
- yfinance
- FRED
- Tushare
- RSS / 新闻源
- 社媒站点

目录设计：

```text
data_collector/
├── config.yaml
├── scheduler.py
├── collectors/
├── processors/
├── storage/
├── output/
└── utils/
```

## Skill 设计

命令集合：

- `scan <代码>`
- `snap <代码>`
- `compare <代码...>`
- `briefing daily|weekly`
- `portfolio status|rebalance|log|thesis|review`
- `risk report|stress|correlation`
- `backtest <规则> <代码> <区间>`
- `discover`
- `regime`
- `policy <关键词或URL>`
- `research <问题>`

## 技术栈

- Python 3.11+
- pandas / numpy / scipy
- SQLite
- APScheduler
- Anthropic API
- Markdown 报告输出

## 开发路线

### Phase 1

- 搭数据采集基座
- 实现技术指标引擎
- 落 `scan` 命令基础版
- 接 SQLite

### Phase 2

- 扩到六维度扫描
- 简报
- 组合管理
- 盘中快照
- 对比命令

### Phase 3

- 主动发现
- 产业链推理
- Regime
- 政策解读
- Thesis 与复盘

### Phase 4

- 风险模块
- 压力测试
- 简易回测
- Research 全量接入

### Phase 5

- 社媒情绪
- 全球资金流
- 移动端推送
- 开源准备

## 当前仓库实现策略

当前代码以 Phase 1 为主，后续模块已预留文件位和基础骨架，逐 Phase 扩展。
