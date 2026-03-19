# Context Map

这份文件的目标只有一个：减少开工时的默认读取量。

## 默认读法

绝大多数任务只需要读：

1. [README.md](../README.md)
2. [AGENTS.md](../AGENTS.md)
3. 你要改的 command / processor / renderer / test

只有任务相关时再继续展开。

## 按任务读什么

### 只改单个命令或输出

读：

- 对应 `src/commands/*.py`
- 对应 `src/processors/*.py`
- 对应 `src/output/*.py`
- 对应 `tests/`

不用默认读：

- [plan.md](../plan.md)
- `docs/plans/strategy.md`
- 大部分 YAML

### 改配置或数据源

先读：

- [config/README.md](../config/README.md)

再按任务读：

- `config/config*.yaml`
- `config/watchlist.yaml`
- `config/stock_pools.yaml`
- `config/news_feeds.yaml`

### 改研究、推荐、组合主链路

先读：

- [docs/status_snapshot.md](./status_snapshot.md)

再读：

- 对应 command / processor / renderer / tests

### 改 `strategy`

先读：

- [docs/plans/strategy.md](./plans/strategy.md)

再读：

- `src/commands/strategy.py`
- `src/processors/strategy.py`
- `src/output/strategy_report.py`
- `src/storage/strategy.py`
- `tests/test_*strategy*`

### 改强因子工程 / 维护强因子

先读：

- [docs/plans/strong_factors.md](./plans/strong_factors.md)
- [docs/status_snapshot.md](./status_snapshot.md)

当前默认口径：

- 阶段 J 已按 `v1 已收口` 进入维护模式
- 新任务优先落在 `校准 / point-in-time / strategy fixtures`
- 不要默认把“继续补新因子”当成当前第一优先级

再读：

- `src/processors/technical.py`
- `src/processors/opportunity_engine.py`
- 相关 pick / analysis command
- 对应 tests

### 改外审 / guard / 文档合同

先读：

- `docs/prompts/`
- `docs/review_kit/README.md`
- [docs/status_snapshot.md](./status_snapshot.md)

必要时再读：

- [plan.md](../plan.md)
- `src/reporting/review_ledger.py`
- `src/commands/review_ledger.py`
- `src/reporting/review_audit.py`
- `src/commands/review_audit.py`

## 默认不要读什么

这些文件默认不是开工前入口：

- [docs/architecture_v2.md](./architecture_v2.md)
  这是历史架构草案，不是当前主合同。
- `reports/`
  这些是输出产物，不是默认上下文。
- `tmp/`
  临时生成文件。
- `.pytest_cache/README.md`
  无产品价值。

## 最常见的最小上下文组合

### 修一个 command bug

- [AGENTS.md](../AGENTS.md)
- 对应 command / processor / renderer / test

### 改 pick 流水线

- [AGENTS.md](../AGENTS.md)
- [docs/status_snapshot.md](./status_snapshot.md)
- 对应 pick command / output / tests
- 相关 config 文件

### 改 `strategy`

- [AGENTS.md](../AGENTS.md)
- [docs/plans/strategy.md](./plans/strategy.md)
- `strategy` 代码和 tests

### 改 YAML

- [config/README.md](../config/README.md)
- 目标 YAML

## 目标

如果一个新 session 只是要改一条主链路，理想读取量应该控制在：

- 1 个短入口文档
- 1 个专题文档
- 2 到 4 个代码 / 测试文件

而不是把整个 repo 的 `.md` / `.yaml` 全塞进上下文。
