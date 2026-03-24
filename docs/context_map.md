# Context Map

这份文件只做一件事：把默认上下文压到最小。

## 默认读法

绝大多数任务只需要读：

1. [README.md](../README.md)
2. [AGENTS.md](../AGENTS.md)
3. 你要改的 command / processor / renderer / test

只有任务相关时再继续展开。

## 按任务读什么

| 任务 | 先读 | 再按需读 | 默认别读 |
| --- | --- | --- | --- |
| 修单个 command / renderer bug | 对应 `src/commands`、`src/processors`、`src/output`、`tests` | [docs/status_snapshot.md](./status_snapshot.md) | `plan.md`、大部分 YAML |
| 改配置 / 数据源 | [config/README.md](../config/README.md) | 目标 YAML、必要时 `config/config*.yaml` | 其它无关 YAML |
| 改推荐 / 简报 / 组合主链路 | [docs/status_snapshot.md](./status_snapshot.md) | 对应 command / processor / renderer / tests | `docs/plans/strategy.md` |
| 改 `strategy` | [docs/plans/strategy.md](./plans/strategy.md) | `src/commands/strategy.py`、`src/processors/strategy.py`、`src/output/strategy_report.py`、`src/storage/strategy.py`、`tests/test_*strategy*` | `plan.md`、大部分 prompt |
| 改强因子维护 | [docs/plans/strong_factors.md](./plans/strong_factors.md) | [docs/status_snapshot.md](./status_snapshot.md)、相关 processor / tests | 历史 final / reports |
| 改外审 / guard / prompt | [docs/prompts/README.md](./prompts/README.md)、[docs/review_kit/README.md](./review_kit/README.md) | [docs/status_snapshot.md](./status_snapshot.md)、`review_ledger / review_audit` 代码 | 业务 command 细节 |

## 默认不要读什么

这些文件或目录默认不是开工入口：

- [docs/history/architecture_v2.md](./history/architecture_v2.md)
  历史架构草案，不是当前主合同。
- [docs/history/2026-03.md](./history/2026-03.md)
  详细变更归档，只在需要追历史判断时再读。
- `reports/`
  输出产物，不是默认上下文。
- `tmp/`
  临时生成文件。
- `.pytest_cache/`
  无产品价值。

## 最小上下文模板

修一个 command bug：

- [AGENTS.md](../AGENTS.md)
- 对应 command / processor / renderer / test

改一条主链路：

- [AGENTS.md](../AGENTS.md)
- [docs/status_snapshot.md](./status_snapshot.md)
- 对应 command / processor / renderer / test

改 `strategy`：

- [AGENTS.md](../AGENTS.md)
- [docs/plans/strategy.md](./plans/strategy.md)
- `strategy` 代码和 tests

改 YAML：

- [config/README.md](../config/README.md)
- 目标 YAML

## 目标

一个普通 patch 的默认读取量应控制在：

- 1 个短入口文档
- 1 个专题文档
- 2 到 4 个代码 / 测试文件

而不是把整个 repo 的 `.md` / `.yaml` 全塞进上下文。
