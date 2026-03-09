# Investment Agent Skill

## 概述

个人投资决策辅助系统。输入标的代码，输出多维度结构化打分卡，并逐步扩展到简报、组合、风险和研究场景。

如果用户没有明确输入命令，而是直接用自然语言描述需求，应优先推断到最匹配的命令；也可以直接使用 `assistant <请求>` 自然语言入口。

当本地命令返回空数据、上游源不可用、或明显缺少用户关心的关键信息时，不要停在失败提示上。应按下面顺序继续完成任务：

1. 先尝试本地缓存和备用数据源
2. 仍不足时，允许联网补查
3. 联网时优先官方、一级媒体、交易所、央行、指数公司、基金公司或主流行情源
4. 最终回答里明确区分“程序输出”和“联网补充”

## 可用命令

| 命令 | 功能 | 示例 |
| --- | --- | --- |
| `scan <代码>` | 六维度标的扫描 | `scan 561380` |
| `snap <代码>` | 盘中实时快照 | `snap 561380` |
| `compare <代码...>` | 同类标的横向对比 | `compare 518880 518800 159934` |
| `briefing daily` | 每日晨报 | `briefing daily` |
| `briefing weekly` | 每周周报 | `briefing weekly` |
| `portfolio status` | 查看持仓状态 | `portfolio status` |
| `portfolio thesis` | 查看 / 管理论点 | `portfolio thesis HSTECH` |
| `portfolio log` | 记录操作 | `portfolio log buy QQQM 185.5 10000` |
| `portfolio review` | 月度操作复盘 | `portfolio review 2026-02` |
| `risk report` | 组合风险报告 | `risk report` |
| `risk stress` | 压力测试 | `risk stress "美股崩盘"` |
| `risk correlation` | 相关性矩阵 | `risk correlation` |
| `backtest <规则>` | 规则回测 | `backtest macd_golden_cross HSTECH 3y` |
| `discover` | 主动机会发现 | `discover` |
| `regime` | 当前宏观体制 | `regime` |
| `policy <关键词>` | 政策解读 | `policy 电网十四五规划` |
| `research <问题>` | 自由研究 | `research 如果降息我的组合会怎样` |
| `lookup <关键词>` | 中文名称 / 主题名转 ETF 代码 | `lookup 有色金属ETF代码是多少` |
| `assistant <请求>` | 自然语言路由入口 | `assistant 帮我写今天的晨报` |

## 工作目录

项目根目录：`AI-Finance/`

## 数据依赖

运行前需要配置 `config/config.yaml` 中的 API keys：

- `FRED_API_KEY`：从 [FRED](https://fred.stlouisfed.org/docs/api/api_key.html) 免费申请
- `TUSHARE_TOKEN`：从 [Tushare](https://tushare.pro) 注册获取，可选

## 运行方式

每个命令对应 `src/commands/` 下的同名 Python 文件，通过下面方式执行：

```bash
python -m src.commands.<命令名> <参数>
```
