# 强因子维护说明

这份文件现在是“维护入口页”，不是阶段 J 的完整实施说明。

默认只有在任务直接涉及：

- 强因子维护
- 因子 metadata / 状态机
- 因子是否能进入 `strategy`
- 因子家族级外审或校准

时再读。

详细的 v1 过程与历史设计见 [docs/history/strong_factors_v1.md](../history/strong_factors_v1.md)。

## 当前状态

强因子工程已按 `v1 已收口` 管理，不再作为当前主开发主线。

当前判断是：

- J-1 ~ J-5 已进入产品链
- 共享因子 metadata 合同已落地
- family-level 产物和 review 记录已存在
- `review_audit` 当前对 `structured-round` 审计为 `0 active findings`

后续同类问题进入常规：

- today final
- 外审
- 校准
- `strategy fixtures / point-in-time coverage`

## 家族快照

| 家族 | 当前状态 | 现在主要作用 | 仍未完全放开的边界 |
| --- | --- | --- | --- |
| `J-1` 价量结构与 setup | `production_factor` | 进入评分、叙事、动作 | 继续做 setup 校准，不等于无限补新形态 |
| `J-2` 季节 / 日历 / 事件窗 | `scoring_supportive` 为主 | 提供事件窗和样本边界说明 | `lag / visibility fixture` 不完整前，不应进一步升格 |
| `J-3` breadth / chips | `scoring_supportive` | 提供行业扩散、龙头确认、拥挤风险 | 必须持续区分市场级 / 行业级 / 个股级代理 |
| `J-4` 质量 / 盈利修正 | `scoring_supportive` | 提供质量、杠杆、基础财务约束 | EPS 修正类仍受 point-in-time 源限制 |
| `J-5` ETF / 基金专属 | `scoring_supportive` | 服务 `etf_pick / fund_pick / scan(cn_etf/cn_fund)` | 先服务产品链，不要反向从 `strategy` 倒推产品合同 |

## 现在还归这条线管什么

这条线现在主要负责四件事：

1. 因子合同不漂移
   因子进入评分、叙事、动作后，metadata / proxy / degradation / point-in-time 口径要保持一致。
2. 家族维护而不是无上限扩张
   新补充应优先服务已有家族的校准、边界和解释质量，而不是继续堆新名字。
3. 产品链优先
   新因子先在 `scan / analysis / pick / renderer / tests` 收口，再讨论是否值得进 `strategy challenger`。
4. 研究层升格门禁
   只有点时可见性、lag 合同和数据质量稳定的因子，才允许进入 `strategy` 候选池。

## 已迁出的长尾

这些问题不再当成“强因子主线未完成”：

- `J-4 EPS 修正`
  等可靠 point-in-time 源接入后再升格，转入阶段 `E / I`
- `J-2 政策事件窗`
  `lag / visibility fixture` 完成后再讨论升格，转入阶段 `E / I`
- setup / breadth / 质量阈值再校准
  转入阶段 `F`

## 最小因子合同

以后新因子或老因子升格，默认至少要能回答下面这些字段：

- `factor_id`
- `family`
- `source_type`
- `visibility_class`
- `degraded`
- `proxy_level`
- `supports_scoring`
- `supports_strategy_candidate`

默认要求：

- `scan / pick / decision_review / strategy` 用同一套字段名
- 先接共享 contract / helper，再让下游消费
- 没有 metadata 合同的因子，不算“已进入产品层”

## 因子状态机

默认状态仍然是：

- `observation_only`
- `scoring_supportive`
- `production_factor`
- `strategy_challenger`
- `champion_candidate`

默认升格规则：

- 没完成 `lag / visibility fixture`，最高只能到 `scoring_supportive`
- 没完成家族级外审收敛，不能标成 `production_factor`
- 没完成 `strategy` 验证，不能标成 `champion_candidate`

## 默认维护顺序

1. 先修产品合同
   `processor / renderer / action wording / tests`
2. 再看 metadata / proxy / degradation 是否一致
3. 再做校准或 review 固化
4. 最后才讨论是否能送进 `strategy challenger`

不要反过来先做 `strategy` 变量，再回头补产品链。

## 什么时候需要 family-level 验证

只有在下面情况同时出现时，才值得从 patch-level 升到家族级：

- 一个家族的合同明显变化
- 因子不只是“表里多一行”，而是真的改变评分、叙事或动作
- 相关 spot check 已稳定

这时再补：

- today final
- round-based 外审
- lesson / backlog / guard 固化

## 最小相关文件

- `src/processors/technical.py`
- `src/processors/opportunity_engine.py`
- `src/processors/factor_meta.py`
- 受影响的 `pick / scan / renderer / tests`

## 最小验证

```bash
pytest -q
python -m src.commands.scan 300308
python -m src.commands.stock_pick --preview
```

如果只动 ETF / 基金专属因子，再补：

```bash
python -m src.commands.etf_pick --preview
python -m src.commands.fund_pick --preview
```
