# /risk

## 输入

- `report`
- `correlation`
- `stress "<场景名>"`

## 执行流程

1. 读取 `data/portfolio.json` 当前持仓
2. 拉取或读取缓存中的历史行情
3. 计算组合收益率、相关性、VaR、CVaR、最大回撤、波动率和 Beta
4. 按 `config/stress_scenarios.yaml` 做场景映射和压力测试

## 输出

- `report`：完整风险报告，含阈值告警
- `correlation`：持仓相关性矩阵和集中度提醒
- `stress`：指定场景下的组合冲击与持仓贡献
