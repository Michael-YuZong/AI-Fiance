# 外审能力迁移清单

把这套能力搬到别的项目时，按下面顺序做。

## 第一层：先搬协议

- [ ] 明确“什么产物算正式交付物”
- [ ] 明确正式交付物默认不能绕过独立外审
- [ ] 明确 `Pass A 结构审` 与 `Pass B 发散审` 必须分离
- [ ] 明确 round-based 停止条件
- [ ] 明确 finding 必须固化到 `prompt / guard / tests / backlog` 至少一层

## 第二层：再搬文档资产

- [ ] 搬 `prompts/generic_structural_reviewer.md`
- [ ] 搬 `prompts/generic_divergent_reviewer.md`
- [ ] 搬 `prompts/external_review_convergence_loop.md`
- [ ] 搬 `prompts/artifact_revision_loop.md`
- [ ] 搬 `templates/review_record_template.md`
- [ ] 搬 `templates/review_ledger_schema.md`

## 第三层：再接工具

- [ ] 在目标项目里约定 review records 目录
- [ ] 接入 review scaffold
- [ ] 接入 review ledger
- [ ] 接入 review audit
- [ ] 接入 final gate
- [ ] 接入 release manifest

## 第四层：最后补领域规则

- [ ] 结构审 prompt 加入目标项目的事实核验点
- [ ] final gate 加入目标项目的交付合同检查
- [ ] audit 加入目标项目的 manifest / contract 审计
- [ ] 为最容易复发的问题补 tests / fixtures

## 迁移后第一轮自检

- [ ] 能不能自动生成 round 1 review scaffold
- [ ] 能不能跑出 round 2 记录
- [ ] ledger 能不能看出这条 loop 仍未收敛
- [ ] audit 能不能抓到 reviewer 分工缺失
- [ ] audit 能不能抓到 PASS 但正文仍有 actionable finding
- [ ] final gate 能不能拦住 review 未 PASS 的正式交付
- [ ] final gate 放行后，manifest 能不能被 audit 追踪到
