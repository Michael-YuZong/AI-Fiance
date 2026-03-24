# 外审记录模板

> 用途：任何 round-based 外审都可以用这份模板落记录。  
> 目标：保证不同 reviewer、不同模块、不同项目的外审记录至少有同一套可比较字段。

```md
# `<对象名>` 外审结果

- 审稿时间：2026-03-15 18:30:00 CST
- 审稿对象：[target.md](/absolute/path/to/target.md)
- 适用 prompt：[external_xxx_reviewer.md](/absolute/path/to/prompt.md)
- previous_round：[..._round1.md](/absolute/path/to/round1.md)
- 审稿方式：`Pass A 结构审 -> 修正 -> Pass B 发散审`
- 结构审 prompt：[external_financial_structural_reviewer.md](/absolute/path/to/prompt.md)
- 发散审 prompt：[external_financial_divergent_reviewer.md](/absolute/path/to/prompt.md)
- review_target：`path/or/logical_target`
- review_prompt：`path/to/prompt.md`

## 结论

`go` / `hold` / `blocked`

## 总评

一句话说明当前 round 的整体判断。

## 主要问题

1. `P1/P2/P3` ...
2. ...

## 缺失的验证手段

- ...

## 缺失的风险控制

- ...

## 框架外问题

1. reviewer 自己发散找到的框架外问题
2. ...

## 零提示发散审

1. 把同一份 Markdown 当成唯一输入、先不沿用 rich checklist 时，最先冒出来的问题
2. 这一轮相比结构化审稿额外多出来的担心
3. 如果没有新增问题，也要明确写：`零提示二审未发现新的实质性问题`

## 建议沉淀

- prompt
  - 要新增或修改什么
  - 建议固化方式：`prompt`
- hard rule / guard / workflow
  - 要新增或修改什么
  - 建议固化方式：`hard rule / guard / workflow`
- tests / fixtures
  - 要新增或修改什么
  - 建议固化方式：`test / fixture`
- lesson / backlog
  - 要登记什么
  - 建议固化方式：`lesson / backlog`

## 收敛结论

- round：2
- previous_round：1
- 结构审执行者：`gpt-5.4 / reviewer_structural`
- 发散审执行者：`gpt-5.4-mini / reviewer_divergent`
- 状态：PASS / BLOCKED
- 本轮新增 P0/P1：是 / 否
- 上一轮 P0/P1 是否已关闭：是 / 否
- carried_p0_p1：...
- closed_items：
  - ...
- new_divergent_findings：...
- zero_prompt_findings：...
- solidification_actions：
  - ...
- 本轮是否收敛：是 / 否
- 是否建议继续下一轮：是 / 否
- 允许作为成稿交付：是 / 否
- 是否允许开始实现：是 / 否
- 说明：...
```

## 使用规则

- 如果是报告终稿外审，至少要保留：
  - `允许作为成稿交付`
- 如果是计划外审，至少要保留：
  - `是否允许开始实现`
- `框架外问题` 不能省略
- `零提示发散审` 不能省略
- `建议沉淀` 不能省略
- `结构审执行者 / 发散审执行者` 不能省略，且不能是同一个 reviewer / 子 agent
- `收敛结论` 必须可被 parser 抽取
