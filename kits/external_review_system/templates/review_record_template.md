# 外审记录模板

```md
# `<对象名>` 外审结果

- 审稿时间：2026-04-01 18:30:00
- 审稿对象：[target.md](/absolute/path/to/target.md)
- 适用 prompt：[generic_structural_reviewer.md](/absolute/path/to/prompt.md)
- previous_round：[..._round1.md](/absolute/path/to/round1.md)
- 审稿方式：`Pass A 结构审 -> 修正 -> Pass B 发散审`
- 结构审 prompt：[generic_structural_reviewer.md](/absolute/path/to/prompt.md)
- 发散审 prompt：[generic_divergent_reviewer.md](/absolute/path/to/prompt.md)
- review_target：`path/or/logical_target`
- review_prompt：`path/to/prompt.md`

## 一句话总评

一句话说明当前 round 的整体判断。

## 主要问题

1. `P1/P2/P3` ...
2. ...

## 独立答案

- reviewer 用自己的思路给出的简版答案。

## 框架外问题

1. reviewer 自己发散找到的框架外问题
2. ...

## 零提示发散审

1. 把同一份交付物当成唯一输入、先不沿用 rich checklist 时，最先冒出来的问题
2. 如果没有新增问题，也要明确写：`零提示二审未发现新的实质性问题`

## 建议沉淀

- prompt
  - 要新增或修改什么
- hard rule / guard / workflow
  - 要新增或修改什么
- tests / fixtures
  - 要新增或修改什么
- lesson / backlog
  - 要登记什么

## 收敛结论

- round：2
- previous_round：1
- 结构审执行者：`reviewer_structural`
- 发散审执行者：`reviewer_divergent`
- 状态：PASS / BLOCKED
- 无新的 P0/P1：是 / 否
- 本轮新增 P0/P1：是 / 否
- 上一轮 P0/P1 是否已关闭：是 / 否 / 不适用
- carried_p0_p1：
  - ...
- closed_items：
  - ...
- new_divergent_findings：
  - ...
- zero_prompt_findings：
  - ...
- solidification_actions：
  - ...
- 本轮是否收敛：是 / 否
- 是否建议继续下一轮：是 / 否
- 允许作为成稿交付：是 / 否
- 是否允许开始实现：是 / 否
- 说明：...
```

## 使用规则

- `框架外问题` 不能省略
- `零提示发散审` 不能省略
- `建议沉淀` 不能省略
- `结构审执行者 / 发散审执行者` 不能省略，且不能是同一个 reviewer / 子 agent
- `收敛结论` 必须可被 parser 抽取
