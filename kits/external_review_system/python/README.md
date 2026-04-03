# Python Reference Implementation

这是一套给 Python 项目用的参考实现。

它不是完整框架，只负责把外审机制最核心的 4 层落下来：

- review scaffold
- review ledger
- review audit
- final gate

## 推荐放置方式

把这一目录整体复制到目标项目，例如：

```text
src/external_review_kit/
```

然后按你的包路径调整 import。

## 这些文件分别做什么

- `review_record_utils.py`
  解析 round record 的公共工具。
- `review_scaffold.py`
  自动生成 round 1 review scaffold，并在无 actionable finding 时自动补 round-based close。
- `review_ledger.py`
  把 review markdown 做成 latest-by-series summary。
- `review_audit.py`
  审计 review 协议本身有没有执行到位。
- `final_gate.py`
  在正式交付前检查 review 是否 PASS，并写 release manifest。
- `cli_review_ledger.py`
  ledger 命令行入口。
- `cli_review_audit.py`
  audit 命令行入口。

## 最小接入示例

```python
from pathlib import Path

from yourpkg.external_review_kit.final_gate import ReviewGateError, review_path_for, run_final_gate
from yourpkg.external_review_kit.review_scaffold import ensure_external_review_scaffold

outputs_root = Path("reports")
reviews_root = Path("reports/reviews")
artifact_path = outputs_root / "final" / "demo_final.md"

review_path = review_path_for(artifact_path, outputs_root=outputs_root, reviews_root=reviews_root)
if not review_path.exists():
    ensure_external_review_scaffold(
        review_path=review_path,
        artifact_path=artifact_path,
        artifact_type="design_doc",
        scaffold_generated_by="demo --final",
    )
    raise SystemExit(f"Review scaffold created: {review_path}")

run_final_gate(
    artifact_path=artifact_path,
    outputs_root=outputs_root,
    reviews_root=reviews_root,
    artifact_type="design_doc",
    artifact_text=artifact_path.read_text(encoding="utf-8"),
)
```

## 你必须自己补的部分

这套参考实现故意不懂你的业务规则。

目标项目里至少要自己补：

1. 结构审 prompt 的领域检查点
2. final gate 的 domain validators
3. audit 的 manifest / contract hook
4. 正式产物路径和 review 路径的约定
