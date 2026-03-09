"""Asset comparison helpers."""

from __future__ import annotations

from typing import Dict, Iterable, List


class AssetComparator:
    """Simple row-based comparator for same-category assets."""

    def compare(self, rows: Iterable[Dict[str, object]], better_is_lower: Iterable[str] = ()) -> List[Dict[str, object]]:
        better_is_lower = set(better_is_lower)
        rows = list(rows)
        if not rows:
            return []
        keys = set().union(*(row.keys() for row in rows)) - {"symbol"}
        comparisons = []
        for key in sorted(keys):
            values = [row.get(key) for row in rows if isinstance(row.get(key), (int, float))]
            if not values:
                continue
            best = min(values) if key in better_is_lower else max(values)
            for row in rows:
                value = row.get(key)
                if not isinstance(value, (int, float)):
                    continue
                icon = "✅" if value == best else "⚠️"
                comparisons.append({"symbol": row.get("symbol"), "metric": key, "value": value, "icon": icon})
        return comparisons
