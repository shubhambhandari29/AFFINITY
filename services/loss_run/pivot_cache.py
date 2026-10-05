"""Shared-item metadata must describe the actual serialized cache values."""

from openpyxl.pivot.cache import SharedItems
from openpyxl.pivot.fields import Index
from openpyxl.pivot.table import RowColItem


def cache_field_values(values):
    """Index text case-insensitively like Excel; retain the first display spelling.

    Only the pivot cache is normalized, not the source/detail worksheet cells.
    """
    unique, references, lookup = [], [], {}
    for value in values:
        key = (
            isinstance(value, str),
            value.casefold() if isinstance(value, str) else value,
        )
        if key not in lookup:
            lookup[key] = len(unique)
            unique.append(value)
        references.append(lookup[key])
    return unique, references


def pivot_row_items(paths):
    """Encode shared row-axis parents with r, and only emit the changed suffix."""
    items, previous = [], []
    for path in paths:
        repeated = 0
        # Keep at least one x, including when a path is identical to its parent.
        while (
            repeated < min(len(previous), len(path) - 1)
            and previous[repeated] == path[repeated]
        ):
            repeated += 1
        items.append(RowColItem(r=repeated, x=[Index(v=v) for v in path[repeated:]]))
        previous = path
    return items + [RowColItem(t="grand", x=[Index(v=0)])]


def shared_items(items):
    # Excel defaults containsSemiMixedTypes/containsString to true when omitted.
    # That is inconsistent with numeric-only caches, despite openpyxl accepting it.
    types = {item.tagname for item in items} - {"m"}
    numbers = [item.v for item in items if item.tagname == "n"]
    return SharedItems(
        _fields=items,
        containsBlank=any(item.tagname == "m" for item in items),
        containsString="s" in types,
        containsSemiMixedTypes="s" in types or "m" in {i.tagname for i in items},
        containsMixedTypes=len(types) > 1,
        containsNumber="n" in types,
        containsInteger=bool(numbers)
        and types == {"n"}
        and all(float(value).is_integer() for value in numbers),
        containsDate="d" in types,
        containsNonDate=any(item.tagname != "d" for item in items),
        longText=any(item.tagname == "s" and len(item.v) > 255 for item in items),
    )
