"""Shared-item metadata must describe the actual serialized cache values."""

from openpyxl.pivot.cache import SharedItems


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
