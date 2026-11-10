"""Recognize explicit coordinate evidence without geocoding or axis guesses."""
from __future__ import annotations

import math
import re
from typing import Any

_NUMBER = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
_PAIR = re.compile(r"^\s*(" + _NUMBER + r")(?:\s*,\s*|\s+)(" + _NUMBER + r")\s*$")


def _finite_number(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def has_coordinates(value: Any) -> bool:
    """Return whether a raw or normalized location contains a numeric pair.

    Source order and values are never modified. Direct pairs and explicitly
    named coordinate fields count; place names, numeric POI IDs, and integer
    text-link positions do not. Zero is valid evidence. This does not assert
    an axis order, coordinate system, GPS accuracy, or posting location.
    """
    if isinstance(value, str):
        match = _PAIR.fullmatch(value)
        return bool(match and all(_finite_number(v) for v in match.groups()))
    if isinstance(value, (list, tuple)):
        if len(value) == 2 and all(_finite_number(v) for v in value):
            return True
        return any(has_coordinates(v) for v in value if isinstance(v, (dict, list, tuple)))
    if not isinstance(value, dict):
        return False
    for latitude, longitude in (("latitude", "longitude"), ("lat", "lon"),
                                ("lat", "lng"), ("lat", "longitude"),
                                ("latitude", "lng"), ("latitude", "lon")):
        if _finite_number(value.get(latitude)) and _finite_number(value.get(longitude)):
            return True
    for key in ("coordinates", "coordinate", "geo", "position", "detail"):
        if key in value and has_coordinates(value[key]):
            return True
    # Only normalized location evidence wrappers, not arbitrary post objects.
    if "kind" in value or "field" in value or "evidence" in value:
        if has_coordinates(value.get("value")):
            return True
        for evidence in value.get("evidence_objects") or []:
            if isinstance(evidence, dict) and has_coordinates(evidence.get("value")):
                return True
    return False
