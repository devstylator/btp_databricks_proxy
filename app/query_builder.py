"""
Applies an allow-listed column selection / filter / row-limit to a pandas
DataFrame already fetched via Delta Sharing.

Filters are evaluated entirely client-side with pandas boolean indexing
(never turned into a query string sent anywhere), so there is no
SQL/predicate-injection surface.
"""

import json
import re

import pandas as pd

IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
MAX_ROWS = 10000
DEFAULT_ROWS = 1000

ALLOWED_OPS = {
    "=": lambda series, value: series == value,
    "!=": lambda series, value: series != value,
    "<": lambda series, value: series < value,
    "<=": lambda series, value: series <= value,
    ">": lambda series, value: series > value,
    ">=": lambda series, value: series >= value,
    "LIKE": lambda series, value: series.astype(str).str.contains(
        _like_to_regex(value), regex=True, na=False
    ),
}


class QueryError(ValueError):
    """Raised for any invalid/unsafe input - mapped to HTTP 400 by callers."""


def _like_to_regex(pattern: str) -> str:
    """Translates a SQL LIKE pattern (% and _ wildcards) to a regex, with all
    other characters escaped so the value can't break out into a regex DoS
    or unintended match."""
    escaped = re.escape(pattern)
    return "^" + escaped.replace(r"\%", ".*").replace(r"\_", ".") + "$"


def apply_query(df: pd.DataFrame, columns: str | None, filters: str | None, top: int | None) -> pd.DataFrame:
    result = df

    column_list = (columns or "*").strip()
    if column_list != "*":
        cols = [c.strip() for c in column_list.split(",")]
        for c in cols:
            if not IDENTIFIER.match(c):
                raise QueryError(f'Invalid column name: "{c}"')
            if c not in result.columns:
                raise QueryError(f'Unknown column: "{c}"')
        result = result[cols]

    if filters:
        try:
            conditions = json.loads(filters)
        except json.JSONDecodeError as exc:
            raise QueryError('"filters" must be valid JSON') from exc
        if not isinstance(conditions, list):
            raise QueryError('"filters" must be a JSON array of {column, op, value}')

        for condition in conditions:
            column = condition.get("column")
            op = condition.get("op")
            value = condition.get("value")
            if not column or not IDENTIFIER.match(column):
                raise QueryError(f'Invalid filter column: "{column}"')
            if column not in result.columns:
                raise QueryError(f'Unknown filter column: "{column}"')
            if op not in ALLOWED_OPS:
                raise QueryError(f'Invalid filter operator: "{op}"')
            mask = ALLOWED_OPS[op](result[column], value)
            result = result[mask]

    limit = top if isinstance(top, int) and 0 < top <= MAX_ROWS else DEFAULT_ROWS
    return result.head(limit)
