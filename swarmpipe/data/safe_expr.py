"""A tiny, safe, vectorized expression language for contract business rules and derived columns.

Why: LLM- or user-supplied logic must never reach eval()/exec() (sandboxed execution;
OWASP ASI05 "unexpected code execution"). Expressions are parsed with `ast`, validated against an
allowlist of node types, column names and functions, then interpreted over pandas Series."""
from __future__ import annotations

import ast
import operator

import numpy as np
import pandas as pd

from swarmpipe.core.errors import GuardrailViolation

_BINOPS = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv,
           ast.Mod: operator.mod, ast.FloorDiv: operator.floordiv}
_CMPOPS = {ast.Eq: operator.eq, ast.NotEq: operator.ne, ast.Lt: operator.lt, ast.LtE: operator.le,
           ast.Gt: operator.gt, ast.GtE: operator.ge}


def _abs(x):
    return x.abs() if isinstance(x, pd.Series) else abs(x)


def _round(x, n=0):
    return x.round(int(n)) if isinstance(x, pd.Series) else round(x, int(n))


FUNCS = {
    "abs": _abs,
    "round": _round,
    "isnull": lambda x: x.isna() if isinstance(x, pd.Series) else pd.isna(x),
    "notnull": lambda x: x.notna() if isinstance(x, pd.Series) else not pd.isna(x),
    "lower": lambda x: x.astype(str).str.lower(),
    "upper": lambda x: x.astype(str).str.upper(),
    "length": lambda x: x.astype(str).str.len(),
    "coalesce": lambda a, b: a.fillna(b) if isinstance(a, pd.Series) else (b if pd.isna(a) else a),
}
_ALLOWED = (ast.Expression, ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare, ast.Name, ast.Load, ast.Constant,
            ast.Call, ast.And, ast.Or, ast.Not, ast.USub, ast.UAdd, *_BINOPS.keys(), *_CMPOPS.keys())


def validate(expr: str, columns: set[str]) -> ast.Expression:
    if not isinstance(expr, str) or len(expr) > 400:
        raise GuardrailViolation("expression must be a string of at most 400 characters", code="UNSAFE_EXPRESSION")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise GuardrailViolation(f"invalid expression syntax: {exc.msg}", code="UNSAFE_EXPRESSION") from exc
    nodes = list(ast.walk(tree))
    if len(nodes) > 120:
        raise GuardrailViolation("expression too complex", code="UNSAFE_EXPRESSION")
    for node in nodes:
        if not isinstance(node, _ALLOWED):
            raise GuardrailViolation(f"disallowed syntax: {type(node).__name__}", code="UNSAFE_EXPRESSION")
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in FUNCS or node.keywords:
                raise GuardrailViolation("only allowlisted functions may be called: " + ", ".join(sorted(FUNCS)), code="UNSAFE_EXPRESSION")
        if isinstance(node, ast.Name) and node.id not in columns and node.id not in FUNCS:
            raise GuardrailViolation(f"unknown column '{node.id}'", code="UNSAFE_EXPRESSION")
        if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float, str, bool, type(None))):
            raise GuardrailViolation("unsupported constant", code="UNSAFE_EXPRESSION")
    return tree


def _eval(node, df: pd.DataFrame):
    if isinstance(node, ast.Expression):
        return _eval(node.body, df)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        col = df[node.id]
        if pd.api.types.is_numeric_dtype(col):
            return col.astype("float64")
        return col
    if isinstance(node, ast.BinOp):
        return _BINOPS[type(node.op)](_eval(node.left, df), _eval(node.right, df))
    if isinstance(node, ast.UnaryOp):
        v = _eval(node.operand, df)
        if isinstance(node.op, ast.USub):
            return -v
        if isinstance(node.op, ast.UAdd):
            return v
        return ~v if isinstance(v, pd.Series) else (not v)
    if isinstance(node, ast.BoolOp):
        vals = [_eval(v, df) for v in node.values]
        out = vals[0]
        for v in vals[1:]:
            out = (out & v) if isinstance(node.op, ast.And) else (out | v)
        return out
    if isinstance(node, ast.Compare):
        left = _eval(node.left, df)
        result = None
        for op, comp in zip(node.ops, node.comparators):
            right = _eval(comp, df)
            part = _CMPOPS[type(op)](left, right)
            result = part if result is None else (result & part)
            left = right
        return result
    if isinstance(node, ast.Call):
        return FUNCS[node.func.id](*[_eval(a, df) for a in node.args])
    raise GuardrailViolation(f"cannot evaluate {type(node).__name__}", code="UNSAFE_EXPRESSION")


def evaluate(expr: str, df: pd.DataFrame) -> pd.Series:
    tree = validate(expr, set(map(str, df.columns)))
    with np.errstate(all="ignore"):
        out = _eval(tree, df)
    if not isinstance(out, pd.Series):
        out = pd.Series([out] * len(df), index=df.index)
    return out
