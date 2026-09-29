"""File readers for the watched folder: .csv/.tsv/.txt (delimited or free text), .xlsx and .xls.

Real-world mess handled deliberately: BOMs and legacy encodings (cp1252), delimiter sniffing, bad
lines (captured, not silently dropped), Excel date cells, empty rows/columns, multi-sheet workbooks.
Everything is read as strings first; typing happens later against the data contract."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from swarmpipe.core.errors import PermanentError

NA_VALUES = ["", " ", "NA", "N/A", "n/a", "null", "NULL", "None", "none", "nan", "NaN", "-"]
_XL_DATETIME = re.compile(r"^(\d{4}-\d{2}-\d{2}) 00:00:00$")
_DELIMS = [",", "\t", "|", ";"]


@dataclass
class SniffResult:
    extension: str
    size: int
    kind_hint: str                    # tabular | document | binary | unknown
    encoding: str | None = None
    delimiter: str | None = None
    header: list[str] | None = None
    sample_lines: list[str] = field(default_factory=list)
    est_lines: int = 0
    reason: str = ""

    def to_dict(self) -> dict:
        return {k: getattr(self, k) for k in ("extension", "size", "kind_hint", "encoding", "delimiter", "header", "est_lines", "reason")} | {
            "sample_lines": self.sample_lines[:8]}


@dataclass
class Frame:
    name: str
    df: pd.DataFrame
    bad_lines: list[dict] = field(default_factory=list)


def detect_encoding(raw: bytes) -> str:
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if raw.startswith(b"\xff\xfe") or raw.startswith(b"\xfe\xff"):
        return "utf-16"
    for enc in ("utf-8", "cp1252"):
        try:
            raw.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _delimiter(lines: list[str]) -> tuple[str | None, float]:
    best, best_score = None, 0.0
    for d in _DELIMS:
        counts = [ln.count(d) for ln in lines]
        if not counts or max(counts) == 0:
            continue
        mode = max(set(counts), key=counts.count)
        if mode == 0:
            continue
        consistency = counts.count(mode) / len(counts)
        score = consistency * min(mode, 10)
        if consistency >= 0.8 and score > best_score:
            best, best_score = d, score
    return best, best_score


def sniff(path: Path) -> SniffResult:
    ext = path.suffix.lower()
    size = path.stat().st_size
    with open(path, "rb") as f:
        head = f.read(65536)
    if ext == ".xlsx":
        ok = head.startswith(b"PK\x03\x04")
        return SniffResult(ext, size, "tabular" if ok else "binary", reason="xlsx zip container" if ok else "not a valid xlsx (zip) file")
    if ext == ".xls":
        ok = head.startswith(b"\xd0\xcf\x11\xe0")
        return SniffResult(ext, size, "tabular" if ok else "binary", reason="xls OLE2 container" if ok else "not a valid xls (OLE2) file")
    if b"\x00" in head[:4096] and not (head.startswith(b"\xff\xfe") or head.startswith(b"\xfe\xff")):
        return SniffResult(ext, size, "binary", reason="NUL bytes in a text file")
    if size == 0:
        return SniffResult(ext, size, "unknown", reason="empty file")
    enc = detect_encoding(head)
    text = head.decode(enc, errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()][:30]
    if len(lines) > 2 and len(head) == 65536:
        lines = lines[:-1]  # last line may be cut
    est = int(size / max(1, len(head)) * text.count("\n")) if head else 0
    delim, score = _delimiter(lines)
    if delim and len(lines) >= 2:
        header = [h.strip() for h in lines[0].split(delim)]
        return SniffResult(ext, size, "tabular", enc, delim, header, lines, est, f"consistent '{delim}' delimiter (score {score:.1f})")
    return SniffResult(ext, size, "document", enc, None, None, lines, est, "no consistent delimiter: free text")


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    drop = [c for c in df.columns if c.startswith("Unnamed:") and df[c].isna().all()]
    if drop:
        df = df.drop(columns=drop)
    df = df.dropna(how="all")
    for c in df.columns:
        s = df[c]
        if s.dtype == object or pd.api.types.is_string_dtype(s):
            s = s.astype("str").where(s.notna(), None).str.strip()
            s = s.str.replace(_XL_DATETIME, r"\1", regex=True)
            df[c] = s.where(s != "", None)
    return df.reset_index(drop=True)


def read_frames(path: Path, sn: SniffResult) -> list[Frame]:
    try:
        if sn.extension in (".xlsx", ".xls"):
            engine = "openpyxl" if sn.extension == ".xlsx" else "xlrd"
            sheets = pd.read_excel(path, sheet_name=None, dtype=str, engine=engine, na_values=NA_VALUES, keep_default_na=False)
            frames = [Frame(str(name), _clean(df)) for name, df in sheets.items()]
            frames = [f for f in frames if not f.df.empty]
            if not frames:
                raise PermanentError("workbook has no non-empty sheets", code="EMPTY_FILE")
            return frames
        bad: list[dict] = []

        def _on_bad(line: list[str]):
            bad.append({"fields": len(line), "preview": sn.delimiter.join(line)[:200]})
            return None

        df = pd.read_csv(path, sep=sn.delimiter, dtype=str, keep_default_na=False, na_values=NA_VALUES,
                         encoding=sn.encoding, engine="python", on_bad_lines=_on_bad, skipinitialspace=True)
        frame = Frame(path.stem, _clean(df), bad)
        if frame.df.empty:
            raise PermanentError("file contains a header but no data rows", code="EMPTY_FILE")
        return [frame]
    except PermanentError:
        raise
    except Exception as exc:  # corrupt workbook, undecodable text, parser errors ...
        raise PermanentError(f"cannot parse {path.name}: {type(exc).__name__}: {exc}", code="PARSE_ERROR") from exc


def read_text(path: Path, sn: SniffResult) -> str:
    raw = path.read_bytes()
    return raw.decode(sn.encoding or detect_encoding(raw[:65536]), errors="replace")
