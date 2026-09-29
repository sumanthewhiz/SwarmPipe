"""Intermediate frame store for step artifacts (raw frames, typed batches, kept rows).

Steps must be restartable, so large intermediate results are persisted as files and only their
paths are checkpointed. Pickle keeps pandas dtypes and the index exactly (Parquet would be the
production choice, but a native pyarrow build is not available on every platform - e.g. Windows on
ARM). These files are internal, trusted artifacts in the processing area: never unpickle files
that arrive from outside."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def save_frame(df: pd.DataFrame, path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_pickle(tmp)
    tmp.replace(path)
    return path


def load_frame(path: str | Path) -> pd.DataFrame:
    return pd.read_pickle(Path(path))
