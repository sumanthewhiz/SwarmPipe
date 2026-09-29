"""Data plane: readers, contracts, transforms, data-assurance checks, publishing/rollback."""
from datetime import date

import pandas as pd

from swarmpipe import scenarios
from swarmpipe.data.profiling import psi, numeric_bins
from swarmpipe.data.readers import read_frames, sniff
from swarmpipe.data.transforms import apply_contract


def test_readers_handle_formats_and_encodings(tmp_path):
    day = date(2026, 9, 28)
    xlsx, xls = scenarios.s_reference(tmp_path, None, day)
    frames = read_frames(xlsx, sniff(xlsx))
    assert [f.name for f in frames] == ["customers", "regions"]
    assert len(read_frames(xls, sniff(xls))[0].df) == 120
    txt = scenarios.s_pipe_txt(tmp_path, None, day)[0]
    sn = sniff(txt)
    assert sn.kind_hint == "tabular" and sn.delimiter == "|"
    cp = scenarios.s_encoding(tmp_path, None, day)[0]
    assert sniff(cp).encoding == "cp1252"
    assert "José Müller" in read_frames(cp, sniff(cp))[0].df["name"].tolist()
    bad = scenarios.s_malformed(tmp_path, None, day)
    assert sniff(bad[0]).kind_hint == "binary" and sniff(bad[1]).kind_hint == "binary"
    doc = scenarios.s_runbook_doc(tmp_path, None, day)[0]
    assert sniff(doc).kind_hint == "document"


def test_contract_matching_by_file_and_sheet(svc):
    assert svc.contracts.match("customers.xlsx", "regions", 1, 2)["dataset"] == "regions"
    assert svc.contracts.match("customers.xlsx", "customers", 0, 2)["dataset"] == "customers"
    assert svc.contracts.match("sales_2026-09-28.csv", None)["dataset"] == "sales_daily"
    assert svc.contracts.match("vendors_q3.csv", None) is None


def test_transform_types_rejects_dedupes_and_maps_aliases(svc):
    c = svc.contracts.active("sales_daily")
    df = scenarios.sales_df(date(2026, 9, 28), n=10).astype(str).rename(columns={"customer_id": "cust_id"})
    df.loc[0, "quantity"] = "not-a-number"
    df = pd.concat([df, df.iloc[[5]]], ignore_index=True)
    tr = apply_contract(df, c, svc.contracts.alias_map(c))
    assert tr.diff["via_alias"] == {"cust_id": "customer_id"}
    assert tr.stats["rejected"] == 1 and tr.stats["duplicates_removed"] == 1 and tr.stats["rows_out"] == 9
    assert str(tr.df["quantity"].dtype) == "Int64"


def test_psi_detects_scale_change():
    base = numeric_bins(pd.Series(range(1, 500)).astype(float))
    assert psi(base, pd.Series(range(1, 500)).astype(float)) < 0.05
    assert psi(base, pd.Series(range(1, 500)).astype(float) * 100) > 1.0


def test_circuit_breaker_quarantines_and_keeps_last_good_version(base_svc):
    from tests.conftest import drive

    before = base_svc.publishing.current("default", "sales_daily")["id"]
    drive(base_svc, "volume_drop")
    after = base_svc.publishing.current("default", "sales_daily")["id"]
    assert before == after
    assert base_svc.db.scalar("SELECT COUNT(*) FROM dataset_versions WHERE dataset='sales_daily' AND status='quarantined'") == 1
    failed = {r["check_name"] for r in base_svc.db.query("SELECT check_name FROM check_results WHERE status='fail'")}
    assert {"volume_vs_baseline", "row_count_min"} <= failed


def test_rollback_restores_tampered_table_from_snapshot(base_svc):
    from swarmpipe.data.warehouse import Warehouse  # noqa: F401

    v = base_svc.publishing.current("default", "sales_daily")
    base_svc.wh.conn().execute(f'UPDATE "{v["table_name"]}" SET amount = amount * 2 WHERE rowid % 10 = 0')
    assert base_svc.wh.checksum(v["table_name"]) != v["checksum"]
    res = base_svc.publishing.restore_from_snapshot(v["id"], "user:test")
    assert res["restored"] and base_svc.wh.checksum(v["table_name"]) == v["checksum"]
