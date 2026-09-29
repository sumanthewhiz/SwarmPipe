"""LLM-as-judge calibration.

Before trusting a judge's scores we measure it against human labels:
  * agreement: exact, within-1, quadratic-weighted Cohen's kappa, Spearman rank correlation
  * verbosity bias: score change when an answer is padded with fact-free filler
  * position bias: pairwise preference consistency when A and B are swapped
Compare judge prompt v1 (naive) with v2 (explicit anti-verbosity rubric):
    swarmpipe evals calibrate-judge --version v1   /   --version v2
"""
from __future__ import annotations

import itertools
import tempfile
from pathlib import Path

from swarmpipe.core.util import remove_tree
from swarmpipe.evals.harness import Workspace, load_cases

FILLER = (" In addition, it is worth emphasizing that data is a strategic asset for the enterprise, that teams should always "
          "collaborate closely, communicate proactively with every stakeholder, follow established processes, document all decisions "
          "carefully, and continuously improve their practices over time to ensure long-term success, resilience and trust.")


def _kappa_quadratic(a: list[int], b: list[int], k: int = 5) -> float:
    n = len(a)
    if n == 0:
        return 0.0
    obs = [[0.0] * k for _ in range(k)]
    for x, y in zip(a, b):
        obs[x - 1][y - 1] += 1
    ha = [sum(obs[i]) for i in range(k)]
    hb = [sum(obs[i][j] for i in range(k)) for j in range(k)]
    num = den = 0.0
    for i in range(k):
        for j in range(k):
            w = ((i - j) ** 2) / ((k - 1) ** 2)
            num += w * obs[i][j]
            den += w * ha[i] * hb[j] / n
    return 1.0 - num / den if den else 1.0


def _ranks(xs: list[float]) -> list[float]:
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for t in range(i, j + 1):
            r[order[t]] = (i + j) / 2 + 1
        i = j + 1
    return r


def _spearman(a: list[float], b: list[float]) -> float:
    ra, rb = _ranks(a), _ranks(b)
    n = len(a)
    ma, mb = sum(ra) / n, sum(rb) / n
    cov = sum((x - ma) * (y - mb) for x, y in zip(ra, rb))
    va = sum((x - ma) ** 2 for x in ra) ** 0.5
    vb = sum((y - mb) ** 2 for y in rb) ** 0.5
    return cov / (va * vb) if va and vb else 0.0


def calibrate(version: str | None = None, console=None) -> dict:
    d = Path(tempfile.mkdtemp(prefix="swarmpipe_eval_judge_"))
    ws = Workspace(d)
    judge = ws.svc.agents.judge
    items = load_cases("judge_calibration.v1.jsonl")
    human, model, padded_delta, rows = [], [], [], []
    for it in items:
        s = judge.score(it["candidate"], it["reference"], version=version)["score"]
        sp = judge.score(it["candidate"] + FILLER, it["reference"], version=version)["score"]
        human.append(int(it["human_score"]))
        model.append(int(s))
        padded_delta.append(sp - s)
        rows.append({"id": it["id"], "human": it["human_score"], "judge": s, "judge_padded": sp})
    consistent, pairs = 0, 0
    by_ref: dict[str, list[dict]] = {}
    for it in items:
        by_ref.setdefault(it["reference"]["root_cause_category"], []).append(it)
    for ref, group in by_ref.items():
        for a, b in itertools.combinations(group, 2):
            w1 = judge.pairwise(a["candidate"], b["candidate"], a["reference"])["winner"]
            w2 = judge.pairwise(b["candidate"], a["candidate"], a["reference"])["winner"]
            pick1 = {"A": "a", "B": "b"}.get(w1, "tie")
            pick2 = {"A": "b", "B": "a"}.get(w2, "tie")
            consistent += 1 if pick1 == pick2 else 0
            pairs += 1
    ws.close()
    remove_tree(d)
    n = len(human)
    res = {"version": version or "latest", "items": n,
           "exact_agreement": round(sum(1 for h, m in zip(human, model) if h == m) / n, 3) if n else None,
           "within_1_agreement": round(sum(1 for h, m in zip(human, model) if abs(h - m) <= 1) / n, 3) if n else None,
           "kappa": round(_kappa_quadratic(human, model), 3), "spearman": round(_spearman(human, model), 3),
           "verbosity_bias": round(sum(padded_delta) / n, 3) if n else None,
           "position_consistency": round(consistent / pairs, 3) if pairs else None, "pairs": pairs, "rows": rows}
    res["trustworthy"] = bool(res["kappa"] >= 0.6 and abs(res["verbosity_bias"] or 0) <= 0.3 and (res["position_consistency"] or 0) >= 0.8)
    if console:
        console.print(f"judge {res['version']}: kappa={res['kappa']} spearman={res['spearman']} exact={res['exact_agreement']} "
                      f"within1={res['within_1_agreement']} verbosity_bias={res['verbosity_bias']} position_consistency={res['position_consistency']} "
                      f"-> {'TRUSTWORTHY' if res['trustworthy'] else 'NOT trustworthy yet'}")
    return res
