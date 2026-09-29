# Runbook: volume anomaly (truncated or partial extract)

Symptoms: volume_vs_baseline fails with a large negative change; row_count_min may fail; reconciliation totals are far below normal.
Typical root cause: the upstream extract job timed out or was cut off, and a partial file was delivered. The job "ended OK" but the data is incomplete.

Steps:
1. Keep the batch quarantined. Do not publish partial data: dashboards and downstream agents would treat it as complete.
2. Hold downstream derived datasets so they keep the last complete version (hold_downstream).
3. Request a complete re-send from the source owner (request_resend) and notify the dataset owner.
4. When a complete file arrives and passes checks, release the holds.
