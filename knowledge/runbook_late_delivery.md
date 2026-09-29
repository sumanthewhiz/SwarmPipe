# Runbook: late or missing delivery (freshness SLA)

Symptoms: freshness_overdue - the contract declares when the dataset is due and no successful load happened within the SLA plus grace.
Typical root cause: the upstream job did not run, failed, or the file transfer stalled.

Steps:
1. Check the upstream job and file transfer status with the source owner.
2. Request the delivery (request_resend) and inform the owners of downstream consumers that data will be late.
