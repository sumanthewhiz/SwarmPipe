# Runbook: referential integrity break (orphan foreign keys)

Symptoms: referential check fails - many customer_id values in sales have no match in the published customers dataset.
Typical root cause: the reference dataset (customers) is stale or was loaded with missing records, or sales references a new customer range that has not been delivered yet.

Steps:
1. Check whether the upstream reference dataset has an open incident or is late.
2. Hold downstream derived datasets that join the two.
3. Request a fresh reference extract from its source owner.
