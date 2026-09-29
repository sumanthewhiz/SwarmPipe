# Runbook: out-of-band change (table modified outside the pipeline)

Symptoms: out_of_band_change - the checksum of a published table no longer matches the checksum recorded at publish time.
Typical root cause: a manual backfill, a direct database write or an accidental update by a person or tool outside the pipeline.

Steps:
1. Restore the published version from its immutable snapshot (rollback_dataset to the current version restores it).
2. Notify the owner and find who changed the table (database audit logs).
3. Route future corrections through the pipeline so they are validated and audited.
