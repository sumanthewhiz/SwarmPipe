# Runbook: stale data re-sent (content freshness)

Symptoms: data_freshness fails - the newest business date inside the file is many days old, even though the file itself arrived on time.
Typical root cause: the export job re-sent an older extract (wrong partition or a cached file). Arrival-based monitoring misses this; content-based freshness catches it.

Steps:
1. Quarantine the batch; publishing it would overwrite current data with old data.
2. Request a re-send of the correct business date from the source owner.
