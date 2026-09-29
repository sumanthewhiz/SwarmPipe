# Runbook: malformed or unreadable file (dead-letter queue)

Symptoms: pipeline_failure - a file was dead-lettered at intake or at the route/read step (unsupported content, corrupt workbook, binary data in a .csv, empty file).
Typical root cause: the export was interrupted, the wrong file type was uploaded, or a transfer corrupted the file.

Steps:
1. Inspect the dead-letter entry (reason and error) and the file in the dlq folder.
2. Request a valid re-delivery from the source owner (request_resend).
3. After fixing the cause, redrive the run with `swarmpipe dlq redrive <id>` if the file itself was fine (for example a transient bug).
