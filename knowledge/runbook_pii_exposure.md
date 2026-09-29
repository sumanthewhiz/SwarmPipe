# Runbook: undeclared PII (card numbers, emails or phone numbers in free-text columns)

Symptoms: pii_undeclared warns - personal data was detected in columns the contract does not declare as PII, such as notes.
Typical root cause: agents or users typed sensitive data into free-text fields in the source application.

Steps:
1. The pipeline tokenizes detected values before publishing; verify no raw values reached published tables.
2. If a previously published version contains raw values, quarantine that version.
3. Notify the data owner and the privacy team; the source application should block free-text PII.
