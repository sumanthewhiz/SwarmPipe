# Runbook: schema drift (renamed or missing columns)

Symptoms: schema_required_columns fails; unknown columns appear (schema_new_columns); rename candidates are listed.
Typical root cause: the upstream system renamed columns in its export (for example cust_id -> customer_ref, amount -> amt) without a contract change.

Steps:
1. Compare the missing contract columns with the new columns. Check value patterns, not just names: a rename is only safe when the values match the contract pattern (for example customer ids look like C0001).
2. If the mapping is well supported, reprocess the quarantined batch with the column mapping (reprocess_with_mapping). This needs approval.
3. Ask the source owner to either restore the old names or file a contract change. Never silently accept a breaking change.
4. For purely additive changes (new optional columns), propose a contract update (update_contract) for human review.
