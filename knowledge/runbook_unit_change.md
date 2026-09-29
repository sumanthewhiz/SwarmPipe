# Runbook: distribution shift and unit or scale changes

Symptoms: distribution_psi fails for amount or price columns; the mean is about 100x (or 1/100) of the baseline; control totals jump.
Typical root cause: an upstream unit change, for example amounts sent in paise instead of rupees, or a currency conversion applied twice.

Steps:
1. Confirm with the mean ratio: a ratio close to 100 or 0.01 strongly suggests a unit change rather than real business growth.
2. Keep the batch quarantined and hold downstream rebuilds.
3. Ask the source owner to confirm the unit and re-send. Do not rescale data silently inside the pipeline.
