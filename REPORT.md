# Data Engineering — Report Notes

## Task 5.3: Anonymisation Justification (< 500 words)

The augmented Atlantis subsample is anonymised along three axes — location,
time, and direct identifiers — followed by a singleton quarantine step.

**Location (~500 m).** Exact `latitude`/`longitude` are removed and replaced
with an **H3 resolution-9 index**. Resolution-9 cells have an edge length of
roughly 174 m and an area of about 0.1 km², so a point is generalised to a
cell of roughly 500 m across. This preserves neighbourhood-level spatial
utility (useful for operational analysis) while making it impossible to
recover the exact property or address from the coordinates.

**Time (~6 h).** The exact `creation_timestamp` (and `completion_timestamp`)
are removed and replaced with a **6-hour bucket** (floored to 00:00, 06:00,
12:00, 18:00). This retains coarse temporal patterns (time-of-day demand)
while preventing correlation of a request to a specific moment a resident was
observed making a call.

**Direct and quasi identifiers.** We drop `notification_number` and
`reference_number` (unique request identifiers that link back to City records
and the individual), as well as the exact timestamps and coordinates already
generalised above. Remaining fields (directorate, department, code, suburb)
describe the *service*, not the *person*.

**Singleton quarantine (k-anonymity).** Even after generalisation, a record
that is **alone** in its (6-hour bucket, resolution-9 cell) combination is a
k=1 singleton: it could still single out the one resident who made a request
at that place and time. We therefore **quarantine all singletons** into a
separate `atlantis_manual_review.csv` for human review, and publish only the
non-singleton records (`atlantis_anonymised.csv`). In this dataset, 4,177 of
7,213 records were singletons — a high proportion, because ~500 m + 6-hour
precision is fine-grained relative to the sparse request density around
Atlantis. This is an explicit privacy/utility trade-off: a production release
that needed to publish more records could coarsen further (e.g. H3 res-8 and
12-hour buckets) to increase group sizes, at the cost of spatial/temporal
precision.

**Why this is now anonymised.** No field allows direct identification
(identifiers removed); location and time are generalised beyond the stated
precision requirements (≤500 m, ≤6 h); and residual re-identification risk
from rare combinations is handled by holding singletons back for manual
review rather than publishing them. The published subset therefore contains
no record that can be tied to an individual resident on the basis of an
unusually precise or unique location/time signature.
