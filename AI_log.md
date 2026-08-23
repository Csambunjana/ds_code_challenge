# AI Assistance Log

This project was completed with the help of an AI coding assistant, used in an
interactive, pair-programming style. I directed the work, made the design
decisions, wrote/edited the code in my editor, and ran and verified everything
myself. This log records what the AI was asked to help with, the model used,
token usage, and an example where I corrected the AI's suggestion.

## Model used

Kiro AI assistant (Auto model selection). The assistant chooses an underlying
model per task, so a single fixed model name is not applicable.

## Token usage

Exact token counts were not exposed by the assistant interface, so a precise
figure is unavailable. I am recording this transparently rather than
fabricating a number. As a rough indication, the assistance spanned an extended
interactive session covering environment setup, all three Data Engineering
tasks, debugging, testing, and documentation.

## Summary of AI-assisted work

Throughout the project I directed the work: I set the requirements, made the
design and interpretation decisions, ran every script myself, and reviewed and
corrected the AI's output where it was wrong or incomplete. The AI was used as
a drafting and explanation aid, not as an unsupervised author. Several times I
had to instruct it to change direction or fix mistakes it had introduced (see
the detailed example below, plus the notes in the "My role" column).

| Area | What the AI was asked to help with | My role (instructing / correcting) |
|------|-------------------------------------|-------------------------------------|
| Setup | Advise on project structure, `requirements.txt`, AWS `cct` profile, robust path handling | Chose the structure and ran all setup. Instructed the AI when it initially placed scripts and config in the wrong folders, and had it move code into `src/` and anchor paths to the script location so the repo runs from any directory. |
| Task 1 | Explain AWS S3 SELECT on GeoJSON; draft the extraction, validation and non-binary conformance-score logic; design `conf/schema.json` | Wrote/edited the code, defined the schema constraints and threshold, ran and validated. Corrected the AI when it tried to add comments to a JSON config (invalid JSON) — I had it move the explanation to a companion doc and keep `schema.json` valid. |
| Task 2 | Explain H3 lat/lon → cell; draft the assignment, join-failure metrics, and validation vs `sr_hex.csv.gz` | Ran against real data, reviewed the 100% validation, and **corrected the join-error threshold design** the AI proposed (see detailed example below). |
| Task 5 | Approach for programmatic suburb centroid (OSM), resilient wind fetch, and anonymisation (H3 res-9 + 6h buckets + singleton quarantine) | Chose external sources, ran the pipeline, reviewed the privacy/utility trade-off, wrote `REPORT.md`. Instructed the AI to fix a broken singleton-detection line (it produced a `KeyError` from incorrect pandas grouping); I had it rewrite the logic into a correct, aligned boolean mask. |
| Testing | Draft pytest unit tests for the pure logic of each task | Ran the suite, confirmed 14 passing. |
| Debugging | Interpret errors during runs (expired AWS tokens, Redshift/Parquet type issues in earlier work, indentation from pasting) | Diagnosed alongside the AI and applied the fixes; caught cases where pasted code lost its indentation and had to be corrected before running. |
| Docs | Draft README and this log | Reviewed, trimmed to the Data Engineering scope, and finalised in my own voice. |

## Example where I corrected/improved the AI's work

**Task 2 join-error threshold.** The AI initially proposed an arbitrary 20%
error threshold applied to the *total* proportion of requests with no H3 index.
When I ran the script against the real `sr.csv.gz` data, 22.55% of requests
legitimately have no coordinates (e.g. call-centre-logged or area-wide issues)
and are correctly assigned index `0`. The AI's threshold would therefore have
falsely failed a completely normal run.

I corrected the design to distinguish two categories:
- **Expected missing** — requests that never had coordinates (index `0` is the
  correct outcome, not a failure); and
- **True join failures** — requests that *do* have coordinates but fail to map
  to a hexagon (a genuine problem).

I moved the error threshold onto **true join failures only**, set at 1%, since a
well-formed coordinate should essentially always produce a valid H3 index. In
practice the true-failure rate is 0%. This makes the threshold evidence-based
and meaningful, rather than an arbitrary number that fails on normal data.
