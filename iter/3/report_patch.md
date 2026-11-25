# report_patch.md — round 3: rubric item 10 (takeaways and cost, 2 pts), section 10

Written Nov 24, 2026, about 8:05 PM PT, against `iter/3/doc.txt` (Drive export 7:50 PM; doc last edited 7:49 PM).
Paste by hand. This patch changes nothing outside section 10.

v1.1 (8:10 PM): the item 10 re-check passed and flagged a few sentences as overstated, none of them failing.
They are reworded here: the spec quote now has an evidence file, and "never", "made the failure visible",
"keeps idle time short" and "leave out the latest runs" are softened to what the evidence shows.

Why this item: the round-3 scorer (`iter/3/score.md`) fails item 10. The number check fails §10: the cost figures 0.156, 0.133 and the 0.03–0.05 per-run estimate are untraced, and the metering screenshot shows only four hourly rows from Nov 20–22. Takeaways 1, 3 and 4 are bare labels with no observation from this project. Item 10 (2 pts) is now the failing item with the most points; items 3, 7 and 11 are 1 point each.

## 10. Takeaways and Cost (rubric item 10)

**Action:** replace. The heading stays. The five numbered one-line takeaways and the old metering screenshot are
replaced by the five takeaways below.

**Locked-text check:** `tools/locked_diff.py iter/2/doc.txt iter/3/doc.txt iter/2/report_patch.md` shows tonight's
changes only in section 9. Section 10's text is the same as in the 2:27 PM export (`iter/0/doc.txt`), so nothing in it is
locked. Only its indentation changed.

### Replace items 1–5 and the screenshot under the heading with

> 1. The snapshot strategy has to fit the data. The assignment's example snapshot uses the timestamp strategy on an
> updated_at column (evidence/00_assignment_snapshot_excerpt.txt), and this dataset has none. Its only time column, scraped_at, holds the scrape time: every row's value falls
> between 00:55 and 01:05 on Dec 14, 2021 (evidence/02_dataset.txt). mutate_source.py never changes it: scraped_at
> changed in 0 rows in both runs (evidence/08_source_diffs.txt). A timestamp snapshot on scraped_at would therefore have
> recorded none of the changes. The check strategy on title, price, availability and brand recorded every one of them:
> 2,551 + 127 + 127 = 2,805 snapshot rows (evidence/08_snapshot_by_run.txt). One of the four columns, availability, is
> "InStock" in all 2,551 rows, so in this data it never triggered a version. The strategy has to come from what the source can
> actually show, not from the template.
>
> 2. Zero-copy clones are copy-on-write, so a clone costs no storage of its own until it changes. Every day HW32 is cloned twice, once to
> the dev database and once to a dated backup, and the restore swapped in a cloned schema. TABLE_STORAGE_METRICS for
> PRODUCTS_SNAPSHOT shows the backup and the dev clone owning 0 active bytes. Prod owns 1,153,024 active bytes, plus
> 1,099,264 bytes kept for Time Travel. The schema left behind by the restore still holds 1,104,896 bytes
> (evidence/10_storage_metrics.txt). A clone starts costing storage only once one side changes, and leftovers such as the
> pre-restore schema keep costing until they are dropped.
>
> 3. Time Travel is short for dbt's tables, so point-in-time work has to be scheduled. dbt creates its tables and
> snapshots on Snowflake as TRANSIENT: the year-end DAG logs "kind=TRANSIENT retention=1 day(s)", and SHOW TABLES gives
> retention_time 1 for the snapshot and the ANALYTICS tables (evidence/92_eoy_logs.txt, evidence/92_price_yoy.txt). A
> Time Travel query back to 1:15 AM on Nov 24 still worked that evening, returning 2678 rows then against 2805 now
> (evidence/10_timetravel_before_run3.txt). Anything older than a day is out of reach. That is why 9.2 clones at the
> year boundary instead of reconstructing the year end later.
>
> 4. Retries fix temporary faults, not configuration errors. The DAGs' default arguments set retries=2 with a 1-minute
> delay, and the pipeline has a failure callback. On Nov 23, extract failed three times, a minute apart, with "The conn_id
> `snowflake_conn` isn't defined". At midnight on Nov 24, extract and backup_database each failed three times with
> "Cannot specify basic authenticator together with private key parameters". Both times the pipeline's callback logged
> "HW32 pipeline FAILED at task extract (try 3)" (evidence/04_retry_callback_extract.txt,
> evidence/01_snowflake_jwt_failure.txt). The retries only delayed each failure by about two minutes. The fix both times
> was configuration: defining the connection, then adding authenticator SNOWFLAKE_JWT. After that fix, the Nov 24 tasks
> succeeded on try 4. The callback logged which task failed and on which try.
>
> 5. Cost comes from idle warehouse time, not from query time. Two X-Small warehouses were used: HW32_WH for the pipeline
> (auto-suspend 60 s) and COMPUTE_WH, Snowsight's default (auto-suspend 300 s). From WAREHOUSE_METERING_HISTORY and
> QUERY_HISTORY since Nov 19 (evidence/10_credits_r3.txt):
>
> ```
> warehouse    credits  queries  execution minutes  auto-suspend
> COMPUTE_WH    1.262    1670          1.5             300 s
> HW32_WH       0.889     742          2.5              60 s
> ```
>
> COMPUTE_WH cost more with less execution time. Snowflake bills a running warehouse per second, with a 60-second minimum
> each time it resumes, until auto-suspend stops it (Snowflake documentation). So most of the bill is idle time after
> queries finish. HW32_WH's 60 s auto-suspend gives it a lower bill per execution minute, but idle time dominates its
> bill too. The metering view can lag by up to about 3 hours (Snowflake documentation), so these totals may not include
> the latest runs. HW32_WH's total rose from 0.791 to 0.889 credits between captures an hour apart
> (evidence/10_credits.txt, evidence/10_credits_r3.txt).

Delete the image "[Screenshot: WAREHOUSE_METERING_HISTORY results]" (four hourly rows from Nov 20 and 22) and its
caption line. If you want an image as well, run the first query in `evidence/10_credits_r3.txt` in Snowsight and
screenshot the result. This is a SELECT, so nothing gets written.

### Comment reply / resolve

Reply to these three threads, quoting their opening words:

- "[Claude] Each takeaway needs 2–3 sentences: what you tried, what you saw, what it means …": *Rewrote all five as
  tried / saw / means, each citing its evidence: 02_dataset.txt, 08_source_diffs.txt, 08_snapshot_by_run.txt,
  10_storage_metrics.txt, 92_eoy_logs.txt, 10_timetravel_before_run3.txt, 04_retry_callback_extract.txt,
  01_snowflake_jwt_failure.txt and 10_credits_r3.txt.*
- "[Claude] These numbers are out of date. WAREHOUSE_METERING_HISTORY …": *Replaced with fresh totals (COMPUTE_WH 1.262,
  HW32_WH 0.889 credits since Nov 19) and dropped the per-run estimate (evidence/10_credits_r3.txt).*
- "[Claude] This shows only four hourly rows …": *Removed the old screenshot; the totals are now a table in takeaway 5.*

Resolve them only once item 10 passes in the next round's scored export.
