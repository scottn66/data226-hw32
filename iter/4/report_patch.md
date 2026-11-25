# report_patch.md — round 4 (last): rubric item 11 (extension idea, 1 pt), section 11; plus one appended GenAI line

Written Nov 24, 2026, about 8:20 PM PT, against `iter/4/doc.txt` (Drive export 8:13 PM; doc last edited 8:12 PM).
Paste by hand. This patch changes nothing outside section 11.

v1.1 (8:28 PM): the item 11 re-check passed and listed some non-failing wording notes. They are applied here:
"suggested Kaggle data" -> "the Kaggle data used here"; the three snippet problems are attributed to Airflow 3.4
and provider 6.21; "each of the two mutations"; a second citation for the transient tables; and the GenAI line
now names what was captured and who checked the drafts. It also appends one line to section 12, the only
change the loop allows there.

Why this item: Scott chose item 11 for the last round. By the spec's tie order, item 3 would come first: items 3, 7 and
11 are each worth 1 point. Items 3 and 7 are screenshot swaps Scott can make himself. Item 11 needs new text.
The round-4 scorer (`iter/4/score.md`) fails item 11 because it "lists only pipeline extensions (Snowpipe or Stream/Task, a dbt incremental model, backups as a post-hook) and says nothing about changing the assignment to make the learning experience better."

## 11. Extension Idea (rubric item 11)

**Action:** replace. The heading stays. The single bullet "Incremental loads: external stage + Snowpipe or Stream/Task,
dbt incremental model; backups as dbt post-hook" is replaced by the bullets below. That idea is kept as point 4, in the
same terms, and framed as a step of the assignment.

**Locked-text check:** `tools/locked_diff.py iter/3/doc.txt iter/4/doc.txt iter/3/report_patch.md` shows tonight's
edits only in sections 9 (re-bulleting) and 10 (the round-3 patch). Section 11 is the same text as in the 2:27 PM export
(`iter/0/doc.txt`), so it is not locked.

### Replace the bullet under the heading with

> • To make the exercise teach more, I would change four things in the assignment, each prompted by something that went
> wrong or went untested in this project:
>
> 1. Update the starter code for Airflow 3 and for the dataset. The handout's snippets use three things that no longer work
> with Airflow 3.4 and Snowflake provider 6.21, the versions used here (evidence/01_versions.txt). `SnowflakeOperator` no longer exists in the Snowflake provider, and
> `DAG()` rejects `schedule_interval` (evidence/10_airflow3_checks.txt). `{{ ds_nodash }}`, which the 9.4 snippet uses,
> is undefined on a manual run; a manual backup run on Nov 24 failed three times on it (evidence/00_assignment_cloning_excerpt.txt,
> evidence/10_platform_limit_failures.txt). The example snapshot also assumes an `updated_at` column
> (evidence/00_assignment_snapshot_excerpt.txt), and the Kaggle data used here has none. A short "what changed in
> Airflow 3" note, plus a task asking students to choose the snapshot strategy from the data, would teach more than
> debugging the template does.
>
> 2. Make the change step cover every SCD2 case. mutate_source.py only edits the price or title of existing rows. Each of the two
> mutations changed 127 products and added or removed none (evidence/08_source_diffs.txt). Adding new products and deleting some
> would make students handle inserts and hard deletes. dbt snapshots ignore deleted rows unless the snapshot's
> hard_deletes setting is changed (dbt documentation; dbt 1.10 is used here).
>
> 3. Make 9.2 and 9.4 hands-on. 9.2 asks for a clone as of a past year end (evidence/00_assignment_cloning_excerpt.txt),
> which Time Travel can't reach here: dbt's tables are transient with 1-day retention (evidence/92_eoy_logs.txt,
> evidence/92_price_yoy.txt). A
> time-boxed version teaches the same idea inside the window: freeze now, change the data, then compare. For 9.4, add a
> "break it" step: delete the snapshot history, then restore it from the backup. Our restore ran on intact data, so
> nothing visibly came back (evidence/94_restore_logs.txt).
>
> 4. Optional last phase: incremental loading. Replace the full reload with an external stage and Snowpipe or a
> Stream/Task, make dim_products a dbt incremental model, and take backups from a dbt post-hook. Students would then
> have to deal with data that arrives late or in pieces, which the batch version never forces.

## 12. GenAI Disclosure — append only

**Action:** insert one bullet after the existing bullet. Do not change the existing bullet; the rest of the
disclosure is yours to write.

> • This pass (Nov 24, evening): Claude Code (Anthropic) captured read-only evidence, kept in hw32/evidence/: query results,
> Airflow logs, version and import checks, and copies of the source files. It drafted new or replacement text for
> sections 6, 9, 10 and 11, and a separate Claude Code scoring step checked each draft against that evidence before it
> was pasted.

### Comment reply / resolve

- "[Claude] The question asks how you'd extend the exercise to make the learning experience better …": reply
  *Replaced with four assignment changes (Airflow 3 starter code and snapshot strategy; inserts and deletes in the
  mutation; time-boxed 9.2 and a break-it 9.4; incremental loading as an optional phase). Evidence:
  evidence/10_airflow3_checks.txt, evidence/10_platform_limit_failures.txt, evidence/00_assignment_*_excerpt.txt,
  evidence/08_source_diffs.txt, evidence/92_eoy_logs.txt, evidence/94_restore_logs.txt.* Resolve it once item 11 passes.
- "[Claude] Still the template text, word for word …" (GenAI): leave it open. The appended line covers only this pass,
  and the rest of the disclosure is yours to write.
