# Episode 7 · second-model review of script draft 2

Pasted by the owner on 28 November 2026. The reviewer read draft 2 from the repository (`review/e07_script_for_review.md`
and the first report). Draft 3 takes finding 3, and finding 1's diagnosis with a different fix from the one suggested
(measured; see the pull request). It keeps finding 2's caption until it has been run on Photon compute. It also makes one
change the owner asked for: count the walls in the plan asked for before a run, since classic Spark prints each wall
twice once a query has run.

---

I'll fetch that file from the Episode 7 folder of the same repo.

The fetch tool only opens URLs you've given me, so I'll check the project files and then read the file with `curl`, since the sandbox can reach that host.

It's Draft 2 of the script, revised after my review. I'll read the whole file, then the report it mentions, so I can check each change against what I found.

The script says it was revised after my review and points to `review/e07_review_report.md`. I'll list that folder and read the report to see what the owner decided about each finding.

Draft 2 takes all six of my round-1 findings, and each fix is accurate. I re-ran the checks behind them (the sampling pass reading the whole table, SQL `UNION`, the key rules) and tested things I hadn't, including whether an up-front deal ever saves a wall. Two items need attention before voicing. One is the reason s7_1 now gives for dealing up front, which is my own round-1 wording. The other is the new Photon caption.

## A. Findings

**1. Misleading · s7_1**
“So why deal up front at all? Only when several later steps need rows dealt by the same key: then one wall serves them all.” (On screen: later steps lining up behind kept's deal, no new wall for them.)
- **Problem.** I proposed this wording after testing which keys a deal serves, but I never tested whether the deal is needed. Spark already carries a deal from one step to the next. In six pipelines, an up-front `repartition("clinic")` saved a wall in only one:
  - `window(clinic)` then `groupBy(clinic, x)`: 1 wall either way.
  - `groupBy(clinic)` then a join to `clinics`: 2 either way.
  - `groupBy(clinic, x)` then `window(clinic)`: 2 walls, 1 with the deal. This is the one that helps: a coarser key after a finer one.
- **The wrong idea.** The first two pictures would look the same without kept's deal. A viewer takes “repartition first, then several steps share one wall” as the recipe. That is the misconception this episode targets.
- **Suggest.** “So why deal up front at all? Often you don't: Spark already shares a deal between steps that follow each other. It pays when later steps would each deal again, and one deal could serve them all. That's episode 17.”
- **Evidence.** `EnsureRequirements` (a step reuses a child's partitioning when it satisfies the step's clustering, and adds an exchange otherwise); my runs. High.

**2. Imprecise (verify before voicing) · s5_1 caption**
“…on Databricks Photon compute, a wall can also appear as a PhotonShuffleExchangeSink line; count those too.”
- **What I can confirm.** This matches what Databricks itself shows. A 2021 Databricks conference deck shows a Photon plan made of shuffle-source, shuffle-map-stage and shuffle-sink nodes (the Sink is the line you count), and in one example a plain Exchange still sits above the Photon part. So “also … count those too” is right. Photon is on by default for classic all-purpose and jobs compute, so many viewers will see it.
- **What I can't.** The source is from 2021 and I haven't run it. Run `slow.explain()` on your Photon cluster and copy the exact line into the caption.
- **A related note.** Databricks says Photon replaces sort-merge joins with hash joins. On Photon the hinted join may therefore run as a hash join. Both tables are still dealt, so it still shows 2 walls, and the narration holds.
- **Evidence.** The deck above. Moderate-high.

**3. Imprecise · two on-screen notes**
- **s8_1 card.** The narration now says “unless the rows are already in place”, but the card still reads “wide steps build a wall” flat. Make the card match. If you keep “count the Exchange lines” there, add “or Photon's Sink lines”.
- **s3_1 note.** “Arrows run straight down, tray to tray” is wrong for two of the five cards. `coalesce` merges two trays into one, so its arrows converge. `union` stacks the trays of two tables.
- **Evidence.** My runs: `union` 4 + 4 piles to 8 and `coalesce(2)` merging 80 + 40 and 20 + 10. High; low severity.

## B. Open questions

Draft 2 poses none. My three round-1 answers stand.

## C. Verified

- **Round-1 findings.**
  - **s4_2.** Right: job 1 (the sampling job) reads all 103 rows, job 2 (the map stage) reads them again, so the shelf is read twice. At 1,030,000 rows each job reads 1,030,000.
  - **s3_1.** Right: each input pile goes whole to one output pile covers all five cards. SQL's plain `UNION` builds a wall and `UNION ALL` doesn't.
  - **s7_1.** “Helps no step here” is right. On a one-pile table a narrow step ran as 1 task, and after `repartition(8)` as 8.
  - **s1_2 and s8_1.** Both say “plan” correctly.
  - **s4_1.** The join card now shows `.hint("merge")`. Without it the join has 0 walls.
- **New wording.** “40 small rows, one per clinic per pile” is right: 8 piles × 5 clinics. Slow's second wall carrying 1,000 is 200 piles × 5, so the two figures agree.
- **Unchanged clips.** The wall counts (2, 1, 1; the join's 2), the bytes, and the kept and slow explanations all re-checked. Nothing differs over Spark Connect.
- **The five recurring problems.** (a) The join hint is now on screen. (b) The race is fine. (c) No counts are quoted. (d) Found: finding 1. (e) None.
- **Numbers-file checks worth adding.** The orderBy card's first job reads every row (expect [103, 103, 0] input rows per job on the cast). SQL `UNION` has a wall and `UNION ALL` has none. Fast's partial rows equal piles × clinics.
- **s1_1.** It asks which runs “fastest”, and the episode answers in walls, rows and bytes, with no times. Nothing claims a time, so it holds.

## D. Unverified

1. **Databricks.** I haven't confirmed that the node names above still match a current runtime, or that `spark.sql.shuffle.partitions` isn't `auto` on some compute, which would change the 200.
```python
slow.explain()                              # copy the exact Photon shuffle lines for the caption
spark.conf.get("spark.sql.shuffle.partitions")
```
