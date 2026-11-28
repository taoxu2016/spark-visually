# Episode 7 · second-model review of script draft 1

Pasted by the owner on 28 November 2026. The reviewer worked from `e07_accuracy_review_prompt.md` (draft 1). Draft 2
takes all six findings; see the pull request for what changed where.

---

I ran Episode 7's gallery and race on PySpark 4.3.0 (local[8], adaptive execution on), then the code panel over a
local Spark Connect server. I also ran extra tests on `orderBy`, `union`, `coalesce` and when a later step reuses an
earlier deal. The facts reproduce: your walls, jobs and stages for every card, slow's first wall (103 rows, 7,004
bytes, 4 tasks), fast, and kept. Wall counts, job counts and the explain lines are identical over Connect. The issues
are what the sampling job does, two rules stated wider than they hold, and one hidden setting. I did not run Databricks.

## A. Findings

**1. Misleading · s4_2**
“orderBy sorts the whole table: one wall, plus a small extra job that samples the rows to choose the ranges.” (On
screen: an extra job beside the wall.)
- **Problem.** The extra job is small in what it returns, not in what it reads. Reservoir sampling has to see every
  row. On the cast, job 1 (the sampling job, 4 tasks) read all 103 rows. Job 2, the map stage, read the same 103 again
  and wrote them to the shuffle. At 1,030,000 rows each job read 1,030,000. The table, and everything upstream of the
  sort, is scanned twice.
- **Job count.** With adaptive execution on, 3 jobs is the sampling job plus the usual two. With it off, `orderBy` ran
  2 jobs.
- **Suggest.** “orderBy sorts the whole table: one wall, plus an extra job that reads the table once more, to sample
  the rows and choose the ranges.” On screen, show the shelf read twice.
- **Evidence.** `ShuffleExchangeExec` (the `RangePartitioning` branch builds a `RangePartitioner` over
  `rddForSampling`); `RangePartitioner.sketch` (`reservoirSampleAndCount`, then `collect`); my per-job read counts.
  High.

**2. Misleading · s7_1**
“A re-deal into 200 piles, just to be safe, helps no step at all.”
- **Problem.** True of the count that follows, but said as a rule. A round-robin re-deal can help a later narrow step,
  which is Episode 2's remedy for too few piles. On a one-pile table the same narrow step ran as 1 task; after
  `repartition(8)` it ran with 8 tasks.
- **Suggest.** “A re-deal into 200 piles, just to be safe, helps no step here.”
- **Evidence.** My one-pile test: tasks per stage [1], then [1, 8]. High.

**3. Imprecise · s3_1 (and Open Question 1)**
“Some steps build each output pile from one input pile. … Union just stacks two tables' piles, and coalesce merges
neighbours where they stand.”
- **Problem.** The first sentence's rule doesn't cover `coalesce`, which the same passage lists: coalesce builds each
  output pile from several input piles. What makes a step narrow is that each input pile goes whole to one output pile.
- **Suggest.** “Some steps send each input pile whole to one output pile.”
- **Union in SQL.** The rest of the passage is accurate for DataFrame `union` (4 + 4 piles became 8, no wall) and SQL
  `UNION ALL`. SQL's plain `UNION` removes duplicates and builds a wall (a hash deal on every column). On screen, add
  “UNION ALL; SQL's plain UNION also removes duplicates, and that is a wall”.
- **Coalesce.** “Neighbours” holds when the parent has no preferred locations, as with cloud storage and local files.
  Piles of 80, 40, 20 and 10 rows became [120, 30] with `coalesce(2)` and [80, 40, 30] with `coalesce(3)`. With
  location hints Spark groups piles by location instead. Neighbours are in pile order, and files are ordered by size
  (Episode 2).
- **Evidence.** `CoalescedRDD` (`DefaultPartitionCoalescer`, `noLocality` branch); my runs. High.

**4. Imprecise · s7_1 (Open Question 3)**
“Only when several later steps need the same dealing…”
- **Problem.** Spark's rule is about keys, and the pile count rarely matters. I tested these cases:
  - **Subset keys.** A deal by `clinic` served `groupBy(clinic)`, `groupBy(clinic, x)` and a window by clinic (1 wall
    each). A deal by `clinic, x` did not serve `groupBy(clinic)` (2 walls).
  - **Narrow steps.** Filter, select and a rename kept the deal. A step that recomputed the key (`upper(clinic)`), or
    dropped it, lost it.
  - **Pile counts.** `repartition(4, "clinic")` served the count as well. For a join with 4 piles on one side and 8 on
    the other, Spark re-dealt both explicit deals to 200 and added no wall.
  - **Dealing only the big side of a join.** It still ended with 2 walls, so it saved nothing.
- **Suggest.** “Only when several later steps need rows dealt by the same key: then one wall serves them all.”
- **Evidence.** `EnsureRequirements` (clustering satisfied by a subset of keys; a common pile count for join children);
  my runs. High.

**5. Imprecise (code on screen) · s4_1**
“A join that deals out both tables builds a wall on each side.” (On screen: the join card ticks 2.)
- **Problem.** The two walls come only from `clinics.hint("merge")`. The plain `df.join(clinics, "clinic")` had 0
  walls, 1 BroadcastExchange and 2 jobs. The wording is careful, so keep the hint visible on the card.
- **Suggest.** Show `.hint("merge")` on the card, or a caption “broadcast switched off for this card”.
- **Evidence.** My default-settings run, classic and over Connect. High.

**6. Imprecise · s1_2 and s8_1**
“Here's a clue you can read straight from the code…” and “Wide steps build a wall…”
- **s1_2.** The 2, 1, 1 come from Spark's plan, as the on-screen note says. Read from the code, `kept` looks like two
  walls (a re-deal plus a count) and Spark builds one. Suggest: “…you can read straight from Spark's plan.”
- **s8_1.** The recap states the rule flat out, after s7_2 said a wide step doesn't always build a wall. Suggest: “Wide
  steps build a wall, unless the rows are already in place.” Likewise “a cost you can read from the code” becomes
  “from the plan”.
- **Evidence.** My plans. Medium-high.

## B. Open questions

1. **s3_1.** Union and coalesce are accurate with the qualifications in finding 3.
2. **s4_2.** Accurate on the count and the cause: the sampling job plus adaptive execution's usual two jobs. “Small” is
   the problem (finding 1).
3. **s7_1.** “The same dealing” is slightly off in both directions (finding 4). It is stricter than needed for
   aggregations and windows, where any deal on a subset of the step's keys works, and loose for joins, where both sides
   must be dealt compatibly. “Dealt by the same key” is the precise form.

## C. Verified

- **Gallery.** Filter, select, withColumn, union and coalesce have no wall and run as 1 stage. groupBy, distinct,
  repartition(4) and the window have 1 wall and 2 jobs. The hinted join has 2 walls and 3 jobs; `orderBy` has 1 range
  wall and 3 jobs; `sortWithinPartitions` has no wall.
- **Stage counts.** Your “stages run” leaves out the skipped stage the UI lists on each wide card, as I saw: 1 skipped
  for groupBy and 2 for the join.
- **Race.** The cast's walls reproduce. At scale, slow crosses 1,030,000 rows and then 1,000 partial counts, fast
  crosses 30 (yours is 40 with 8 piles), and kept crosses 1,030,000. All three give the same answer.
- **Bytes.** Slow's first wall is twice kept's in compressed bytes (276 KB against 163 KB) but identical raw (28,840,000
  each). The gap is how well each bin compresses, not the extra wall. The ordering holds either way: raw, slow is
  28.88 MB against kept's 28.84 MB.
- **Kept.** Its count reuses the deal because a deal by `clinic` already satisfies the count's grouping.
- **Spark Connect.** Walls, jobs, `hint("merge")` and the default broadcast join all match. Nothing differs here.
- **The five recurring problems.** (a) Found: the join hint (finding 5). (b) The race compares different dealings,
  which the byte note above covers. (c) None in the narration. (d) Found in findings 2, 3 and 6. (e) None found.

## D. Unverified

1. **Databricks, Photon.** This is from Databricks documentation as I recall it, not run. On Photon compute, `explain()`
   shows `PhotonShuffleExchangeSink` and `PhotonShuffleExchangeSource` rather than `Exchange` lines. The tip “count the
   lines that begin with Exchange” would find none, and a viewer should count the Sinks. This matters for S05's central
   instruction. Check on the target compute:
   ```python
   slow.explain()   # open-source: lines starting with Exchange; Photon: count the ...ShuffleExchangeSink lines
   ```
2. **Shuffle partitions.** `spark.sql.shuffle.partitions` may be `auto` on some Databricks compute, which would change
   the 200 in the plan.
   ```python
   spark.conf.get("spark.sql.shuffle.partitions")
   ```
