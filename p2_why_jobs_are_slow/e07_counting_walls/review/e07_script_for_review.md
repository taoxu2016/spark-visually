# Episode 7: Counting Walls · script for review

Draft 3, revised after the second review, of draft 2 (see review/e07_review_report_2.md): for the owner's review. Not voiced.

8 scenes, 15 clips, 508 words. Every number comes from `e07_numbers.json`.

## S01 · Cold open

| Clip | On screen | Narration |
| --- | --- | --- |
| s1_1 | The code panel types three lines, slow, fast and kept: one count per clinic, written three ways. A wall counter sits in the corner, blank. | Here's one question written three ways: patients per clinic. Every version gives the same answer. Which one runs fastest? Make a guess before they run. |
| s1_2 | Three cards, one per line; each card's wall counter fills in from Spark's own plan: two, one, one. | Here's a clue you can read straight from Spark's plan: how many walls each one builds. Two, one, and one. |

## S02 · Title

| Clip | On screen | Narration |
| --- | --- | --- |
| s2_1 | Series wordmark, Spark, Visually; Part II, Why jobs are slow; the episode title. | Welcome to Spark, Visually. Part Two: why jobs are slow. Today: counting walls. |

## S03 · No wall

| Clip | On screen | Narration |
| --- | --- | --- |
| s3_1 | Five cards: filter, select, withColumn, union, coalesce. For filter, select and withColumn, arrows run straight down, tray to tray; union stacks the two tables' trays, 4 and 4 into 8; coalesce's arrows merge neighbouring trays, 4 into 2. The wall counter stays at 0, and each runs as one stage. The union card notes: in SQL that is UNION ALL; plain UNION also removes duplicates, and that is a wall. | Some steps send each input pile whole to one output pile. A filter, a select, a new column: each pile is worked where it lies. Union just stacks two tables' piles, and coalesce merges neighbours where they stand. No wall, so each runs as a single stage. |

## S04 · A wall each

| Clip | On screen | Narration |
| --- | --- | --- |
| s4_1 | Six cards: groupBy, distinct, join, repartition, a window over each clinic, orderBy. Each lights Episode 5's all-to-all lattice across the benches; the wall counter ticks: 1, 1, 2 for the join, 1, 1, 1. The join card shows its hint, .hint("merge"): copying the small table instead is Episode 8. | Other steps need rows from every pile: a count per clinic, distinct rows, a join, a re-deal, a window over each clinic, a sort. Each one builds a wall. A join that deals out both tables, as this hint asks, builds a wall on each side. |
| s4_2 | orderBy's card: the shelf is read twice, once by the sampling job and once by the map stage, then one wall. Beside it, sortWithinPartitions: no wall. | Sorting needs care. orderBy sorts the whole table: one wall, plus an extra job that reads the table once more, to sample the rows and choose the ranges. To sort each pile where it lies, there's sortWithinPartitions, with no wall at all. |

## S05 · Count from the plan

| Clip | On screen | Narration |
| --- | --- | --- |
| s5_1 | Line 4 types: slow.explain(). Two Exchange lines light: RoundRobinPartitioning(200), then hashpartitioning(clinic, 200). Slow's wall counter: two. A caption: on Databricks Photon compute, a wall can also appear as a PhotonShuffleExchangeSink line; count those too. | You don't have to guess. Ask for the plan before it runs, and count the lines that begin with Exchange. Each one is a wall. Slow has two: a re-deal into 200 piles, then the deal by clinic that the count needs anyway. |
| s5_2 | Fast's and kept's plans: one Exchange each. Kept's count needs no second wall: its rows are already dealt by clinic. A caption: after a run, classic Spark prints the final plan above the starting one, so each wall appears twice; over Spark Connect, explain() shows the starting plan every time. | Fast has one. So does kept: it deals by clinic up front, so the count finds each clinic's rows already in one place. |

## S06 · The race

| Clip | On screen | Narration |
| --- | --- | --- |
| s6_1 | Line 5 glows: all three run, on the cast ten thousand times over. A ledger per version: its walls, and the rows crossing each. | Now run all three on the cast ten thousand times over, just over a million rows. Slow sends every row across its first wall, then 1,000 partial counts across its second. Fast sends only partial counts: 40 small rows, one per clinic per pile. Kept sends every row, once. |
| s6_2 | Bytes written per version, on this table: fast 2.9 kB, kept 164 kB, slow 404 kB. | In bytes, fast wins by far, then kept, then slow. Fast wins because there are only five clinics, so the partial counts are tiny. With many more groups, they shrink less. |

## S07 · Already in place

| Clip | On screen | Narration |
| --- | --- | --- |
| s7_1 | A window over each clinic, then a count per clinic: one wall, Spark's own deal by clinic, which the count reuses; nothing is dealt up front. A small card points ahead to Episode 17: a deal cached once, then a count by clinic on it with no new wall. Beside it, slow's round-robin deal, which no step here needs. | So why deal up front at all? Often you don't need to: when one step follows another on the same key, Spark reuses the first step's deal on its own. Deal once, cache the result, and every pass of a model fit skips a wall: that's episode 17. A re-deal into 200 piles, just to be safe, helps no step here. |
| s7_2 | A wide step with no new wall: rows already dealt the right way. A small table copied to every bench, a hint of Episode 8. | And a wide step doesn't always build a new wall: not when the rows are already dealt the right way, and, as you'll see, not when a small table is copied instead. |

## S08 · Recap

| Clip | On screen | Narration |
| --- | --- | --- |
| s8_1 | Three recap cards: narrow steps work each pile where it lies; wide steps build a wall, unless the rows are already in place; count the plan's Exchange lines, before it runs. | Let's gather it up. Narrow steps work each pile where it lies. Wide steps build a wall, unless the rows are already in place, and the plan's Exchange lines count them. Fewer walls, and less across each: that's a cost you can read from the plan. |
| s8_2 | A join's two walls; the next episode's title: Send the small table. | A join is the commonest wall of all. Must it be one? Next time: send the small table. |
| s8_3 | Series end card with the next episode title. | This has been Spark, Visually. Thanks for watching the cluster with us. |
