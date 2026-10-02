# Hands-on L6: Report

**Name:*Preetam Manjunath Hegde*
**Student ID:*801496859*
**Email:*phegde4@charlotte.edu*

---

## Seed and commands

Seed used for `datagen.py`:

My seed (student ID) is: 801496859
            & 
I ran the command:

python3 datagen.py 801496859

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

1) For the 1st step I wrote the command: 

python3 datagen.py 801496859

2) After that I switched on docker using: 

docker compose up 

3) In a GitHub Codespace, start the cluster with the other compose file instead:

docker compose -f docker-compose.codespaces.yml up -d

4) Running main.py using the controls: 

docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output 2>&1 | tee spark_run.log

  I wrote spark_run.log in order to store the explain output result because it was getting difficult to view the explain() result from the terminal. You can find it from line 461 of the spark_run.log file till line 482

5) Working in the PySpark shell first is a good way to try an expression before putting it in the file:

docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077

6) Stop the cluster:

docker compose down

---

## Results

For each task, the first ten rows of your output (from the terminal or the CSV file) and one
or two sentences on what they say about your data.

### Task 1: favorite genre per user

```
user_id	  genre	     play_count
user_1	  Rock	     8
user_10	  Classical	 3
user_100  Rock	     7
user_11	  Hip-Hop	   7
user_12	  Hip-Hop	   4
user_13	  Hip-Hop	   9
user_14	  Hip-Hop	   6
user_15	  Hip-Hop	   2
user_16	  Classical	 3
user_17	  Hip-Hop	   9

This shows each user's most-played genre. Some users have a clear favorite (user_1 with 8 Rock plays, user_13 with 9 Hip-Hop plays), while others barely have one (user_15 only has 2 plays of their top genre).
```

### Task 2: average listening time per song

```
song_id	 title	        avg_duration_sec	play_count
song_49	 Title_song_49	212.79	            14
song_37	 Title_song_37	193.6	              15
song_27	 Title_song_27	191.68	            19
song_40	 Title_song_40	191.59	            27
song_33	 Title_song_33	190.07	            15
song_42	 Title_song_42	187.2	              20
song_34	 Title_song_34	186.57	            21
song_43	 Title_song_43	184.65	            17
song_11	 Title_song_11	183.36	            11
song_23	 Title_song_23	181.1	              10

This shows how long people usually listen to each song, longest first. The top ten all average around 180–213 seconds, so most people listen to nearly the whole song.
```

### Task 3: genre loyalty score, top 10

```
user_id	   genre    	  play_count	   total_plays	     loyalty_score
user_75	   Classical	    6	               6	             1.0
user_89	   Rock	          6	               6	             1.0
user_96	   Jazz	          5	               5	             1.0
user_59	   Pop	          4	               4	             1.0
user_23	   Hip-Hop	      3	               3	             1.0
user_15	   Hip-Hop	      2	               2	             1.0
user_43	   Rock	          1	               1	             1.0
user_61	   Jazz	          14	            15	             0.933
user_79	   Jazz	          10	            11	             0.909
user_50	   Jazz	          8	               9	             0.889

This shows how loyal each user is to their top genre. Seven users have a perfect score of 1.0, meaning they only ever listen to one genre. But some of those only have 1 or 2 plays, so it's not very meaningful. The next three (user_61, user_79, user_50) listen to Jazz almost all the time but occasionally branch out.
```

Why do users with few plays tend to get a score of 1.0? Would you change the definition of
the score to account for that?
The score is just top_genre_plays / total_plays. If someone only has 1 or 2 plays, they've only listened to one genre, so they get 1.0 by default. It doesn't really mean they're loyal — we just haven't seen enough of their listening yet. Yes I would change it. A perfect score from one play doesn't tell us much. I'd add a minimum play count before including a user, or shrink the score toward the middle for low-count users so they don't look artificially loyal.

### Task 4: night owls

```
user_id	    night_plays
user_100	    6
user_25	      5
user_61	      5
user_17	      4
user_21	      4
user_37	      4
user_44	      4
user_47	      4
user_57	      4
user_62	      4

This shows who listens the most between midnight and 5am. The counts are pretty low overall — the top user only has 6 night plays out of 27 days, so nobody is a heavy night listener. Most users here are tied at 4, so there isn't a clear standout.
```
---

## The plan

Paste the `explain()` output of task 1:

AdaptiveSparkPlan isFinalPlan=false
+- Sort [user_id#0 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(user_id#0 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=975]
      +- Project [user_id#0, genre#7, play_count#28L]
         +- Filter (rank#38 = 1)
            +- Window [row_number() windowspecdefinition(user_id#0, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#38], [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST]
               +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Final
                  +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                     +- Exchange hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=968]
                        +- WindowGroupLimit [user_id#0], [play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], row_number(), 1, Partial
                           +- Sort [user_id#0 ASC NULLS FIRST, play_count#28L DESC NULLS LAST, genre#7 ASC NULLS FIRST], false, 0
                              +- HashAggregate(keys=[user_id#0, genre#7], functions=[count(1)])
                                 +- Exchange hashpartitioning(user_id#0, genre#7, 200), ENSURE_REQUIREMENTS, [plan_id=962]
                                    +- HashAggregate(keys=[user_id#0, genre#7], functions=[partial_count(1)])
                                       +- Project [user_id#0, genre#7]
                                          +- BroadcastHashJoin [song_id#1], [song_id#4], Inner, BuildRight, false, false
                                             :- Filter isnotnull(song_id#1)
                                             :  +- FileScan csv [user_id#0,song_id#1] Batched: false, DataFilters: [isnotnull(song_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/listening_logs.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<user_id:string,song_id:string>
                                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=957]
                                                +- Filter isnotnull(song_id#4)
                                                   +- FileScan csv [song_id#4,genre#7] Batched: false, DataFilters: [isnotnull(song_id#4)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/songs_metadata.csv], PartitionFilters: [], PushedFilters: [IsNotNull(song_id)], ReadSchema: struct<song_id:string,genre:string>


Your reading of it: where are the two file scans, which operator is the join and which kind
of join did Spark choose, where are the shuffles (`Exchange`) and why are they needed, and
how does this match the diagram in the SQL / DataFrame tab of the Spark UI?

You can find the scans on the bottom of both .csvs in input(listening_logs and songs_metadata) folder. The join spark choose was broadcast hash join. The excahnge appear 3 times, 2 hashpartitionings (user_id, genre) and (user_id), and 1 rangepartitioning of (user_id). 
---

## Transformations and actions

Which lines of your `main.py` are actions? How many jobs did the program launch according to
the Spark UI, and is that what you expected?

df.show(20, truncate=False)
writer = df.coalesce(1).write.mode("overwrite").option("header", True)
I guess these are the main lines of codes for actions.

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

I wasn't able to get(or should I say view) the explain() output from the terminal. Hence I wrrote the command to generate everything in a separate file which helped me in viewing it .
Also sometimes I had an issue with accessing the ports .
