"""Hands-on L6: Music Streaming Analysis with the Spark Structured APIs.

Usage (on the Docker cluster from Hands-on L5):
    spark-submit main.py <input directory> <output directory>

Reads listening_logs.csv and songs_metadata.csv from the input directory and writes one
CSV result per task under the output directory (task1/ ... task4/).
"""
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if len(sys.argv) != 3:
    print(__doc__)
    sys.exit(2)

in_dir = sys.argv[1].rstrip("/")
out_dir = sys.argv[2].rstrip("/")

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()


def save(df, name):
    """Print a DataFrame and write it as a single CSV file with a header."""
    if df is None:
        print(f"\n=== {name}: not implemented yet ===")
        return

    print(f"\n=== {name} ===")
    df.show(20, truncate=False)

    writer = df.coalesce(1).write.mode("overwrite").option("header", True)
    writer.csv(f"{out_dir}/{name}")


# ================================================================ Step 0: load the data
# Explicit schemas: no inference pass, and `timestamp` is read as a real timestamp.

LOG_FIELDS = [
    ("user_id", StringType()),
    ("song_id", StringType()),
    ("timestamp", TimestampType()),
    ("duration_sec", IntegerType()),
]
logs_schema = StructType([StructField(name, dtype, True) for name, dtype in LOG_FIELDS])

# songs_metadata.csv schema, written as a DDL string
songs_schema = "song_id STRING, title STRING, artist STRING, genre STRING, mood STRING"

logs = spark.read.csv(f"{in_dir}/listening_logs.csv", header=True, schema=logs_schema)
songs = spark.read.csv(f"{in_dir}/songs_metadata.csv", header=True, schema=songs_schema)

logs.printSchema()
print(f"{logs.count()} log rows, {songs.count()} songs")

# Every play enriched with its song metadata (genre is needed by tasks 1 and 3)
plays = logs.join(songs, on="song_id")


# ================================================================ Task 1
def task1_favorite_genre():
    """Each user's favorite genre (most plays; ties broken alphabetically by genre).

    Columns: user_id, genre, play_count. One row per user, ordered by user_id.
    """
    genre_counts = (
        plays
        .groupBy("user_id", "genre")
        .agg(F.count(F.lit(1)).alias("play_count"))
    )

    by_user = Window.partitionBy("user_id").orderBy(
        F.col("play_count").desc(),
        F.col("genre").asc(),
    )

    top_genre = (
        genre_counts
        .withColumn("rank", F.row_number().over(by_user))
        .where(F.col("rank") == 1)
    )

    return top_genre.select("user_id", "genre", "play_count").orderBy("user_id")


# ================================================================ Task 2
def task2_average_listen_time():
    """Average listening time per song, longest first.

    Columns: song_id, title, avg_duration_sec (2 decimals), play_count.
    """
    per_song = (
        logs
        .groupBy("song_id")
        .agg(
            F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
            F.count(F.lit(1)).alias("play_count"),
        )
    )

    with_titles = per_song.join(songs, on="song_id")

    return (
        with_titles
        .select("song_id", "title", "avg_duration_sec", "play_count")
        .orderBy(F.col("avg_duration_sec").desc())
    )


# ================================================================ Task 3
def task3_genre_loyalty(favorite):
    """Top 10 users by loyalty_score = favorite-genre plays / total plays (3 decimals).

    Columns: user_id, genre, play_count, total_plays, loyalty_score.
    Ordered by loyalty_score desc, total_plays desc, user_id.
    """
    totals = plays.groupBy("user_id").agg(F.count(F.lit(1)).alias("total_plays"))

    loyalty = F.round(F.col("play_count") / F.col("total_plays"), 3)

    scored = (
        favorite
        .join(totals, on="user_id")
        .withColumn("loyalty_score", loyalty)
    )

    columns = ["user_id", "genre", "play_count", "total_plays", "loyalty_score"]
    ordering = [F.col("loyalty_score").desc(), F.col("total_plays").desc(), F.col("user_id")]

    return scored.select(*columns).orderBy(*ordering).limit(10)


# ================================================================ Task 4
def task4_night_owls():
    """Users who listen between 12 AM and 5 AM (timestamp hour 0 to 4).

    Columns: user_id, night_plays. Ordered by night_plays desc, then user_id.
    """
    is_night = F.hour("timestamp").between(0, 4)

    night_counts = (
        logs
        .where(is_night)
        .groupBy("user_id")
        .agg(F.count(F.lit(1)).alias("night_plays"))
    )

    return night_counts.orderBy(F.col("night_plays").desc(), F.col("user_id"))


# ================================================================ Run everything
if __name__ == "__main__":
    favorite = task1_favorite_genre()
    save(favorite, "task1")
    if favorite is not None:
        favorite.explain()  # physical plan of task 1 for the report

    save(task2_average_listen_time(), "task2")
    save(task3_genre_loyalty(favorite) if favorite is not None else None, "task3")
    save(task4_night_owls(), "task4")

    spark.stop()