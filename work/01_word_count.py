"""Word count on Shakespeare's complete works — the classic MapReduce example.

Run with `make analyze-shakespeare-data-use-case-a` (after
`make download-shakespeare-data`) or `python 01_word_count.py`.

The pipeline narrows the data in stages:  lines -> words -> (word, count) -> ranked.
It mirrors MapReduce: explode() is the Map (emit one row per word) and
groupBy(word).count() is the Reduce (sum those rows per key). The peeks show the
table's shape change — one row per line becomes one row per word.

Structure: the transformations live in small named functions; main() runs them,
peeks at a few stages (the only prints), then verifies the result with asserts.
"""
import time

from pyspark.sql import DataFrame, functions as F

from constants import SHAKESPEARE_TXT
from spark_helper import get_spark, print_ui_urls, require_files, show_step

# Split a line on any run of characters that aren't letters or apostrophes.
WORD_PATTERN = r"[^a-z']+"

# Common words to drop for the "interesting words" ranking.
STOPWORDS = frozenset({
    "the", "and", "of", "to", "a", "i", "my", "in", "you", "is", "that",
    "not", "with", "this", "his", "for", "but", "me", "be", "he", "your",
    "it", "as", "thou", "so", "him", "have", "her", "will", "what", "all",
    "thy", "are", "by", "we", "no", "do", "shall", "if", "thee", "on",
    "from", "or", "our", "they", "their", "she", "would", "lord",
    "now", "more", "good", "us", "come", "let", "was", "an", "at", "had",
    "than", "may", "well", "yet", "go", "love", "did", "should", "make",
    "one", "know", "out", "like", "up", "am", "o", "hath", "must", "doth",
})


def word_counts(lines: DataFrame) -> DataFrame:
    """Turn a lines DataFrame (column "value") into (word, count).

    explode() is the Map step; groupBy(word).count() is the Reduce step.
    """
    words = (
        lines
        .select(F.lower("value").alias("line"))                  # case-fold: "The" == "the"
        .select(F.split("line", WORD_PATTERN).alias("tokens"))   # line -> array of words
        .select(F.explode("tokens").alias("word"))               # MAP: array -> one row per word
        .filter(F.length("word") > 0)                            # drop empties from punctuation
    )
    return words.groupBy("word").count()                         # REDUCE: sum 1 per word


def ranked(counts: DataFrame, n: int, exclude: frozenset = frozenset()) -> DataFrame:
    """The top n words by count, optionally dropping a set of stopwords."""
    kept = counts.filter(~F.col("word").isin(list(exclude))) if exclude else counts
    return kept.orderBy(F.col("count").desc()).limit(n)


def main() -> None:
    require_files((SHAKESPEARE_TXT, "make download-shakespeare-data"))
    spark = get_spark("cs675-word-count")
    start = time.time()

    lines = spark.read.text(SHAKESPEARE_TXT)        # one row per line, column "value"
    counts = word_counts(lines)

    # --- Peek: how the data narrows, then the two rankings ---
    show_step("Raw lines", lines)
    show_step("Word counts (unsorted)", counts)
    show_step("Top 20 words", ranked(counts, 20), n=20)
    show_step("Top 20 excluding stopwords", ranked(counts, 20, STOPWORDS), n=20)

    # --- Verify ---
    assert lines.count() > 0
    assert counts.count() > 0
    assert ranked(counts, 1).first()["word"] in STOPWORDS  # the most common word is a stopword

    print(f"\nDone in {time.time() - start:.1f}s.")
    print_ui_urls()
    spark.stop()


if __name__ == "__main__":
    main()
