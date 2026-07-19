"""Parallel word count on Shakespeare's complete works — native Python, no Spark.

Demonstrates the same MapReduce logic as the PySpark version:
   raw lines  ->  lowercased  ->  tokenized  ->  counted (Map)  ->  merged (Reduce)

Uses multiprocessing to split the file into chunks and count in parallel,
then merges the partial counts (the reduce phase).

Worth pausing on: on a file this size (~5.6 MB, fits easily in memory) this
plain-Python version finishes in a fraction of a second — *faster* than the
Spark version in 01_word_count.py. Spark pays fixed overheads (JVM startup,
query planning, task scheduling, shuffle, serialization) that only pay off when
the data is too big for one machine. Reaching for Spark here is over-engineering:
use the simplest tool that fits the data. Spark earns its keep at cluster scale,
not on a laptop-sized file.

Run:  python 01_word_count_parallel.py
      python 01_word_count_parallel.py --workers 8
"""
import argparse
import re
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count

from constants import SHAKESPEARE_TXT
from spark_helper import require_files

TOKEN_RE = re.compile(r"[a-z']+")

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


def map_chunk(lines: list[str]) -> Counter:
    """Map phase: tokenize and count words in a chunk of lines."""
    counts: Counter = Counter()
    for line in lines:
        tokens = TOKEN_RE.findall(line.lower())
        counts.update(tokens)
    return counts


def reduce_counts(counters: list[Counter]) -> Counter:
    """Reduce phase: merge partial counters into one."""
    total: Counter = Counter()
    for c in counters:
        total += c
    return total


def partition_lines(lines: list[str], n_chunks: int) -> list[list[str]]:
    """Split lines into roughly equal chunks for parallel processing."""
    chunk_size = max(1, len(lines) // n_chunks)
    return [lines[i : i + chunk_size] for i in range(0, len(lines), chunk_size)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Parallel word count (no Spark)")
    parser.add_argument("--workers", type=int, default=cpu_count(),
                        help="Number of parallel workers (default: CPU count)")
    args = parser.parse_args()
    n_workers = args.workers

    require_files((SHAKESPEARE_TXT, "make download-shakespeare-data"))

    start = time.time()

    print(f">>> Reading {SHAKESPEARE_TXT}")
    with open(SHAKESPEARE_TXT, "r", encoding="utf-8") as f:
        lines = f.readlines()
    print(f"    Total lines: {len(lines):,}")

    print(f"\n>>> Partitioning into {n_workers} chunks for parallel Map phase")
    chunks = partition_lines(lines, n_workers)
    print(f"    Chunk sizes: {[len(c) for c in chunks]}")

    print(f"\n>>> Map phase: counting words in parallel ({n_workers} workers)")
    t_map = time.time()
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        partial_counts = list(executor.map(map_chunk, chunks))
    print(f"    Map done in {time.time() - t_map:.3f}s — produced {len(partial_counts)} partial counters")

    print("\n>>> Reduce phase: merging partial counters")
    t_reduce = time.time()
    total_counts = reduce_counts(partial_counts)
    print(f"    Reduce done in {time.time() - t_reduce:.3f}s")
    print(f"    Unique words: {len(total_counts):,}")
    print(f"    Total word tokens: {sum(total_counts.values()):,}")

    print("\n>>> Top 20 words (with stopwords):")
    print(f"    {'Word':<20} Count")
    print(f"    {'----':<20} -----")
    for word, count in total_counts.most_common(20):
        print(f"    {word:<20} {count:,}")

    print("\n>>> Top 20 words (without stopwords):")
    filtered = Counter({w: c for w, c in total_counts.items() if w not in STOPWORDS})
    print(f"    {'Word':<20} Count")
    print(f"    {'----':<20} -----")
    for word, count in filtered.most_common(20):
        print(f"    {word:<20} {count:,}")

    # Sanity check: the single most common word is always a stopword ("the").
    assert total_counts.most_common(1)[0][0] in STOPWORDS

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.2f}s using {n_workers} workers.")
    print("On data this small this beats the Spark word count (01_word_count.py) —")
    print("Spark's distributed overhead only pays off once the data outgrows one machine.")


if __name__ == "__main__":
    main()
