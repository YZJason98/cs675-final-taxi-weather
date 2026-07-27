# Phase 7 Performance Optimization and Comparison

## 7.1 Baseline Version

The baseline pipeline processed 2,964,624 source taxi records and
produced 2,857,438 cleaned and joined records. It used sort-merge joins,
disabled Adaptive Query Execution, and did not use broadcast joins or
persistence.

The baseline completed in 183 seconds. It used 0.399 vCPU-hours,
1.596 memory GB-hours, and 1.994 storage GB-hours. Its Spark execution
included 61 completed stages and 5,078 successful tasks. Shuffle read
was 720.833 MB, and shuffle write was 716.291 MB.

## 7.2 Optimized Version

The optimized pipeline used Parquet input, column pruning, early
filtering, weather partition pruning, broadcast joins, Adaptive Query
Execution, partition coalescing, and MEMORY_AND_DISK persistence. It
also combined three separate quality count actions into one aggregation
and removed unnecessary show actions.

The optimized pipeline completed in 91 seconds. It used 0.194
vCPU-hours, 0.778 memory GB-hours, and 0.972 storage GB-hours. It
completed with 32 stages and 56 successful tasks. Shuffle read decreased
to 1.044 MB, and shuffle write decreased to 0.651 MB.

Both versions produced 2,857,438 joined records with 100 percent zone
and weather match rates.

## 7.3 Baseline vs Optimized Results

Runtime decreased by approximately 50.3 percent. vCPU and memory
resource usage decreased by slightly more than 51 percent. Completed
stages decreased by approximately 47.5 percent, and successful tasks
decreased by approximately 98.9 percent.

Shuffle read decreased by approximately 99.86 percent, while shuffle
write decreased by approximately 99.91 percent. The optimized physical
plan used BroadcastHashJoin and BroadcastExchange instead of the
baseline SortMergeJoin and large shuffle exchanges.

The optimized version therefore reduced execution time, shuffle
operations, file fragmentation, and relative cloud-compute cost without
changing the output row count or join match rates.

## 7.4 Daily vs Hourly Join

The Daily Join aggregated 744 hourly weather observations into 31 daily
weather records before joining them to taxi trips. The Daily Join
completed in 94 seconds, compared with 91 seconds for the Hourly Join.

Daily and Hourly joins both produced 2,857,438 records and achieved
100 percent weather and taxi-zone match rates. The Daily Join used
0.201 vCPU-hours and 0.804 memory GB-hours, while the Hourly Join used
0.194 vCPU-hours and 0.778 memory GB-hours.

The Daily Join did not provide a performance advantage in this test
because it required an additional weather aggregation step. More
importantly, daily aggregation removes intraday weather variation.
The Hourly Join preserves the timing of rain, snow, temperature, and
precipitation changes, making it more appropriate for analyzing
hourly taxi demand and tipping behavior.

## Conclusion

The optimized Hourly Join is the preferred production design. It
provides the best combination of execution efficiency, low shuffle,
resource reduction, complete join coverage, and analytical precision.
