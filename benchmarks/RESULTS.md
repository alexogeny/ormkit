# OrmKit Benchmark Results

## SQLite Benchmarks

| Operation | Rows | OrmKit (ms) | OrmKit (rows/s) |
|-----------|------|-----------------|---------------------|
| Single Insert | 100 | 5.97 | 16,750 |
| Bulk Insert | 100 | 0.25 | 393,915 |
| Bulk Insert | 1,000 | 1.15 | 868,315 |
| Bulk Insert | 10,000 | 7.53 | 1,327,944 |
| Select All | 11,100 | 8.64 | 1,284,901 |
| Select by ID | 1 | 3.58 | 27,929 |
| Select with Filter | 6,510 | 4.22 | 1,542,530 |