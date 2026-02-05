# ForeignKey Benchmark Results

## SQLite Benchmarks

| Operation | Rows | ForeignKey (ms) | ForeignKey (rows/s) |
|-----------|------|-----------------|---------------------|
| Single Insert | 100 | 4.05 | 24,721 |
| Bulk Insert | 100 | 0.20 | 492,145 |
| Bulk Insert | 1,000 | 0.94 | 1,068,929 |
| Bulk Insert | 10,000 | 7.80 | 1,282,325 |
| Select All | 11,100 | 7.52 | 1,476,593 |
| Select by ID | 1 | 3.12 | 32,055 |
| Select with Filter | 6,510 | 4.63 | 1,405,041 |