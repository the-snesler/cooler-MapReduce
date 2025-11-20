# Benchmark Results Summary

| Benchmark | Maps | Reduces | Input (MB) | Avg Runtime (s) | Std Dev | Throughput (MB/s) |
|-----------|------|---------|------------|-----------------|---------|-------------------|
| combined_2_2          |    2 |       2 |      48.00 |           16.46 |   0.000 |             2.917 |
| combined_4_2          |    4 |       2 |      48.00 |           13.30 |   0.000 |             3.610 |
| combined_8_4          |    8 |       4 |      48.00 |           18.79 |   0.000 |             2.554 |
| input_size_large      |    4 |       2 |       9.60 |            3.42 |   0.000 |             2.808 |
| input_size_medium     |    4 |       2 |       0.96 |            2.53 |   0.000 |             0.379 |
| input_size_small      |    4 |       2 |       0.00 |            2.52 |   0.000 |             0.000 |
| input_size_xlarge     |    4 |       2 |      48.00 |           30.98 |   0.000 |             1.550 |
| map_scaling_1         |    1 |       2 |       9.60 |           12.03 |   0.000 |             0.798 |
| map_scaling_2         |    2 |       2 |       9.60 |            7.05 |   0.000 |             1.362 |
| map_scaling_4         |    4 |       2 |       9.60 |            7.97 |   0.000 |             1.204 |
| map_scaling_8         |    8 |       2 |       9.60 |           13.57 |   0.000 |             0.707 |
| reduce_scaling_1      |    4 |       1 |       9.60 |            7.82 |   0.000 |             1.227 |
| reduce_scaling_2      |    4 |       2 |       9.60 |            4.68 |   0.000 |             2.050 |
| reduce_scaling_4      |    4 |       4 |       9.60 |            7.49 |   0.000 |             1.282 |
| reduce_scaling_8      |    4 |       8 |       9.60 |            6.99 |   0.000 |             1.373 |