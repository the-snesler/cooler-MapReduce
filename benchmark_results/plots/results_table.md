# Benchmark Results Summary

| Benchmark | Maps | Reduces | Input (MB) | Avg Runtime (s) | Std Dev | Throughput (MB/s) |
|-----------|------|---------|------------|-----------------|---------|-------------------|
| input_size_large      |    4 |       2 |       9.60 |            6.63 |   0.000 |             1.447 |
| input_size_medium     |    4 |       2 |       0.96 |            2.31 |   0.000 |             0.414 |
| input_size_small      |    4 |       2 |       0.00 |            2.31 |   0.000 |             0.000 |
| map_scaling_1         |    1 |       2 |       9.60 |           11.01 |   0.000 |             0.872 |
| map_scaling_2         |    2 |       2 |       9.60 |            8.81 |   0.000 |             1.089 |
| map_scaling_4         |    4 |       2 |       9.60 |            6.66 |   0.000 |             1.441 |
| map_scaling_8         |    8 |       2 |       9.60 |            8.88 |   0.000 |             1.081 |
| reduce_scaling_1      |    4 |       1 |       9.60 |            4.46 |   0.000 |             2.153 |
| reduce_scaling_2      |    4 |       2 |       9.60 |            4.58 |   0.000 |             2.098 |
| reduce_scaling_4      |    4 |       4 |       9.60 |            4.53 |   0.000 |             2.120 |
| reduce_scaling_8      |    4 |       8 |       9.60 |            4.42 |   0.000 |             2.170 |