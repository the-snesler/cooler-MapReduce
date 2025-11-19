# Benchmark Results Summary

| Benchmark | Maps | Reduces | Input (MB) | Avg Runtime (s) | Std Dev | Throughput (MB/s) |
|-----------|------|---------|------------|-----------------|---------|-------------------|
| input_size_large      |    4 |       2 |       9.60 |           11.00 |   0.000 |             0.872 |
| input_size_medium     |    4 |       2 |       0.96 |            2.35 |   0.000 |             0.407 |
| input_size_small      |    4 |       2 |       0.00 |            2.32 |   0.000 |             0.000 |
| input_size_xlarge     |    4 |       2 |      48.00 |          116.53 |   0.000 |             0.412 |
| map_scaling_1         |    1 |       2 |       9.60 |            6.64 |   0.000 |             1.447 |
| map_scaling_2         |    2 |       2 |       9.60 |            8.78 |   0.000 |             1.093 |
| map_scaling_4         |    4 |       2 |       9.60 |           10.99 |   0.000 |             0.874 |
| map_scaling_8         |    8 |       2 |       9.60 |            6.65 |   0.000 |             1.443 |
| reduce_scaling_1      |    4 |       1 |       9.60 |           11.05 |   0.000 |             0.869 |
| reduce_scaling_2      |    4 |       2 |       9.60 |           10.95 |   0.000 |             0.877 |
| reduce_scaling_4      |    4 |       4 |       9.60 |           10.95 |   0.000 |             0.877 |
| reduce_scaling_8      |    4 |       8 |       9.60 |            8.78 |   0.000 |             1.094 |