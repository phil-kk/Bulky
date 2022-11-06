Short Run Merge 100 000 New Items

|             Method |        Job |                Toolchain | LaunchCount | RunStrategy | UnrollFactor | WarmupCount |    Mean | Error |  StdErr |  StdDev |     Min |      Q1 |  Median |      Q3 |     Max |   Op/s |       Gen0 |      Gen1 |      Gen2 | Allocated |
|------------------- |----------- |------------------------- |------------ |------------ |------------- |------------ |--------:|------:|--------:|--------:|--------:|--------:|--------:|--------:|--------:|-------:|-----------:|----------:|----------:|----------:|
| FastBulkOperations | Job-BAQPJF |                  Default |     Default |  Monitoring |            1 |     Default | 1.156 s |    NA | 0.000 s | 0.000 s | 1.156 s | 1.156 s | 1.156 s | 1.156 s | 1.156 s | 0.8649 |  2000.0000 |         - |         - |  23.69 MB |
|         DapperPlus | Job-BAQPJF |                  Default |     Default |  Monitoring |            1 |     Default | 2.008 s |    NA | 0.000 s | 0.000 s | 2.008 s | 2.008 s | 2.008 s | 2.008 s | 2.008 s | 0.4979 | 10000.0000 | 5000.0000 | 1000.0000 |  78.33 MB |
| FastBulkOperations |   ShortRun | InProcessNoEmitToolchain |           1 |     Default |           16 |           1 | 1.007 s |    NA | 0.000 s | 0.000 s | 1.007 s | 1.007 s | 1.007 s | 1.007 s | 1.007 s | 0.9930 |  2000.0000 |         - |         - |  23.69 MB |
|         DapperPlus |   ShortRun | InProcessNoEmitToolchain |           1 |     Default |           16 |           1 | 2.071 s |    NA | 0.000 s | 0.000 s | 2.071 s | 2.071 s | 2.071 s | 2.071 s | 2.071 s | 0.4829 |  9000.0000 | 2000.0000 |         - |  78.32 MB |

Default Run Merge 100 000 New Items

|             Method |        Job |                Toolchain | RunStrategy | UnrollFactor |       Mean |    Error |   StdDev |   StdErr |        Min |         Q1 |     Median |         Q3 |        Max |   Op/s |      Gen0 |      Gen1 | Allocated |
|------------------- |----------- |------------------------- |------------ |------------- |-----------:|---------:|---------:|---------:|-----------:|-----------:|-----------:|-----------:|-----------:|-------:|----------:|----------:|----------:|
| FastBulkOperations | Job-GFDLXF |                  Default |  Monitoring |            1 |   934.3 ms | 56.72 ms | 37.52 ms | 11.86 ms |   913.3 ms |   914.3 ms |   919.4 ms |   924.0 ms | 1,028.0 ms | 1.0704 | 2000.0000 |         - |  23.68 MB |
|         DapperPlus | Job-GFDLXF |                  Default |  Monitoring |            1 | 1,416.4 ms | 51.59 ms | 34.12 ms | 10.79 ms | 1,395.6 ms | 1,402.8 ms | 1,405.2 ms | 1,411.8 ms | 1,512.0 ms | 0.7060 | 9000.0000 | 2000.0000 |  78.32 MB |
| FastBulkOperations | Job-ROGSOD | InProcessNoEmitToolchain |     Default |           16 |   934.9 ms |  9.51 ms |  8.43 ms |  2.25 ms |   924.5 ms |   927.3 ms |   932.9 ms |   942.0 ms |   950.5 ms | 1.0696 | 2000.0000 |         - |  23.69 MB |
|         DapperPlus | Job-ROGSOD | InProcessNoEmitToolchain |     Default |           16 | 1,543.6 ms |  8.85 ms |  8.28 ms |  2.14 ms | 1,529.5 ms | 1,539.5 ms | 1,542.9 ms | 1,547.9 ms | 1,559.1 ms | 0.6478 | 9000.0000 | 2000.0000 |  78.32 MB |


