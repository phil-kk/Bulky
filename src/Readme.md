|         Method |        Job |                Toolchain | IterationCount | LaunchCount | RunStrategy | UnrollFactor | WarmupCount |       Mean |       Error |   StdDev |   StdErr |        Min |         Q1 |     Median |         Q3 |        Max |   Op/s |       Gen0
 |      Gen1 | Allocated |
 
|--------------- |----------- |------------------------- |--------------- |------------ |------------ |------------- |------------ |-----------:|------------:|---------:|---------:|-----------:|-----------:|-----------:|-----------:|-----------:|-------:|-----------
:|----------:|----------:|

| BulkExtensions | Job-CELPXL |                  Default |        Default |     Default |  Monitoring |            1 |     Default |   740.9 ms |    65.98 ms | 43.64 ms | 13.80 ms |   684.2 ms |   722.1 ms |   731.9 ms |   772.9 ms |   805.6 ms | 1.3497 |  3000.0000
 |         - |  24.44 MB |
|     DapperPlus | Job-CELPXL |                  Default |        Default |     Default |  Monitoring |            1 |     Default | 1,359.8 ms |    81.15 ms | 53.68 ms | 16.97 ms | 1,297.0 ms | 1,304.0 ms | 1,371.0 ms | 1,404.3 ms | 1,437.6 ms | 0.7354 | 14000.0000
 | 2000.0000 | 119.46 MB |
| BulkExtensions |   ShortRun | InProcessNoEmitToolchain |              3 |           1 |     Default |           16 |           3 |   717.9 ms |   643.58 ms | 35.28 ms | 20.37 ms |   694.3 ms |   697.7 ms |   701.0 ms |   729.8 ms |   758.5 ms | 1.3929 |  3000.0000
 |         - |  24.44 MB |
|     DapperPlus |   ShortRun | InProcessNoEmitToolchain |              3 |           1 |     Default |           16 |           3 | 1,334.8 ms | 1,053.08 ms | 57.72 ms | 33.33 ms | 1,297.8 ms | 1,301.6 ms | 1,305.4 ms | 1,353.4 ms | 1,401.4 ms | 0.7491 | 14000.0000
 | 2000.0000 | 119.46 MB |
