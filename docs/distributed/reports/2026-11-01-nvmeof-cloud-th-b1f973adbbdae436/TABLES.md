# Complete operation tables: th-b1f973adbbdae436

All durations below are microseconds. One repetition. Raw operation/outcome populations remain separate; diagnostic twins are never pooled with baseline. Cases encode backend, bytes, tier, method, concurrency and instrumentation. All125cases completed, but campaign monitoring is incomplete.

API: backend call. Scheduling: offered-to-start delay. Response: scheduling plus API according to the recorded sample schema. Throughput uses the observed raw measurement span and is not reciprocal median API latency. Batch32 is one API call and32logical keys; Calls/s and Keys/s are not interchangeable.

Every case has fewer than10,000 successful API calls: p99 is descriptive under the project warning threshold; p99.9 is descriptive below100,000. No run-to-run error bars, confidence intervals or statistical significance are claimed. The CSV retains each raw warning, every clock’s minimum/mean/SD/maximum/percentiles, counts and failures.


## Baseline reads

| Case | N | p50 | p95 | p99 | p99.9 | Mean | SD | Max | Calls/s | Keys/s | Unsuccessful |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0000-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-baseline | 8192 | 228.575 | 454.980 | 480.721 | 533.772 | 276.097 | 111.303 | 1031.553 | 3165.958 | 3165.958 | 0 |
| 0002-tidehunter_nvme_tcp-1024-repeat_read-scalar-c1-baseline | 8192 | 354.957 | 505.311 | 530.541 | 606.052 | 388.324 | 83.000 | 1142.234 | 2303.588 | 2303.588 | 0 |
| 0004-tidehunter_nvme_tcp-1024-first_touch-scalar-c16-baseline | 8192 | 647.813 | 1384.896 | 1866.945 | 2567.309 | 722.475 | 334.643 | 3570.269 | 21135.539 | 21135.539 | 0 |
| 0005-tidehunter_nvme_tcp-1024-repeat_read-scalar-c16-baseline | 8192 | 651.862 | 1481.619 | 2053.040 | 2915.148 | 749.775 | 375.849 | 4881.786 | 20366.543 | 20366.543 | 0 |
| 0006-tidehunter_nvme_tcp-1024-first_touch-batch32-c1-baseline | 2048 | 1586.726 | 2009.104 | 2235.107 | 2697.754 | 1573.882 | 281.556 | 2760.496 | 601.145 | 19236.637 | 0 |
| 0007-tidehunter_nvme_tcp-1024-repeat_read-batch32-c1-baseline | 2048 | 918.418 | 1115.492 | 1267.625 | 1849.287 | 933.361 | 110.644 | 1983.959 | 987.424 | 31597.576 | 0 |
| 0008-tidehunter_nvme_tcp-1024-first_touch-batch32-c16-baseline | 2048 | 11109.598 | 50526.446 | 82939.544 | 108895.486 | 17754.630 | 16401.447 | 138486.658 | 894.695 | 28630.239 | 0 |
| 0009-tidehunter_nvme_tcp-1024-repeat_read-batch32-c16-baseline | 2048 | 26435.290 | 54131.094 | 65721.743 | 84994.191 | 28543.735 | 13401.644 | 92887.986 | 557.999 | 17855.979 | 0 |
| 0010-tidehunter_tcp-1024-first_touch-scalar-c1-baseline | 8192 | 125.102 | 211.684 | 243.274 | 282.636 | 141.583 | 36.212 | 685.594 | 5375.080 | 5375.080 | 0 |
| 0012-tidehunter_tcp-1024-repeat_read-scalar-c1-baseline | 8192 | 203.474 | 241.014 | 265.385 | 320.446 | 198.944 | 30.177 | 443.519 | 4018.117 | 4018.117 | 0 |
| 0014-tidehunter_tcp-1024-first_touch-scalar-c16-baseline | 8192 | 281.485 | 843.226 | 1195.972 | 1594.630 | 357.732 | 230.910 | 2514.687 | 40549.676 | 40549.676 | 0 |
| 0015-tidehunter_tcp-1024-repeat_read-scalar-c16-baseline | 8192 | 159.734 | 273.265 | 383.178 | 46184.142 | 275.071 | 2094.562 | 47488.517 | 46424.870 | 46424.870 | 0 |
| 0016-tidehunter_tcp-1024-first_touch-batch32-c1-baseline | 2048 | 557.840 | 676.744 | 754.385 | 1355.607 | 558.148 | 87.592 | 1748.935 | 1531.501 | 49008.018 | 0 |
| 0017-tidehunter_tcp-1024-repeat_read-batch32-c1-baseline | 2048 | 358.967 | 433.769 | 488.340 | 559.241 | 364.206 | 63.980 | 2598.711 | 2232.119 | 71427.801 | 0 |
| 0018-tidehunter_tcp-1024-first_touch-batch32-c16-baseline | 2048 | 987.750 | 2347.465 | 3181.912 | 3969.848 | 1131.251 | 589.159 | 5928.046 | 13159.200 | 421094.398 | 0 |
| 0019-tidehunter_tcp-1024-repeat_read-batch32-c16-baseline | 2048 | 2356.946 | 4684.942 | 5685.832 | 6940.106 | 2528.511 | 1117.814 | 7381.395 | 6116.939 | 195742.052 | 0 |
| 0020-native-1024-first_touch-scalar-c1-baseline | 8192 | 3.410 | 99.092 | 102.982 | 138.752 | 24.492 | 40.740 | 1126.551 | 18494.815 | 18494.815 | 0 |
| 0021-native-1024-repeat_read-scalar-c1-baseline | 8192 | 2.880 | 3.710 | 4.520 | 19.210 | 2.862 | 1.095 | 26.251 | 32921.257 | 32921.257 | 0 |
| 0022-native-1024-first_touch-scalar-c16-baseline | 8192 | 96.392 | 149.733 | 178.874 | 1147.252 | 91.445 | 74.516 | 1217.753 | 114892.975 | 114892.975 | 0 |
| 0023-native-1024-repeat_read-scalar-c16-baseline | 8192 | 3.770 | 5.700 | 7.640 | 21.761 | 3.934 | 1.623 | 34.421 | 122647.267 | 122647.267 | 0 |
| 0024-native-1024-first_touch-batch32-c1-baseline | 2048 | 334.097 | 892.668 | 1132.592 | 2119.091 | 375.113 | 286.788 | 2268.484 | 2185.998 | 69951.932 | 0 |
| 0025-native-1024-repeat_read-batch32-c1-baseline | 2048 | 58.941 | 71.871 | 81.282 | 132.242 | 60.317 | 7.531 | 245.664 | 7737.379 | 247596.117 | 0 |
| 0026-native-1024-first_touch-batch32-c16-baseline | 2048 | 927.958 | 2057.050 | 2328.265 | 2597.751 | 980.036 | 516.961 | 2626.441 | 15223.107 | 487139.440 | 0 |
| 0027-native-1024-repeat_read-batch32-c16-baseline | 2048 | 103.892 | 125.902 | 146.423 | 364.217 | 107.396 | 22.842 | 460.749 | 85703.917 | 2742525.346 | 0 |
| 0028-tidehunter_efa_native-1024-first_touch-scalar-c1-baseline | 8192 | 65.812 | 105.222 | 129.882 | 317.096 | 74.657 | 21.836 | 623.512 | 8985.501 | 8985.501 | 0 |
| 0030-tidehunter_efa_native-1024-repeat_read-scalar-c1-baseline | 8192 | 66.211 | 76.421 | 83.282 | 92.332 | 66.775 | 6.050 | 194.824 | 9978.000 | 9978.000 | 0 |
| 0032-tidehunter_efa_native-1024-first_touch-scalar-c16-baseline | 8192 | 169.283 | 443.678 | 743.115 | 1257.834 | 205.013 | 131.650 | 1545.700 | 67072.986 | 67072.986 | 0 |
| 0033-tidehunter_efa_native-1024-repeat_read-scalar-c16-baseline | 8192 | 76.342 | 123.762 | 165.803 | 254.935 | 82.830 | 20.890 | 324.106 | 134522.666 | 134522.666 | 0 |
| 0034-tidehunter_efa_native-1024-first_touch-batch32-c1-baseline | 2048 | 463.619 | 505.910 | 532.681 | 980.619 | 463.100 | 41.482 | 1177.923 | 1898.401 | 60748.844 | 0 |
| 0035-tidehunter_efa_native-1024-repeat_read-batch32-c1-baseline | 2048 | 161.593 | 178.564 | 193.644 | 294.496 | 163.838 | 12.155 | 466.789 | 4047.970 | 129535.036 | 0 |
| 0036-tidehunter_efa_native-1024-first_touch-batch32-c16-baseline | 2048 | 964.439 | 2320.715 | 3386.936 | 4816.653 | 1143.288 | 609.155 | 5546.018 | 13067.706 | 418166.592 | 0 |
| 0037-tidehunter_efa_native-1024-repeat_read-batch32-c16-baseline | 2048 | 2062.510 | 4270.772 | 5331.074 | 6499.485 | 2244.855 | 1078.975 | 6851.963 | 6890.303 | 220489.701 | 0 |
| 0038-tidehunter_nvme_tcp-8192-first_touch-scalar-c1-baseline | 8192 | 250.605 | 357.877 | 423.988 | 536.261 | 254.202 | 61.291 | 986.899 | 3338.970 | 3338.970 | 0 |
| 0039-tidehunter_nvme_tcp-8192-repeat_read-scalar-c1-baseline | 8192 | 331.008 | 410.959 | 472.750 | 529.942 | 333.609 | 45.790 | 577.002 | 2593.596 | 2593.596 | 0 |
| 0040-tidehunter_nvme_tcp-8192-first_touch-scalar-c16-baseline | 8192 | 709.834 | 1395.718 | 1870.226 | 2500.509 | 778.312 | 323.447 | 3146.231 | 19618.063 | 19618.063 | 0 |
| 0041-tidehunter_nvme_tcp-8192-repeat_read-scalar-c16-baseline | 8192 | 668.283 | 1429.688 | 1998.729 | 2826.456 | 756.672 | 339.652 | 3376.617 | 20150.487 | 20150.487 | 0 |
| 0042-tidehunter_nvme_tcp-8192-first_touch-batch32-c1-baseline | 2048 | 1905.097 | 2420.217 | 2633.302 | 2855.296 | 1894.351 | 335.663 | 3180.212 | 474.977 | 15199.274 | 0 |
| 0043-tidehunter_nvme_tcp-8192-repeat_read-batch32-c1-baseline | 2048 | 1052.321 | 1371.947 | 1598.351 | 1869.187 | 1080.391 | 158.386 | 2398.607 | 800.147 | 25604.701 | 0 |
| 0044-tidehunter_nvme_tcp-8192-first_touch-batch32-c16-baseline | 2048 | 13634.585 | 51709.756 | 76649.301 | 96045.639 | 19499.716 | 15473.941 | 117423.215 | 810.391 | 25932.504 | 0 |
| 0046-tidehunter_nvme_tcp-8192-repeat_read-batch32-c16-baseline | 2048 | 26835.386 | 54496.579 | 67388.982 | 88872.683 | 29265.370 | 13638.989 | 100483.230 | 542.185 | 17349.936 | 0 |
| 0048-tidehunter_tcp-8192-first_touch-scalar-c1-baseline | 8192 | 146.082 | 245.385 | 289.716 | 433.449 | 166.929 | 46.289 | 708.914 | 4691.465 | 4691.465 | 0 |
| 0049-tidehunter_tcp-8192-repeat_read-scalar-c1-baseline | 8192 | 212.824 | 252.705 | 292.226 | 359.207 | 208.518 | 32.920 | 468.319 | 3981.056 | 3981.056 | 0 |
| 0050-tidehunter_tcp-8192-first_touch-scalar-c16-baseline | 8192 | 339.087 | 893.338 | 1344.587 | 2024.690 | 407.675 | 251.905 | 2497.649 | 36042.764 | 36042.764 | 0 |
| 0051-tidehunter_tcp-8192-repeat_read-scalar-c16-baseline | 8192 | 177.644 | 310.816 | 450.949 | 872.536 | 241.938 | 1417.556 | 46811.920 | 37743.309 | 37743.309 | 0 |
| 0052-tidehunter_tcp-8192-first_touch-batch32-c1-baseline | 2048 | 894.578 | 1169.233 | 1338.537 | 2113.992 | 900.606 | 169.881 | 2594.991 | 943.308 | 30185.846 | 0 |
| 0053-tidehunter_tcp-8192-repeat_read-batch32-c1-baseline | 2048 | 535.030 | 600.612 | 641.813 | 771.105 | 537.674 | 40.103 | 1142.892 | 1404.300 | 44937.613 | 0 |
| 0054-tidehunter_tcp-8192-first_touch-batch32-c16-baseline | 2048 | 3868.725 | 4562.289 | 5393.945 | 9149.838 | 3908.934 | 468.330 | 9652.718 | 3874.284 | 123977.095 | 0 |
| 0056-tidehunter_tcp-8192-repeat_read-batch32-c16-baseline | 2048 | 2525.038 | 5132.169 | 7393.103 | 9603.606 | 2755.259 | 1293.458 | 11063.295 | 5405.565 | 172978.087 | 0 |
| 0058-native-8192-first_touch-scalar-c1-baseline | 8192 | 41.490 | 48.920 | 115.903 | 278.296 | 44.506 | 30.393 | 1684.432 | 12523.523 | 12523.523 | 0 |
| 0059-native-8192-repeat_read-scalar-c1-baseline | 8192 | 4.210 | 5.480 | 6.460 | 18.140 | 4.382 | 1.176 | 35.351 | 27736.342 | 27736.342 | 0 |
| 0060-native-8192-first_touch-scalar-c16-baseline | 8192 | 303.686 | 739.384 | 897.868 | 1031.099 | 351.964 | 188.012 | 1199.803 | 41695.551 | 41695.551 | 0 |
| 0061-native-8192-repeat_read-scalar-c16-baseline | 8192 | 5.930 | 8.310 | 15.910 | 33.960 | 6.245 | 2.357 | 79.422 | 119953.007 | 119953.007 | 0 |
| 0062-native-8192-first_touch-batch32-c1-baseline | 2048 | 1264.595 | 3595.760 | 6745.769 | 7417.743 | 1867.931 | 1210.068 | 8011.434 | 493.818 | 15802.176 | 0 |
| 0063-native-8192-repeat_read-batch32-c1-baseline | 2048 | 112.052 | 133.702 | 148.032 | 269.385 | 115.786 | 11.822 | 388.297 | 3637.151 | 116388.844 | 0 |
| 0064-native-8192-first_touch-batch32-c16-baseline | 2048 | 4876.414 | 6246.920 | 6767.650 | 7806.041 | 4790.891 | 1028.556 | 7886.392 | 3211.019 | 102752.595 | 0 |
| 0065-native-8192-repeat_read-batch32-c16-baseline | 2048 | 166.073 | 215.724 | 274.085 | 1287.584 | 182.044 | 96.790 | 1305.735 | 45205.867 | 1446587.745 | 0 |
| 0066-tidehunter_efa_native-8192-first_touch-scalar-c1-baseline | 8192 | 105.322 | 126.792 | 161.733 | 353.717 | 109.781 | 32.910 | 2290.304 | 6453.397 | 6453.397 | 0 |
| 0067-tidehunter_efa_native-8192-repeat_read-scalar-c1-baseline | 8192 | 72.701 | 81.862 | 88.941 | 107.762 | 73.123 | 5.587 | 213.414 | 9044.841 | 9044.841 | 0 |
| 0068-tidehunter_efa_native-8192-first_touch-scalar-c16-baseline | 8192 | 260.105 | 844.047 | 1420.548 | 2186.153 | 350.193 | 258.765 | 3220.793 | 41665.726 | 41665.726 | 0 |
| 0069-tidehunter_efa_native-8192-repeat_read-scalar-c16-baseline | 8192 | 85.431 | 138.113 | 184.134 | 11185.438 | 110.252 | 447.730 | 12103.036 | 104122.596 | 104122.596 | 0 |
| 0070-tidehunter_efa_native-8192-first_touch-batch32-c1-baseline | 2048 | 1033.380 | 1300.186 | 1374.766 | 2614.760 | 1096.397 | 142.887 | 2783.564 | 770.570 | 24658.235 | 0 |
| 0071-tidehunter_efa_native-8192-repeat_read-batch32-c1-baseline | 2048 | 287.875 | 334.797 | 365.707 | 408.977 | 292.765 | 23.232 | 744.765 | 2150.489 | 68815.632 | 0 |
| 0072-tidehunter_efa_native-8192-first_touch-batch32-c16-baseline | 2048 | 2765.964 | 3479.648 | 4201.491 | 8091.717 | 2833.740 | 448.084 | 8139.368 | 5280.068 | 168962.184 | 0 |
| 0074-tidehunter_efa_native-8192-repeat_read-batch32-c16-baseline | 2048 | 2196.866 | 4332.999 | 5320.410 | 6423.452 | 2355.640 | 1047.955 | 6998.084 | 6317.396 | 202156.668 | 0 |
| 0076-tidehunter_nvme_tcp-64-first_touch-scalar-c1-baseline | 8192 | 284.106 | 455.739 | 480.970 | 563.921 | 311.583 | 101.259 | 1022.240 | 2937.633 | 2937.633 | 0 |
| 0077-tidehunter_nvme_tcp-64-repeat_read-scalar-c1-baseline | 8192 | 286.946 | 326.857 | 367.317 | 435.479 | 279.490 | 38.384 | 855.867 | 3046.855 | 3046.855 | 0 |
| 0078-tidehunter_tcp-64-first_touch-scalar-c1-baseline | 8192 | 124.943 | 208.314 | 238.685 | 273.266 | 141.482 | 35.501 | 574.691 | 5660.485 | 5660.485 | 0 |
| 0079-tidehunter_tcp-64-repeat_read-scalar-c1-baseline | 8192 | 194.054 | 231.334 | 254.905 | 284.806 | 189.924 | 28.646 | 398.737 | 4414.537 | 4414.537 | 0 |
| 0080-native-64-first_touch-scalar-c1-baseline | 8192 | 1.230 | 1.791 | 2.230 | 24.261 | 1.408 | 4.600 | 406.288 | 34043.898 | 34043.898 | 0 |
| 0081-native-64-repeat_read-scalar-c1-baseline | 8192 | 1.170 | 1.620 | 1.960 | 11.990 | 1.242 | 0.728 | 40.361 | 31816.620 | 31816.620 | 0 |
| 0082-tidehunter_efa_native-64-first_touch-scalar-c1-baseline | 8192 | 61.521 | 75.301 | 119.053 | 314.426 | 63.502 | 17.765 | 749.925 | 10008.797 | 10008.797 | 0 |
| 0083-tidehunter_efa_native-64-repeat_read-scalar-c1-baseline | 8192 | 65.832 | 74.761 | 81.212 | 88.272 | 66.429 | 5.262 | 221.285 | 10132.584 | 10132.584 | 0 |
| 0084-tidehunter_nvme_tcp-256-first_touch-scalar-c1-baseline | 8192 | 302.296 | 491.529 | 519.320 | 587.921 | 337.279 | 108.796 | 1028.980 | 2598.587 | 2598.587 | 0 |
| 0085-tidehunter_nvme_tcp-256-repeat_read-scalar-c1-baseline | 8192 | 290.636 | 347.537 | 411.128 | 471.339 | 288.361 | 40.045 | 580.872 | 3002.237 | 3002.237 | 0 |
| 0086-tidehunter_tcp-256-first_touch-scalar-c1-baseline | 8192 | 155.153 | 210.564 | 232.935 | 270.605 | 151.974 | 36.395 | 556.151 | 4989.720 | 4989.720 | 0 |
| 0087-tidehunter_tcp-256-repeat_read-scalar-c1-baseline | 8192 | 196.574 | 230.544 | 252.815 | 289.345 | 192.918 | 26.782 | 411.768 | 4153.049 | 4153.049 | 0 |
| 0088-native-256-first_touch-scalar-c1-baseline | 8192 | 0.980 | 1.411 | 1.840 | 22.311 | 1.133 | 3.663 | 315.876 | 32304.866 | 32304.866 | 0 |
| 0089-native-256-repeat_read-scalar-c1-baseline | 8192 | 0.980 | 1.410 | 1.740 | 6.401 | 1.038 | 0.469 | 23.640 | 31883.341 | 31883.341 | 0 |
| 0090-tidehunter_efa_native-256-first_touch-scalar-c1-baseline | 8192 | 60.821 | 95.571 | 116.652 | 314.466 | 64.806 | 28.869 | 2151.132 | 9997.876 | 9997.876 | 0 |
| 0091-tidehunter_efa_native-256-repeat_read-scalar-c1-baseline | 8192 | 65.872 | 74.022 | 80.501 | 89.622 | 66.119 | 5.213 | 190.874 | 10313.351 | 10313.351 | 0 |
| 0092-tidehunter_nvme_tcp-65536-first_touch-scalar-c1-baseline | 8192 | 427.668 | 675.673 | 747.995 | 850.596 | 449.293 | 141.326 | 1237.884 | 1863.178 | 1863.178 | 0 |
| 0093-tidehunter_nvme_tcp-65536-repeat_read-scalar-c1-baseline | 8192 | 526.430 | 672.053 | 735.594 | 826.176 | 530.144 | 80.539 | 941.088 | 1631.846 | 1631.846 | 0 |
| 0094-tidehunter_tcp-65536-first_touch-scalar-c1-baseline | 8192 | 250.384 | 523.170 | 701.394 | 860.007 | 285.673 | 106.218 | 2700.333 | 2764.042 | 2764.042 | 0 |
| 0095-tidehunter_tcp-65536-repeat_read-scalar-c1-baseline | 8192 | 337.866 | 420.318 | 485.140 | 527.859 | 337.204 | 52.143 | 557.951 | 2446.709 | 2446.709 | 0 |
| 0096-native-65536-first_touch-scalar-c1-baseline | 8192 | 94.392 | 290.265 | 354.017 | 1124.181 | 129.250 | 89.463 | 1525.579 | 5235.060 | 5235.060 | 0 |
| 0097-native-65536-repeat_read-scalar-c1-baseline | 8192 | 15.511 | 17.830 | 28.610 | 34.870 | 15.711 | 2.465 | 60.911 | 14405.104 | 14405.104 | 0 |
| 0098-tidehunter_efa_native-65536-first_touch-scalar-c1-baseline | 8192 | 255.665 | 474.559 | 498.000 | 680.933 | 289.638 | 82.617 | 2400.376 | 2800.643 | 2800.643 | 0 |
| 0099-tidehunter_efa_native-65536-repeat_read-scalar-c1-baseline | 8192 | 176.154 | 189.883 | 197.694 | 226.225 | 176.203 | 18.064 | 1594.941 | 4087.286 | 4087.286 | 0 |


## Read diagnostic twins

| Case | N | p50 | p95 | p99 | p99.9 | Mean | SD | Max | Calls/s | Keys/s | Unsuccessful |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0001-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-diagnostic | 8192 | 241.945 | 486.689 | 514.520 | 559.421 | 288.974 | 119.359 | 1017.811 | 3060.595 | 3060.595 | 0 |
| 0003-tidehunter_nvme_tcp-1024-repeat_read-scalar-c1-diagnostic | 8192 | 321.736 | 497.380 | 531.850 | 874.097 | 370.210 | 99.020 | 3022.158 | 2484.845 | 2484.845 | 0 |
| 0011-tidehunter_tcp-1024-first_touch-scalar-c1-diagnostic | 8192 | 127.482 | 214.534 | 242.075 | 295.046 | 146.725 | 38.978 | 780.445 | 5625.574 | 5625.574 | 0 |
| 0013-tidehunter_tcp-1024-repeat_read-scalar-c1-diagnostic | 8192 | 198.384 | 232.994 | 254.475 | 299.366 | 193.738 | 28.722 | 397.707 | 4100.200 | 4100.200 | 0 |
| 0029-tidehunter_efa_native-1024-first_touch-scalar-c1-diagnostic | 8192 | 66.221 | 133.003 | 182.843 | 429.328 | 82.077 | 34.686 | 768.835 | 8337.262 | 8337.262 | 0 |
| 0031-tidehunter_efa_native-1024-repeat_read-scalar-c1-diagnostic | 8192 | 67.461 | 79.011 | 160.163 | 185.944 | 70.777 | 29.925 | 2306.195 | 9083.402 | 9083.402 | 0 |
| 0045-tidehunter_nvme_tcp-8192-first_touch-batch32-c16-diagnostic | 2048 | 13271.349 | 48371.394 | 78679.715 | 113374.002 | 18867.039 | 15537.492 | 127004.587 | 836.615 | 26771.691 | 0 |
| 0047-tidehunter_nvme_tcp-8192-repeat_read-batch32-c16-diagnostic | 2048 | 27313.518 | 55873.331 | 69559.884 | 88167.768 | 29428.267 | 13690.486 | 95152.498 | 539.174 | 17253.576 | 0 |
| 0055-tidehunter_tcp-8192-first_touch-batch32-c16-diagnostic | 2048 | 3835.745 | 4655.621 | 5325.763 | 9223.310 | 3913.240 | 456.152 | 10238.389 | 3882.459 | 124238.693 | 0 |
| 0057-tidehunter_tcp-8192-repeat_read-batch32-c16-diagnostic | 2048 | 2497.908 | 4856.344 | 7289.771 | 9655.545 | 2697.344 | 1282.341 | 13255.165 | 5533.052 | 177057.651 | 0 |
| 0073-tidehunter_efa_native-8192-first_touch-batch32-c16-diagnostic | 2048 | 2966.468 | 4482.016 | 5592.808 | 10643.705 | 3164.389 | 799.684 | 18975.066 | 4762.052 | 152385.650 | 0 |
| 0075-tidehunter_efa_native-8192-repeat_read-batch32-c16-diagnostic | 2048 | 2154.072 | 4802.803 | 9917.573 | 13319.788 | 2451.393 | 1576.564 | 15449.719 | 6078.412 | 194509.186 | 0 |


## Unchanged native/RF1 write controls

| Case | N | p50 | p95 | p99 | p99.9 | Mean | SD | Max | Calls/s | Keys/s | Unsuccessful |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0100-tidehunter_tcp-1024-write-c16-baseline | 4096 | 987.259 | 1805.454 | 56488.506 | 59186.018 | 2541.442 | 9004.587 | 59207.808 | 6177.410 | 6177.410 | 0 |
| 0101-tidehunter_tcp-1024-write-batch32-c16-baseline | 2048 | 3168.844 | 5682.905 | 100583.184 | 117127.058 | 5529.781 | 15155.738 | 117139.228 | 2829.559 | 90545.885 | 0 |
| 0103-native-1024-write-c16-baseline | 4096 | 656.863 | 1804.855 | 2406.506 | 3234.273 | 808.451 | 499.270 | 4888.614 | 18569.848 | 18569.848 | 0 |
| 0104-native-1024-write-batch32-c16-baseline | 2048 | 2032.399 | 3679.901 | 4644.140 | 5583.028 | 2187.583 | 752.874 | 5692.900 | 6902.457 | 220878.611 | 0 |
| 0105-tidehunter_efa_native-1024-write-c16-baseline | 4096 | 798.056 | 1271.945 | 60464.940 | 65103.071 | 2452.665 | 9756.078 | 65160.942 | 6398.428 | 6398.428 | 0 |
| 0106-tidehunter_efa_native-1024-write-batch32-c16-baseline | 2048 | 2709.802 | 4967.776 | 108033.080 | 137906.969 | 5377.679 | 16519.652 | 137912.929 | 2915.013 | 93280.404 | 0 |
| 0108-tidehunter_tcp-8192-write-c16-baseline | 4096 | 987.680 | 2065.131 | 54692.188 | 60320.850 | 2521.531 | 8786.410 | 60969.482 | 6212.544 | 6212.544 | 0 |
| 0109-tidehunter_tcp-8192-write-batch32-c16-baseline | 2048 | 7931.444 | 108659.417 | 120202.101 | 123306.361 | 19279.218 | 31383.578 | 126144.146 | 818.306 | 26185.789 | 0 |
| 0111-native-8192-write-c16-baseline | 4096 | 783.766 | 1576.281 | 2070.831 | 2812.295 | 897.228 | 321.375 | 3655.372 | 16273.149 | 16273.149 | 0 |
| 0112-native-8192-write-batch32-c16-baseline | 2048 | 9417.423 | 14652.735 | 16079.763 | 20877.097 | 9499.917 | 3007.552 | 24101.979 | 1627.918 | 52093.364 | 0 |
| 0113-tidehunter_efa_native-8192-write-c16-baseline | 4096 | 957.999 | 1694.403 | 55257.997 | 58284.555 | 2490.684 | 8828.381 | 58826.836 | 6284.389 | 6284.389 | 0 |
| 0114-tidehunter_efa_native-8192-write-batch32-c16-baseline | 2048 | 7660.839 | 112206.985 | 124199.929 | 130833.569 | 19282.939 | 31980.823 | 131630.464 | 819.933 | 26237.863 | 0 |
| 0116-tidehunter_tcp-64-write-c16-baseline | 4096 | 647.503 | 1366.857 | 18588.878 | 21931.004 | 1175.686 | 2866.089 | 22249.749 | 13114.809 | 13114.809 | 0 |
| 0117-native-64-write-c16-baseline | 4096 | 429.989 | 1214.904 | 1790.975 | 2659.692 | 522.997 | 354.643 | 3183.303 | 28098.916 | 28098.916 | 0 |
| 0118-tidehunter_efa_native-64-write-c16-baseline | 4096 | 577.791 | 955.569 | 19728.191 | 21879.284 | 1111.293 | 3073.627 | 21898.865 | 13815.119 | 13815.119 | 0 |
| 0119-tidehunter_tcp-256-write-c16-baseline | 4096 | 729.374 | 1352.937 | 17805.713 | 20072.558 | 1232.485 | 2749.578 | 20795.952 | 12542.672 | 12542.672 | 0 |
| 0120-native-256-write-c16-baseline | 4096 | 608.872 | 1905.128 | 2922.517 | 4096.940 | 761.287 | 574.326 | 4920.296 | 19806.827 | 19806.827 | 0 |
| 0121-tidehunter_efa_native-256-write-c16-baseline | 4096 | 650.603 | 1012.360 | 19186.534 | 20392.518 | 1152.844 | 2919.212 | 21042.591 | 13387.776 | 13387.776 | 0 |
| 0122-tidehunter_tcp-65536-write-c16-baseline | 4096 | 2131.351 | 3653.851 | 4559.058 | 5203.830 | 2250.458 | 724.822 | 5338.704 | 6870.879 | 6870.879 | 0 |
| 0123-native-65536-write-c16-baseline | 4096 | 3269.084 | 4758.932 | 6505.047 | 8259.821 | 3287.976 | 953.472 | 9532.986 | 4714.286 | 4714.286 | 0 |
| 0124-tidehunter_efa_native-65536-write-c16-baseline | 4096 | 2161.803 | 3667.222 | 4703.292 | 5902.875 | 2278.071 | 762.492 | 8851.543 | 6804.750 | 6804.750 | 0 |


## Write diagnostic twins

| Case | N | p50 | p95 | p99 | p99.9 | Mean | SD | Max | Calls/s | Keys/s | Unsuccessful |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0102-tidehunter_tcp-1024-write-c16-diagnostic | 4096 | 1009.509 | 1862.026 | 58814.373 | 61106.707 | 2548.698 | 8976.470 | 61125.577 | 6145.058 | 6145.058 | 0 |
| 0107-tidehunter_efa_native-1024-write-c16-diagnostic | 4096 | 773.466 | 1362.827 | 61532.407 | 64554.085 | 2468.318 | 9912.458 | 64592.847 | 6361.130 | 6361.130 | 0 |
| 0110-tidehunter_tcp-8192-write-c16-diagnostic | 4096 | 1015.220 | 2047.110 | 52951.106 | 54272.232 | 2477.348 | 8294.376 | 55083.208 | 6316.339 | 6316.339 | 0 |
| 0115-tidehunter_efa_native-8192-write-c16-diagnostic | 4096 | 921.018 | 1644.213 | 56769.623 | 59315.783 | 2476.958 | 8983.504 | 59353.862 | 6315.167 | 6315.167 | 0 |


## Scheduling timing, all independent cases

| Case | N | p50 | p95 | p99 | p99.9 | Mean | SD | Max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0000-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-baseline | 8192 | 14.630 | 19.321 | 26.380 | 39.301 | 15.649 | 12.566 | 983.291 |
| 0001-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-diagnostic | 8192 | 13.910 | 29.251 | 32.370 | 63.901 | 16.700 | 13.420 | 895.517 |
| 0002-tidehunter_nvme_tcp-1024-repeat_read-scalar-c1-baseline | 8192 | 23.971 | 26.790 | 53.541 | 58.691 | 21.790 | 14.051 | 791.316 |
| 0003-tidehunter_nvme_tcp-1024-repeat_read-scalar-c1-diagnostic | 8192 | 12.821 | 16.870 | 25.301 | 34.660 | 13.875 | 9.163 | 570.391 |
| 0004-tidehunter_nvme_tcp-1024-first_touch-scalar-c16-baseline | 8192 | 13.910 | 19.781 | 30.741 | 75.992 | 14.319 | 4.582 | 133.312 |
| 0005-tidehunter_nvme_tcp-1024-repeat_read-scalar-c16-baseline | 8192 | 14.131 | 20.981 | 32.131 | 49.231 | 14.755 | 4.349 | 111.572 |
| 0006-tidehunter_nvme_tcp-1024-first_touch-batch32-c1-baseline | 2048 | 47.451 | 87.221 | 90.781 | 620.671 | 50.778 | 27.315 | 714.882 |
| 0007-tidehunter_nvme_tcp-1024-repeat_read-batch32-c1-baseline | 2048 | 40.491 | 46.311 | 56.322 | 164.804 | 42.213 | 27.970 | 1123.652 |
| 0008-tidehunter_nvme_tcp-1024-first_touch-batch32-c16-baseline | 2048 | 47.521 | 65.231 | 98.372 | 156.883 | 49.356 | 19.536 | 765.255 |
| 0009-tidehunter_nvme_tcp-1024-repeat_read-batch32-c16-baseline | 2048 | 47.601 | 66.371 | 94.992 | 256.055 | 49.745 | 15.121 | 259.275 |
| 0010-tidehunter_tcp-1024-first_touch-scalar-c1-baseline | 8192 | 17.451 | 19.601 | 29.121 | 36.410 | 17.329 | 10.187 | 902.168 |
| 0011-tidehunter_tcp-1024-first_touch-scalar-c1-diagnostic | 8192 | 12.290 | 20.051 | 26.940 | 37.631 | 13.215 | 7.317 | 613.582 |
| 0012-tidehunter_tcp-1024-repeat_read-scalar-c1-baseline | 8192 | 18.140 | 19.860 | 32.171 | 42.270 | 18.126 | 9.933 | 696.903 |
| 0013-tidehunter_tcp-1024-repeat_read-scalar-c1-diagnostic | 8192 | 17.600 | 20.350 | 31.921 | 45.191 | 17.624 | 6.879 | 576.622 |
| 0014-tidehunter_tcp-1024-first_touch-scalar-c16-baseline | 8192 | 14.131 | 22.731 | 33.440 | 60.401 | 15.140 | 5.017 | 107.172 |
| 0015-tidehunter_tcp-1024-repeat_read-scalar-c16-baseline | 8192 | 15.301 | 30.400 | 44.170 | 85.642 | 17.101 | 7.409 | 165.583 |
| 0016-tidehunter_tcp-1024-first_touch-batch32-c1-baseline | 2048 | 43.311 | 90.592 | 116.912 | 162.534 | 56.696 | 32.955 | 1097.232 |
| 0017-tidehunter_tcp-1024-repeat_read-batch32-c1-baseline | 2048 | 44.871 | 56.031 | 64.351 | 73.141 | 45.501 | 6.546 | 222.905 |
| 0018-tidehunter_tcp-1024-first_touch-batch32-c16-baseline | 2048 | 43.291 | 62.621 | 85.581 | 172.124 | 45.946 | 11.383 | 183.103 |
| 0019-tidehunter_tcp-1024-repeat_read-batch32-c16-baseline | 2048 | 47.791 | 67.241 | 90.342 | 151.663 | 50.027 | 10.947 | 169.763 |
| 0020-native-1024-first_touch-scalar-c1-baseline | 8192 | 13.190 | 14.280 | 24.780 | 29.311 | 12.853 | 2.445 | 89.811 |
| 0021-native-1024-repeat_read-scalar-c1-baseline | 8192 | 12.670 | 13.520 | 22.800 | 29.900 | 12.099 | 2.664 | 122.172 |
| 0022-native-1024-first_touch-scalar-c16-baseline | 8192 | 11.470 | 14.961 | 26.631 | 39.530 | 11.934 | 2.958 | 57.561 |
| 0023-native-1024-repeat_read-scalar-c16-baseline | 8192 | 12.300 | 17.960 | 26.810 | 48.790 | 12.758 | 3.674 | 73.472 |
| 0024-native-1024-first_touch-batch32-c1-baseline | 2048 | 43.751 | 56.962 | 92.762 | 124.333 | 44.894 | 8.847 | 203.514 |
| 0025-native-1024-repeat_read-batch32-c1-baseline | 2048 | 39.791 | 48.631 | 56.621 | 95.522 | 41.248 | 5.454 | 209.715 |
| 0026-native-1024-first_touch-batch32-c16-baseline | 2048 | 41.871 | 58.271 | 79.632 | 109.712 | 44.056 | 8.264 | 116.863 |
| 0027-native-1024-repeat_read-batch32-c16-baseline | 2048 | 46.881 | 60.971 | 71.191 | 257.335 | 48.186 | 16.169 | 276.465 |
| 0028-tidehunter_efa_native-1024-first_touch-scalar-c1-baseline | 8192 | 14.850 | 18.491 | 28.040 | 33.701 | 15.697 | 3.160 | 177.043 |
| 0029-tidehunter_efa_native-1024-first_touch-scalar-c1-diagnostic | 8192 | 14.160 | 18.190 | 26.810 | 32.360 | 15.291 | 3.137 | 159.783 |
| 0030-tidehunter_efa_native-1024-repeat_read-scalar-c1-baseline | 8192 | 13.610 | 16.770 | 27.371 | 33.860 | 14.185 | 2.432 | 108.432 |
| 0031-tidehunter_efa_native-1024-repeat_read-scalar-c1-diagnostic | 8192 | 14.161 | 17.940 | 27.300 | 40.790 | 15.266 | 2.553 | 61.681 |
| 0032-tidehunter_efa_native-1024-first_touch-scalar-c16-baseline | 8192 | 13.470 | 17.400 | 28.790 | 38.851 | 13.548 | 3.197 | 79.262 |
| 0033-tidehunter_efa_native-1024-repeat_read-scalar-c16-baseline | 8192 | 13.340 | 17.141 | 30.950 | 48.351 | 13.473 | 3.719 | 97.362 |
| 0034-tidehunter_efa_native-1024-first_touch-batch32-c1-baseline | 2048 | 35.551 | 46.211 | 53.271 | 67.161 | 37.671 | 15.472 | 666.862 |
| 0035-tidehunter_efa_native-1024-repeat_read-batch32-c1-baseline | 2048 | 43.530 | 50.001 | 59.212 | 97.042 | 44.236 | 5.353 | 174.413 |
| 0036-tidehunter_efa_native-1024-first_touch-batch32-c16-baseline | 2048 | 40.240 | 55.451 | 68.301 | 135.022 | 42.673 | 8.806 | 155.032 |
| 0037-tidehunter_efa_native-1024-repeat_read-batch32-c16-baseline | 2048 | 43.511 | 60.401 | 89.672 | 171.213 | 46.030 | 11.015 | 199.114 |
| 0038-tidehunter_nvme_tcp-8192-first_touch-scalar-c1-baseline | 8192 | 17.121 | 31.770 | 50.711 | 63.851 | 22.388 | 14.723 | 890.037 |
| 0039-tidehunter_nvme_tcp-8192-repeat_read-scalar-c1-baseline | 8192 | 21.460 | 31.351 | 52.711 | 67.691 | 23.048 | 21.340 | 1296.338 |
| 0040-tidehunter_nvme_tcp-8192-first_touch-scalar-c16-baseline | 8192 | 16.440 | 23.021 | 35.041 | 90.642 | 17.154 | 5.135 | 136.463 |
| 0041-tidehunter_nvme_tcp-8192-repeat_read-scalar-c16-baseline | 8192 | 16.330 | 23.760 | 35.431 | 76.402 | 17.018 | 5.200 | 118.833 |
| 0042-tidehunter_nvme_tcp-8192-first_touch-batch32-c1-baseline | 2048 | 138.352 | 261.265 | 288.156 | 678.644 | 150.976 | 48.365 | 807.976 |
| 0043-tidehunter_nvme_tcp-8192-repeat_read-batch32-c1-baseline | 2048 | 113.262 | 139.082 | 156.703 | 643.282 | 119.748 | 28.169 | 866.507 |
| 0044-tidehunter_nvme_tcp-8192-first_touch-batch32-c16-baseline | 2048 | 138.772 | 179.013 | 331.857 | 1063.741 | 147.033 | 77.629 | 1125.852 |
| 0045-tidehunter_nvme_tcp-8192-first_touch-batch32-c16-diagnostic | 2048 | 137.432 | 174.353 | 356.007 | 1107.592 | 146.722 | 87.195 | 1134.302 |
| 0046-tidehunter_nvme_tcp-8192-repeat_read-batch32-c16-baseline | 2048 | 129.042 | 163.463 | 307.516 | 1146.112 | 138.176 | 81.554 | 1184.053 |
| 0047-tidehunter_nvme_tcp-8192-repeat_read-batch32-c16-diagnostic | 2048 | 132.822 | 166.183 | 432.419 | 1143.133 | 140.773 | 85.649 | 1174.494 |
| 0048-tidehunter_tcp-8192-first_touch-scalar-c1-baseline | 8192 | 18.930 | 22.490 | 34.451 | 44.711 | 19.911 | 11.641 | 1035.641 |
| 0049-tidehunter_tcp-8192-repeat_read-scalar-c1-baseline | 8192 | 17.511 | 21.480 | 31.181 | 46.751 | 18.124 | 12.867 | 926.928 |
| 0050-tidehunter_tcp-8192-first_touch-scalar-c16-baseline | 8192 | 16.450 | 25.211 | 36.870 | 69.161 | 17.414 | 5.265 | 122.643 |
| 0051-tidehunter_tcp-8192-repeat_read-scalar-c16-baseline | 8192 | 18.820 | 33.031 | 49.531 | 119.303 | 31.920 | 722.479 | 46773.611 |
| 0052-tidehunter_tcp-8192-first_touch-batch32-c1-baseline | 2048 | 109.442 | 148.833 | 156.823 | 713.464 | 119.068 | 28.862 | 872.746 |
| 0053-tidehunter_tcp-8192-repeat_read-batch32-c1-baseline | 2048 | 118.232 | 134.302 | 136.033 | 152.043 | 120.425 | 18.484 | 753.165 |
| 0054-tidehunter_tcp-8192-first_touch-batch32-c16-baseline | 2048 | 134.723 | 168.493 | 342.036 | 1174.183 | 141.591 | 83.674 | 1183.383 |
| 0055-tidehunter_tcp-8192-first_touch-batch32-c16-diagnostic | 2048 | 125.003 | 161.953 | 279.565 | 988.299 | 134.548 | 73.001 | 1056.551 |
| 0056-tidehunter_tcp-8192-repeat_read-batch32-c16-baseline | 2048 | 126.192 | 171.624 | 281.355 | 1179.953 | 139.163 | 87.177 | 1423.327 |
| 0057-tidehunter_tcp-8192-repeat_read-batch32-c16-diagnostic | 2048 | 123.293 | 162.394 | 263.845 | 1184.143 | 135.496 | 85.187 | 1207.173 |
| 0058-native-8192-first_touch-scalar-c1-baseline | 8192 | 16.561 | 17.640 | 28.530 | 38.020 | 16.877 | 2.097 | 98.662 |
| 0059-native-8192-repeat_read-scalar-c1-baseline | 8192 | 15.280 | 16.181 | 26.940 | 32.091 | 15.586 | 2.050 | 109.632 |
| 0060-native-8192-first_touch-scalar-c16-baseline | 8192 | 15.180 | 20.480 | 29.801 | 38.080 | 15.639 | 3.400 | 60.451 |
| 0061-native-8192-repeat_read-scalar-c16-baseline | 8192 | 15.380 | 21.400 | 31.220 | 67.811 | 16.106 | 4.581 | 147.732 |
| 0062-native-8192-first_touch-batch32-c1-baseline | 2048 | 109.322 | 124.502 | 264.955 | 1186.903 | 117.389 | 55.032 | 1301.956 |
| 0063-native-8192-repeat_read-batch32-c1-baseline | 2048 | 113.812 | 125.942 | 129.662 | 173.503 | 115.145 | 14.358 | 723.582 |
| 0064-native-8192-first_touch-batch32-c16-baseline | 2048 | 130.002 | 184.284 | 297.966 | 850.987 | 141.129 | 67.418 | 856.127 |
| 0065-native-8192-repeat_read-batch32-c16-baseline | 2048 | 116.602 | 157.753 | 170.613 | 968.078 | 132.223 | 73.057 | 1000.419 |
| 0066-tidehunter_efa_native-8192-first_touch-scalar-c1-baseline | 8192 | 18.590 | 20.810 | 31.430 | 43.131 | 18.408 | 12.983 | 1155.792 |
| 0067-tidehunter_efa_native-8192-repeat_read-scalar-c1-baseline | 8192 | 16.900 | 20.330 | 30.190 | 35.060 | 17.428 | 2.497 | 120.902 |
| 0068-tidehunter_efa_native-8192-first_touch-scalar-c16-baseline | 8192 | 15.610 | 18.600 | 31.451 | 39.220 | 15.560 | 3.124 | 53.152 |
| 0069-tidehunter_efa_native-8192-repeat_read-scalar-c16-baseline | 8192 | 16.110 | 21.020 | 37.621 | 80.001 | 20.912 | 216.743 | 11471.363 |
| 0070-tidehunter_efa_native-8192-first_touch-batch32-c1-baseline | 2048 | 146.023 | 168.444 | 171.473 | 511.230 | 142.826 | 31.914 | 1056.290 |
| 0071-tidehunter_efa_native-8192-repeat_read-batch32-c1-baseline | 2048 | 119.863 | 136.403 | 138.332 | 143.123 | 121.892 | 8.021 | 372.657 |
| 0072-tidehunter_efa_native-8192-first_touch-batch32-c16-baseline | 2048 | 127.853 | 181.903 | 314.146 | 1153.552 | 140.676 | 84.991 | 1210.133 |
| 0073-tidehunter_efa_native-8192-first_touch-batch32-c16-diagnostic | 2048 | 118.192 | 157.363 | 317.826 | 1001.389 | 132.293 | 74.722 | 1048.960 |
| 0074-tidehunter_efa_native-8192-repeat_read-batch32-c16-baseline | 2048 | 119.112 | 146.863 | 165.763 | 1181.914 | 130.611 | 83.993 | 1186.985 |
| 0075-tidehunter_efa_native-8192-repeat_read-batch32-c16-diagnostic | 2048 | 122.013 | 152.803 | 191.124 | 1292.425 | 134.339 | 94.049 | 1340.806 |
| 0076-tidehunter_nvme_tcp-64-first_touch-scalar-c1-baseline | 8192 | 11.140 | 14.701 | 20.951 | 28.690 | 11.437 | 11.042 | 782.956 |
| 0077-tidehunter_nvme_tcp-64-repeat_read-scalar-c1-baseline | 8192 | 17.330 | 19.401 | 30.770 | 51.251 | 17.400 | 14.336 | 1036.620 |
| 0078-tidehunter_tcp-64-first_touch-scalar-c1-baseline | 8192 | 14.750 | 17.950 | 27.811 | 34.741 | 15.386 | 8.366 | 716.274 |
| 0079-tidehunter_tcp-64-repeat_read-scalar-c1-baseline | 8192 | 12.440 | 19.011 | 27.371 | 42.431 | 14.597 | 9.583 | 809.596 |
| 0080-native-64-first_touch-scalar-c1-baseline | 8192 | 12.220 | 13.060 | 22.340 | 27.601 | 11.756 | 2.241 | 74.982 |
| 0081-native-64-repeat_read-scalar-c1-baseline | 8192 | 12.480 | 13.531 | 23.200 | 30.071 | 12.763 | 1.857 | 93.112 |
| 0082-tidehunter_efa_native-64-first_touch-scalar-c1-baseline | 8192 | 13.741 | 17.900 | 26.671 | 34.331 | 14.844 | 2.759 | 132.442 |
| 0083-tidehunter_efa_native-64-repeat_read-scalar-c1-baseline | 8192 | 13.470 | 16.530 | 26.041 | 29.331 | 13.943 | 2.031 | 95.122 |
| 0084-tidehunter_nvme_tcp-256-first_touch-scalar-c1-baseline | 8192 | 17.501 | 24.120 | 31.150 | 61.951 | 17.981 | 15.005 | 947.098 |
| 0085-tidehunter_nvme_tcp-256-repeat_read-scalar-c1-baseline | 8192 | 17.140 | 24.010 | 34.861 | 53.861 | 16.942 | 14.082 | 1020.650 |
| 0086-tidehunter_tcp-256-first_touch-scalar-c1-baseline | 8192 | 18.080 | 20.110 | 31.481 | 42.710 | 18.019 | 10.937 | 965.588 |
| 0087-tidehunter_tcp-256-repeat_read-scalar-c1-baseline | 8192 | 17.540 | 19.371 | 28.410 | 37.051 | 17.502 | 10.531 | 942.908 |
| 0088-native-256-first_touch-scalar-c1-baseline | 8192 | 12.661 | 13.470 | 22.690 | 27.390 | 12.896 | 1.542 | 52.001 |
| 0089-native-256-repeat_read-scalar-c1-baseline | 8192 | 12.470 | 13.120 | 22.561 | 26.900 | 12.689 | 1.740 | 97.922 |
| 0090-tidehunter_efa_native-256-first_touch-scalar-c1-baseline | 8192 | 13.280 | 17.011 | 26.760 | 37.121 | 14.148 | 3.150 | 167.273 |
| 0091-tidehunter_efa_native-256-repeat_read-scalar-c1-baseline | 8192 | 13.360 | 14.490 | 26.160 | 29.320 | 13.718 | 2.081 | 104.042 |
| 0092-tidehunter_nvme_tcp-65536-first_touch-scalar-c1-baseline | 8192 | 62.272 | 81.981 | 100.482 | 118.092 | 55.382 | 28.050 | 1013.250 |
| 0093-tidehunter_nvme_tcp-65536-repeat_read-scalar-c1-baseline | 8192 | 38.390 | 76.691 | 100.462 | 118.812 | 51.437 | 24.760 | 776.075 |
| 0094-tidehunter_tcp-65536-first_touch-scalar-c1-baseline | 8192 | 39.361 | 43.911 | 54.141 | 65.871 | 39.789 | 11.612 | 877.828 |
| 0095-tidehunter_tcp-65536-repeat_read-scalar-c1-baseline | 8192 | 38.700 | 43.820 | 53.421 | 68.511 | 39.230 | 17.485 | 1006.999 |
| 0096-native-65536-first_touch-scalar-c1-baseline | 8192 | 32.211 | 75.012 | 80.972 | 108.573 | 37.742 | 24.020 | 1888.586 |
| 0097-native-65536-repeat_read-scalar-c1-baseline | 8192 | 33.631 | 37.131 | 47.241 | 53.321 | 34.222 | 2.945 | 151.683 |
| 0098-tidehunter_efa_native-65536-first_touch-scalar-c1-baseline | 8192 | 37.540 | 41.011 | 51.221 | 68.991 | 36.563 | 12.978 | 823.015 |
| 0099-tidehunter_efa_native-65536-repeat_read-scalar-c1-baseline | 8192 | 37.940 | 43.511 | 52.611 | 75.561 | 37.678 | 9.892 | 653.683 |
| 0100-tidehunter_tcp-1024-write-c16-baseline | 4096 | 15.561 | 22.590 | 32.131 | 48.401 | 16.311 | 4.204 | 85.882 |
| 0101-tidehunter_tcp-1024-write-batch32-c16-baseline | 2048 | 56.511 | 102.962 | 139.223 | 164.983 | 62.090 | 19.404 | 204.274 |
| 0102-tidehunter_tcp-1024-write-c16-diagnostic | 4096 | 16.560 | 24.281 | 39.551 | 72.632 | 17.493 | 5.205 | 98.452 |
| 0103-native-1024-write-c16-baseline | 4096 | 15.070 | 26.621 | 36.811 | 49.661 | 16.136 | 4.990 | 103.562 |
| 0104-native-1024-write-batch32-c16-baseline | 2048 | 58.581 | 79.561 | 95.362 | 112.052 | 59.629 | 11.220 | 115.232 |
| 0105-tidehunter_efa_native-1024-write-c16-baseline | 4096 | 15.440 | 22.921 | 37.331 | 55.331 | 16.189 | 4.946 | 88.392 |
| 0106-tidehunter_efa_native-1024-write-batch32-c16-baseline | 2048 | 53.251 | 70.601 | 93.101 | 132.373 | 54.145 | 10.692 | 138.393 |
| 0107-tidehunter_efa_native-1024-write-c16-diagnostic | 4096 | 15.510 | 22.991 | 33.621 | 44.530 | 16.052 | 4.089 | 56.601 |
| 0108-tidehunter_tcp-8192-write-c16-baseline | 4096 | 19.011 | 26.231 | 39.240 | 75.202 | 19.733 | 5.158 | 98.042 |
| 0109-tidehunter_tcp-8192-write-batch32-c16-baseline | 2048 | 161.203 | 267.715 | 362.067 | 513.310 | 174.540 | 49.842 | 574.961 |
| 0110-tidehunter_tcp-8192-write-c16-diagnostic | 4096 | 17.831 | 31.921 | 41.141 | 66.482 | 18.953 | 5.924 | 86.821 |
| 0111-native-8192-write-c16-baseline | 4096 | 17.501 | 24.150 | 36.611 | 89.751 | 18.215 | 5.273 | 113.493 |
| 0112-native-8192-write-batch32-c16-baseline | 2048 | 195.874 | 262.305 | 300.896 | 340.657 | 194.568 | 42.003 | 358.447 |
| 0113-tidehunter_efa_native-8192-write-c16-baseline | 4096 | 18.580 | 24.630 | 37.741 | 72.382 | 19.064 | 4.627 | 83.322 |
| 0114-tidehunter_efa_native-8192-write-batch32-c16-baseline | 2048 | 141.502 | 194.993 | 270.075 | 381.087 | 151.371 | 31.438 | 775.105 |
| 0115-tidehunter_efa_native-8192-write-c16-diagnostic | 4096 | 18.610 | 26.230 | 38.691 | 50.351 | 19.268 | 4.554 | 72.572 |
| 0116-tidehunter_tcp-64-write-c16-baseline | 4096 | 14.681 | 20.451 | 31.541 | 41.451 | 15.121 | 3.688 | 52.501 |
| 0117-native-64-write-c16-baseline | 4096 | 13.610 | 20.021 | 31.230 | 52.321 | 14.250 | 4.050 | 74.212 |
| 0118-tidehunter_efa_native-64-write-c16-baseline | 4096 | 15.310 | 21.151 | 32.730 | 50.341 | 15.847 | 4.253 | 82.571 |
| 0119-tidehunter_tcp-256-write-c16-baseline | 4096 | 13.320 | 21.850 | 33.401 | 49.821 | 14.204 | 4.446 | 60.301 |
| 0120-native-256-write-c16-baseline | 4096 | 13.280 | 17.791 | 30.810 | 42.131 | 13.806 | 3.351 | 51.491 |
| 0121-tidehunter_efa_native-256-write-c16-baseline | 4096 | 14.540 | 21.200 | 31.410 | 41.831 | 15.066 | 4.020 | 64.881 |
| 0122-tidehunter_tcp-65536-write-c16-baseline | 4096 | 41.770 | 68.751 | 91.911 | 128.602 | 45.577 | 16.515 | 818.106 |
| 0123-native-65536-write-c16-baseline | 4096 | 40.510 | 59.131 | 79.111 | 111.662 | 42.599 | 9.383 | 132.203 |
| 0124-tidehunter_efa_native-65536-write-c16-baseline | 4096 | 41.491 | 53.151 | 64.891 | 100.671 | 42.388 | 6.754 | 134.472 |


## Response timing, all independent cases

| Case | N | p50 | p95 | p99 | p99.9 | Mean | SD | Max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0000-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-baseline | 8192 | 244.286 | 471.830 | 498.651 | 558.112 | 291.746 | 112.991 | 1439.481 |
| 0001-tidehunter_nvme_tcp-1024-first_touch-scalar-c1-diagnostic | 8192 | 256.265 | 510.830 | 542.430 | 593.502 | 305.674 | 123.453 | 1480.548 |
| 0002-tidehunter_nvme_tcp-1024-repeat_read-scalar-c1-baseline | 8192 | 380.328 | 530.681 | 558.482 | 649.633 | 410.114 | 85.339 | 1343.098 |
| 0003-tidehunter_nvme_tcp-1024-repeat_read-scalar-c1-diagnostic | 8192 | 336.777 | 511.390 | 548.431 | 889.867 | 384.085 | 99.437 | 3034.279 |
| 0004-tidehunter_nvme_tcp-1024-first_touch-scalar-c16-baseline | 8192 | 661.632 | 1398.547 | 1882.336 | 2584.459 | 736.793 | 334.963 | 3580.789 |
| 0005-tidehunter_nvme_tcp-1024-repeat_read-scalar-c16-baseline | 8192 | 666.082 | 1496.059 | 2069.741 | 2926.238 | 764.530 | 375.893 | 4897.746 |
| 0006-tidehunter_nvme_tcp-1024-first_touch-batch32-c1-baseline | 2048 | 1637.637 | 2072.694 | 2332.288 | 2751.045 | 1624.661 | 287.263 | 2890.627 |
| 0007-tidehunter_nvme_tcp-1024-repeat_read-batch32-c1-baseline | 2048 | 959.630 | 1157.493 | 1328.307 | 2013.500 | 975.574 | 114.684 | 2097.171 |
| 0008-tidehunter_nvme_tcp-1024-first_touch-batch32-c16-baseline | 2048 | 11153.660 | 50576.197 | 83001.935 | 108940.047 | 17803.987 | 16401.359 | 138535.399 |
| 0009-tidehunter_nvme_tcp-1024-repeat_read-batch32-c16-baseline | 2048 | 26483.541 | 54175.955 | 65769.023 | 85050.912 | 28593.481 | 13401.943 | 92937.977 |
| 0010-tidehunter_tcp-1024-first_touch-scalar-c1-baseline | 8192 | 142.782 | 229.485 | 261.275 | 311.466 | 158.911 | 38.263 | 1102.492 |
| 0011-tidehunter_tcp-1024-first_touch-scalar-c1-diagnostic | 8192 | 141.563 | 227.584 | 259.085 | 323.146 | 159.939 | 39.931 | 858.527 |
| 0012-tidehunter_tcp-1024-repeat_read-scalar-c1-baseline | 8192 | 221.304 | 259.795 | 284.895 | 345.637 | 217.070 | 32.086 | 893.827 |
| 0013-tidehunter_tcp-1024-repeat_read-scalar-c1-diagnostic | 8192 | 215.614 | 251.395 | 272.375 | 317.746 | 211.362 | 29.739 | 783.446 |
| 0014-tidehunter_tcp-1024-first_touch-scalar-c16-baseline | 8192 | 296.325 | 858.776 | 1212.022 | 1617.131 | 372.872 | 231.447 | 2527.697 |
| 0015-tidehunter_tcp-1024-repeat_read-scalar-c16-baseline | 8192 | 176.754 | 293.066 | 404.948 | 46195.862 | 292.172 | 2094.503 | 47507.358 |
| 0016-tidehunter_tcp-1024-first_touch-batch32-c1-baseline | 2048 | 606.762 | 756.195 | 845.047 | 1532.060 | 614.844 | 104.754 | 1975.789 |
| 0017-tidehunter_tcp-1024-repeat_read-batch32-c1-baseline | 2048 | 403.858 | 479.399 | 537.430 | 601.852 | 409.707 | 65.330 | 2647.143 |
| 0018-tidehunter_tcp-1024-first_touch-batch32-c16-baseline | 2048 | 1031.820 | 2401.617 | 3245.443 | 4007.318 | 1177.197 | 590.367 | 5975.287 |
| 0019-tidehunter_tcp-1024-repeat_read-batch32-c16-baseline | 2048 | 2412.687 | 4726.903 | 5730.823 | 7001.297 | 2578.538 | 1118.653 | 7419.505 |
| 0020-native-1024-first_touch-scalar-c1-baseline | 8192 | 16.820 | 112.423 | 116.822 | 155.163 | 37.345 | 40.970 | 1136.232 |
| 0021-native-1024-repeat_read-scalar-c1-baseline | 8192 | 15.540 | 16.950 | 27.150 | 34.761 | 14.961 | 3.157 | 132.712 |
| 0022-native-1024-first_touch-scalar-c16-baseline | 8192 | 107.772 | 161.363 | 190.063 | 1160.243 | 103.379 | 74.418 | 1226.804 |
| 0023-native-1024-repeat_read-scalar-c16-baseline | 8192 | 16.160 | 23.100 | 32.860 | 70.561 | 16.691 | 4.667 | 106.373 |
| 0024-native-1024-first_touch-batch32-c1-baseline | 2048 | 379.397 | 942.088 | 1227.554 | 2164.922 | 420.006 | 290.004 | 2312.635 |
| 0025-native-1024-repeat_read-batch32-c1-baseline | 2048 | 99.492 | 114.652 | 126.963 | 227.764 | 101.565 | 11.476 | 455.379 |
| 0026-native-1024-first_touch-batch32-c16-baseline | 2048 | 970.379 | 2100.691 | 2370.256 | 2635.662 | 1024.092 | 516.268 | 2663.972 |
| 0027-native-1024-repeat_read-batch32-c16-baseline | 2048 | 150.383 | 174.903 | 199.524 | 579.841 | 155.582 | 36.953 | 602.032 |
| 0028-tidehunter_efa_native-1024-first_touch-scalar-c1-baseline | 8192 | 81.632 | 121.512 | 146.452 | 333.076 | 90.354 | 22.501 | 637.122 |
| 0029-tidehunter_efa_native-1024-first_touch-scalar-c1-diagnostic | 8192 | 81.751 | 148.923 | 202.104 | 447.739 | 97.367 | 35.438 | 928.618 |
| 0030-tidehunter_efa_native-1024-repeat_read-scalar-c1-baseline | 8192 | 80.222 | 91.712 | 101.582 | 117.393 | 80.960 | 6.942 | 297.656 |
| 0031-tidehunter_efa_native-1024-repeat_read-scalar-c1-diagnostic | 8192 | 82.441 | 97.831 | 177.804 | 205.395 | 86.043 | 30.410 | 2331.645 |
| 0032-tidehunter_efa_native-1024-first_touch-scalar-c16-baseline | 8192 | 182.783 | 458.319 | 758.214 | 1271.565 | 218.561 | 132.051 | 1559.210 |
| 0033-tidehunter_efa_native-1024-repeat_read-scalar-c16-baseline | 8192 | 89.742 | 138.253 | 181.504 | 281.926 | 96.302 | 21.890 | 412.908 |
| 0034-tidehunter_efa_native-1024-first_touch-batch32-c1-baseline | 2048 | 500.329 | 545.571 | 573.621 | 1095.121 | 500.771 | 46.825 | 1462.199 |
| 0035-tidehunter_efa_native-1024-repeat_read-batch32-c1-baseline | 2048 | 205.764 | 222.794 | 237.424 | 368.017 | 208.074 | 15.103 | 641.202 |
| 0036-tidehunter_efa_native-1024-first_touch-batch32-c16-baseline | 2048 | 1006.979 | 2360.366 | 3422.546 | 4857.144 | 1185.962 | 610.073 | 5589.398 |
| 0037-tidehunter_efa_native-1024-repeat_read-batch32-c16-baseline | 2048 | 2108.770 | 4310.723 | 5404.474 | 6545.486 | 2290.885 | 1079.390 | 6893.203 |
| 0038-tidehunter_nvme_tcp-8192-first_touch-scalar-c1-baseline | 8192 | 270.135 | 386.227 | 449.859 | 608.452 | 276.591 | 66.619 | 1266.935 |
| 0039-tidehunter_nvme_tcp-8192-repeat_read-scalar-c1-baseline | 8192 | 351.447 | 438.939 | 499.561 | 571.312 | 356.657 | 52.762 | 1699.917 |
| 0040-tidehunter_nvme_tcp-8192-first_touch-scalar-c16-baseline | 8192 | 726.835 | 1413.278 | 1886.286 | 2515.689 | 795.466 | 323.575 | 3161.941 |
| 0041-tidehunter_nvme_tcp-8192-repeat_read-scalar-c16-baseline | 8192 | 684.394 | 1450.029 | 2016.510 | 2845.126 | 773.690 | 339.896 | 3392.547 |
| 0042-tidehunter_nvme_tcp-8192-first_touch-batch32-c1-baseline | 2048 | 2050.250 | 2600.001 | 2838.615 | 3009.339 | 2045.327 | 346.566 | 3858.856 |
| 0043-tidehunter_nvme_tcp-8192-repeat_read-batch32-c1-baseline | 2048 | 1174.563 | 1495.669 | 1740.674 | 2320.425 | 1200.139 | 165.710 | 3041.889 |
| 0044-tidehunter_nvme_tcp-8192-first_touch-batch32-c16-baseline | 2048 | 13773.938 | 51845.919 | 76804.414 | 96206.652 | 19646.749 | 15475.130 | 117563.138 |
| 0045-tidehunter_nvme_tcp-8192-first_touch-batch32-c16-diagnostic | 2048 | 13429.882 | 48486.186 | 78828.048 | 113510.284 | 19013.762 | 15541.116 | 127143.050 |
| 0046-tidehunter_nvme_tcp-8192-repeat_read-batch32-c16-baseline | 2048 | 26966.789 | 54636.122 | 67504.404 | 89001.236 | 29403.546 | 13640.932 | 100594.672 |
| 0047-tidehunter_nvme_tcp-8192-repeat_read-batch32-c16-diagnostic | 2048 | 27425.370 | 55983.163 | 69680.007 | 88319.511 | 29569.040 | 13694.301 | 95306.211 |
| 0048-tidehunter_tcp-8192-first_touch-scalar-c1-baseline | 8192 | 167.133 | 265.695 | 309.126 | 504.580 | 186.840 | 48.269 | 1244.975 |
| 0049-tidehunter_tcp-8192-repeat_read-scalar-c1-baseline | 8192 | 231.684 | 271.295 | 310.506 | 384.198 | 226.642 | 35.718 | 1174.093 |
| 0050-tidehunter_tcp-8192-first_touch-scalar-c16-baseline | 8192 | 356.236 | 915.218 | 1362.797 | 2052.540 | 425.089 | 252.458 | 2513.949 |
| 0051-tidehunter_tcp-8192-repeat_read-scalar-c16-baseline | 8192 | 198.534 | 334.117 | 477.799 | 45118.859 | 273.858 | 1591.953 | 47091.817 |
| 0052-tidehunter_tcp-8192-first_touch-batch32-c1-baseline | 2048 | 1012.070 | 1294.185 | 1474.489 | 2232.164 | 1019.675 | 175.839 | 3319.475 |
| 0053-tidehunter_tcp-8192-repeat_read-batch32-c1-baseline | 2048 | 655.183 | 723.225 | 766.365 | 890.517 | 658.099 | 48.036 | 1742.164 |
| 0054-tidehunter_tcp-8192-first_touch-batch32-c16-baseline | 2048 | 3997.768 | 4708.812 | 5568.388 | 10203.189 | 4050.525 | 515.020 | 10638.477 |
| 0055-tidehunter_tcp-8192-first_touch-batch32-c16-diagnostic | 2048 | 3966.137 | 4777.143 | 5642.320 | 10208.098 | 4047.788 | 497.959 | 11123.627 |
| 0056-tidehunter_tcp-8192-repeat_read-batch32-c16-baseline | 2048 | 2664.671 | 5280.063 | 7846.443 | 9728.798 | 2894.422 | 1314.428 | 11178.627 |
| 0057-tidehunter_tcp-8192-repeat_read-batch32-c16-diagnostic | 2048 | 2628.191 | 5020.077 | 7749.459 | 9777.648 | 2832.840 | 1293.638 | 13424.288 |
| 0058-native-8192-first_touch-scalar-c1-baseline | 8192 | 58.131 | 67.431 | 137.112 | 295.666 | 61.383 | 31.174 | 1720.553 |
| 0059-native-8192-repeat_read-scalar-c1-baseline | 8192 | 19.521 | 21.581 | 32.130 | 37.841 | 19.969 | 2.602 | 144.983 |
| 0060-native-8192-first_touch-scalar-c16-baseline | 8192 | 319.746 | 755.594 | 915.538 | 1044.269 | 367.604 | 188.170 | 1219.803 |
| 0061-native-8192-repeat_read-scalar-c16-baseline | 8192 | 21.460 | 28.971 | 39.361 | 113.892 | 22.351 | 6.182 | 181.853 |
| 0062-native-8192-first_touch-batch32-c1-baseline | 2048 | 1376.047 | 3721.311 | 6918.773 | 7827.200 | 1985.319 | 1223.684 | 8119.846 |
| 0063-native-8192-repeat_read-batch32-c1-baseline | 2048 | 226.994 | 248.834 | 264.135 | 442.888 | 230.931 | 22.962 | 1111.879 |
| 0064-native-8192-first_touch-batch32-c16-baseline | 2048 | 5000.457 | 6418.974 | 6905.283 | 8001.995 | 4932.020 | 1036.830 | 8131.727 |
| 0065-native-8192-repeat_read-batch32-c16-baseline | 2048 | 281.765 | 361.117 | 429.668 | 2235.872 | 314.268 | 169.213 | 2278.273 |
| 0066-tidehunter_efa_native-8192-first_touch-scalar-c1-baseline | 8192 | 123.733 | 145.543 | 179.234 | 381.097 | 128.189 | 35.726 | 2322.204 |
| 0067-tidehunter_efa_native-8192-repeat_read-scalar-c1-baseline | 8192 | 89.931 | 101.302 | 107.712 | 130.382 | 90.551 | 6.507 | 334.316 |
| 0068-tidehunter_efa_native-8192-first_touch-scalar-c16-baseline | 8192 | 275.485 | 859.897 | 1438.528 | 2201.334 | 365.753 | 259.134 | 3239.113 |
| 0069-tidehunter_efa_native-8192-repeat_read-scalar-c16-baseline | 8192 | 101.952 | 155.914 | 207.374 | 11511.194 | 131.164 | 497.713 | 12120.226 |
| 0070-tidehunter_efa_native-8192-first_touch-batch32-c1-baseline | 2048 | 1180.603 | 1445.048 | 1596.361 | 2741.533 | 1239.224 | 147.126 | 2923.526 |
| 0071-tidehunter_efa_native-8192-repeat_read-batch32-c1-baseline | 2048 | 410.338 | 455.849 | 486.629 | 531.780 | 414.657 | 26.541 | 1117.422 |
| 0072-tidehunter_efa_native-8192-first_touch-batch32-c16-baseline | 2048 | 2896.456 | 3616.371 | 4317.454 | 9023.965 | 2974.417 | 502.544 | 9161.338 |
| 0073-tidehunter_efa_native-8192-first_touch-batch32-c16-diagnostic | 2048 | 3088.209 | 4600.898 | 5871.434 | 11547.543 | 3296.682 | 846.530 | 19947.325 |
| 0074-tidehunter_efa_native-8192-repeat_read-batch32-c16-baseline | 2048 | 2318.398 | 4501.303 | 5465.003 | 7000.394 | 2486.251 | 1058.461 | 7116.047 |
| 0075-tidehunter_efa_native-8192-repeat_read-batch32-c16-diagnostic | 2048 | 2280.744 | 4918.755 | 10538.955 | 14028.971 | 2585.732 | 1614.456 | 15591.511 |
| 0076-tidehunter_nvme_tcp-64-first_touch-scalar-c1-baseline | 8192 | 295.126 | 467.549 | 492.120 | 578.552 | 323.020 | 102.668 | 1264.125 |
| 0077-tidehunter_nvme_tcp-64-repeat_read-scalar-c1-baseline | 8192 | 303.646 | 345.127 | 386.937 | 470.609 | 296.890 | 41.207 | 1271.325 |
| 0078-tidehunter_tcp-64-first_touch-scalar-c1-baseline | 8192 | 140.633 | 224.314 | 255.415 | 295.786 | 156.868 | 36.949 | 885.677 |
| 0079-tidehunter_tcp-64-repeat_read-scalar-c1-baseline | 8192 | 207.274 | 247.435 | 271.185 | 304.146 | 204.521 | 31.093 | 1011.280 |
| 0080-native-64-first_touch-scalar-c1-baseline | 8192 | 13.421 | 14.530 | 25.010 | 36.741 | 13.165 | 5.702 | 481.270 |
| 0081-native-64-repeat_read-scalar-c1-baseline | 8192 | 13.691 | 14.840 | 24.951 | 34.441 | 14.005 | 2.170 | 114.952 |
| 0082-tidehunter_efa_native-64-first_touch-scalar-c1-baseline | 8192 | 76.041 | 91.881 | 137.152 | 329.556 | 78.346 | 18.461 | 773.956 |
| 0083-tidehunter_efa_native-64-repeat_read-scalar-c1-baseline | 8192 | 79.692 | 89.872 | 96.012 | 105.142 | 80.371 | 5.997 | 316.407 |
| 0084-tidehunter_nvme_tcp-256-first_touch-scalar-c1-baseline | 8192 | 321.477 | 512.590 | 539.831 | 631.333 | 355.260 | 110.746 | 1317.976 |
| 0085-tidehunter_nvme_tcp-256-repeat_read-scalar-c1-baseline | 8192 | 305.436 | 369.218 | 431.409 | 503.070 | 305.303 | 43.416 | 1316.065 |
| 0086-tidehunter_tcp-256-first_touch-scalar-c1-baseline | 8192 | 173.224 | 228.235 | 252.725 | 302.556 | 169.993 | 38.344 | 1130.602 |
| 0087-tidehunter_tcp-256-repeat_read-scalar-c1-baseline | 8192 | 213.924 | 248.444 | 270.466 | 310.766 | 210.420 | 29.148 | 1151.872 |
| 0088-native-256-first_touch-scalar-c1-baseline | 8192 | 13.670 | 14.720 | 24.881 | 35.441 | 14.029 | 4.350 | 367.877 |
| 0089-native-256-repeat_read-scalar-c1-baseline | 8192 | 13.470 | 14.350 | 23.991 | 29.120 | 13.728 | 1.978 | 121.562 |
| 0090-tidehunter_efa_native-256-first_touch-scalar-c1-baseline | 8192 | 74.681 | 109.342 | 133.513 | 327.767 | 78.954 | 29.415 | 2170.713 |
| 0091-tidehunter_efa_native-256-repeat_read-scalar-c1-baseline | 8192 | 79.462 | 88.751 | 96.322 | 105.432 | 79.836 | 5.960 | 294.916 |
| 0092-tidehunter_nvme_tcp-65536-first_touch-scalar-c1-baseline | 8192 | 485.279 | 743.054 | 811.746 | 975.149 | 504.674 | 148.057 | 1424.008 |
| 0093-tidehunter_nvme_tcp-65536-repeat_read-scalar-c1-baseline | 8192 | 577.302 | 731.504 | 800.055 | 918.748 | 581.582 | 87.204 | 1391.627 |
| 0094-tidehunter_tcp-65536-first_touch-scalar-c1-baseline | 8192 | 289.846 | 563.061 | 741.965 | 947.218 | 325.462 | 107.052 | 2742.304 |
| 0095-tidehunter_tcp-65536-repeat_read-scalar-c1-baseline | 8192 | 376.797 | 459.009 | 526.381 | 578.691 | 376.434 | 55.288 | 1390.047 |
| 0096-native-65536-first_touch-scalar-c1-baseline | 8192 | 129.882 | 354.207 | 426.918 | 1182.843 | 166.992 | 98.545 | 2201.932 |
| 0097-native-65536-repeat_read-scalar-c1-baseline | 8192 | 49.190 | 59.422 | 64.442 | 71.231 | 49.934 | 4.155 | 212.594 |
| 0098-tidehunter_efa_native-65536-first_touch-scalar-c1-baseline | 8192 | 292.426 | 511.390 | 534.420 | 1037.100 | 326.201 | 84.021 | 2448.457 |
| 0099-tidehunter_efa_native-65536-repeat_read-scalar-c1-baseline | 8192 | 213.595 | 229.825 | 238.474 | 300.636 | 213.881 | 21.102 | 1638.872 |
| 0100-tidehunter_tcp-1024-write-c16-baseline | 4096 | 1003.269 | 1832.045 | 56502.226 | 59209.608 | 2557.753 | 9004.546 | 59230.099 |
| 0101-tidehunter_tcp-1024-write-batch32-c16-baseline | 2048 | 3227.965 | 5768.986 | 100640.675 | 117179.189 | 5591.872 | 15155.307 | 117195.919 |
| 0102-tidehunter_tcp-1024-write-c16-diagnostic | 4096 | 1027.440 | 1886.167 | 58827.923 | 61120.467 | 2566.192 | 8976.543 | 61150.017 |
| 0103-native-1024-write-c16-baseline | 4096 | 672.983 | 1818.885 | 2419.136 | 3254.503 | 824.588 | 499.578 | 4906.505 |
| 0104-native-1024-write-batch32-c16-baseline | 2048 | 2092.541 | 3740.282 | 4704.631 | 5631.238 | 2247.211 | 753.417 | 5759.071 |
| 0105-tidehunter_efa_native-1024-write-c16-baseline | 4096 | 813.736 | 1293.955 | 60480.261 | 65115.771 | 2468.854 | 9755.983 | 65174.993 |
| 0106-tidehunter_efa_native-1024-write-batch32-c16-baseline | 2048 | 2765.733 | 5004.417 | 108082.061 | 137967.360 | 5431.823 | 16519.805 | 137970.580 |
| 0107-tidehunter_efa_native-1024-write-c16-diagnostic | 4096 | 789.175 | 1391.867 | 61551.387 | 64569.876 | 2484.370 | 9912.364 | 64606.087 |
| 0108-tidehunter_tcp-8192-write-c16-baseline | 4096 | 1007.949 | 2083.732 | 54712.609 | 60339.340 | 2541.264 | 8786.395 | 60987.833 |
| 0109-tidehunter_tcp-8192-write-batch32-c16-baseline | 2048 | 8101.228 | 108811.160 | 120443.436 | 123479.115 | 19453.758 | 31386.358 | 126356.550 |
| 0110-tidehunter_tcp-8192-write-c16-diagnostic | 4096 | 1032.760 | 2087.181 | 52969.677 | 54295.323 | 2496.301 | 8294.285 | 55102.228 |
| 0111-native-8192-write-c16-baseline | 4096 | 802.146 | 1591.152 | 2094.231 | 2832.596 | 915.443 | 322.314 | 3671.742 |
| 0112-native-8192-write-batch32-c16-baseline | 2048 | 9617.997 | 14839.439 | 16286.477 | 21133.711 | 9694.485 | 3015.602 | 24237.221 |
| 0113-tidehunter_efa_native-8192-write-c16-baseline | 4096 | 976.639 | 1714.603 | 55277.567 | 58305.916 | 2509.747 | 8828.350 | 58845.326 |
| 0114-tidehunter_efa_native-8192-write-batch32-c16-baseline | 2048 | 7800.222 | 112341.777 | 124345.142 | 130974.832 | 19434.310 | 31980.083 | 131765.787 |
| 0115-tidehunter_efa_native-8192-write-c16-diagnostic | 4096 | 940.279 | 1681.923 | 56787.033 | 59335.533 | 2496.226 | 8983.461 | 59370.263 |
| 0116-tidehunter_tcp-64-write-c16-baseline | 4096 | 662.273 | 1380.957 | 18603.958 | 21944.244 | 1190.807 | 2866.083 | 22263.120 |
| 0117-native-64-write-c16-baseline | 4096 | 443.359 | 1229.645 | 1804.565 | 2687.653 | 537.247 | 354.991 | 3197.983 |
| 0118-tidehunter_efa_native-64-write-c16-baseline | 4096 | 593.541 | 972.489 | 19743.371 | 21898.414 | 1127.140 | 3073.512 | 21912.945 |
| 0119-tidehunter_tcp-256-write-c16-baseline | 4096 | 742.465 | 1368.136 | 17819.744 | 20089.708 | 1246.689 | 2749.557 | 20808.422 |
| 0120-native-256-write-c16-baseline | 4096 | 622.372 | 1921.658 | 2944.347 | 4112.121 | 775.092 | 574.577 | 4931.617 |
| 0121-tidehunter_efa_native-256-write-c16-baseline | 4096 | 665.003 | 1026.490 | 19199.595 | 20408.429 | 1167.910 | 2919.278 | 21056.861 |
| 0122-tidehunter_tcp-65536-write-c16-baseline | 4096 | 2178.412 | 3705.212 | 4609.519 | 5248.231 | 2296.034 | 726.560 | 5381.565 |
| 0123-native-65536-write-c16-baseline | 4096 | 3310.265 | 4805.833 | 6545.117 | 8298.872 | 3330.575 | 953.331 | 9571.607 |
| 0124-tidehunter_efa_native-65536-write-c16-baseline | 4096 | 2204.392 | 3709.773 | 4755.402 | 5958.286 | 2320.459 | 763.431 | 8892.744 |
