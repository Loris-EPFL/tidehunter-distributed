# Complete matched metric tables
Run `th-e4a783f03101a780`. Repetitions **0 and 1** only. Every numeric cell is the median of the corresponding per-repetition statistic; quantiles are not pooled. API/response/scheduling values are microseconds. Population calls/s uses that operation/outcome count divided by whole-case exposure. Rejections have no API/scheduling clock. A population absent from either repetition is not assigned a two-run statistic: `—` is unavailable, not zero. p99/p999 are descriptive for these small populations. [all-results.csv](all-results.csv) retains every individual repetition, mean, standard deviation, maximum, case-wide goodput, failure and unrun case.

## cold_os_cache / cold-os-large / 65536 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | read / success | 2/2 | 366.189 | 662.072 | 805.430 | 698.188 | 57.506 | 37150.5 |
| TH TCP | read / success | 2/2 | 1334.132 | 14989.242 | 19060.800 | 15204.612 | 97.632 | 8359.2 |
| TH EFA | read / success | 2/2 | 1135.293 | 11196.024 | 20650.578 | 11591.120 | 89.803 | 9875.5 |
| TH EFA-direct | read / success | 2/2 | 1173.784 | 13801.598 | 18972.158 | 14194.623 | 86.992 | 9490.2 |
| TH EFA-native | read / success | 2/2 | 1171.062 | 15073.989 | 19759.862 | 15106.530 | 73.516 | 9598.7 |
| TiKV | read / success | 2/2 | 838.365 | 1764.202 | 3105.340 | 1803.543 | 71.487 | 17029.2 |
| Cassandra | read / success | 2/2 | 1077.151 | 44004.800 | 53380.817 | 44038.331 | 106.538 | 10082.1 |
| FoundationDB | read / success | 2/2 | 1306.946 | 3060.889 | 12230.697 | 3117.780 | 88.217 | 10884.4 |

## coordinator_cache_configured / coordinator-cache / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TH TCP | read / success | 2/2 | 0.810 | 1.725 | 5.450 | 24.225 | 22.916 | 34109.2 |
| TH EFA | read / success | 2/2 | 0.770 | 1.690 | 11.511 | 24.535 | 23.355 | 34355.7 |
| TH EFA-direct | read / success | 2/2 | 0.825 | 1.736 | 11.941 | 24.041 | 22.951 | 35373.3 |
| TH EFA-native | read / success | 2/2 | 0.870 | 1.825 | 6.691 | 23.910 | 22.721 | 35954.4 |

## coordinator_cache_configured / coordinator-cache / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TH TCP | read / success | 2/2 | 0.951 | 1.780 | 9.265 | 28.446 | 27.176 | 30923.0 |
| TH EFA | read / success | 2/2 | 0.880 | 1.790 | 9.625 | 27.875 | 26.611 | 30345.0 |
| TH EFA-direct | read / success | 2/2 | 0.915 | 1.925 | 9.316 | 27.051 | 25.861 | 33064.7 |
| TH EFA-native | read / success | 2/2 | 0.855 | 1.725 | 11.125 | 26.890 | 25.780 | 30991.7 |

## native_pagecache / point-mixed / 64 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 4.901 | 13.175 | 15.255 | 28.870 | 23.526 | 1536.5 |
| Native | missing_read / success | 2/2 | 0.605 | 1.170 | 3.541 | 23.955 | 23.275 | 1517.8 |
| Native | put / success | 2/2 | 4.965 | 13.225 | 18.366 | 29.521 | 23.471 | 14166.3 |
| Native | read / success | 2/2 | 1.190 | 2.056 | 8.645 | 25.230 | 23.650 | 13479.4 |

## native_pagecache / point-mixed / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 4.960 | 13.771 | 18.855 | 29.500 | 23.411 | 1520.2 |
| Native | missing_read / success | 2/2 | 0.595 | 1.040 | 1.446 | 19.921 | 19.145 | 1504.3 |
| Native | put / success | 2/2 | 4.915 | 15.850 | 36.261 | 31.410 | 23.766 | 14039.5 |
| Native | read / success | 2/2 | 1.060 | 1.946 | 7.695 | 25.986 | 24.615 | 13362.9 |

## native_pagecache / point-mixed / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 5.840 | 13.030 | 22.520 | 34.341 | 23.905 | 1376.5 |
| Native | missing_read / success | 2/2 | 0.825 | 1.635 | 2.100 | 26.515 | 25.721 | 1358.0 |
| Native | put / success | 2/2 | 6.050 | 26.155 | 51.416 | 43.191 | 27.446 | 12675.7 |
| Native | read / success | 2/2 | 1.745 | 4.955 | 13.805 | 30.011 | 27.191 | 12058.5 |

## native_pagecache / point-mixed / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 6.091 | 17.601 | 20.410 | 35.011 | 24.221 | 1174.2 |
| Native | missing_read / success | 2/2 | 0.700 | 2.070 | 9.140 | 25.596 | 24.421 | 1162.4 |
| Native | put / success | 2/2 | 12.370 | 84.682 | 256.291 | 112.432 | 29.755 | 10849.2 |
| Native | read / success | 2/2 | 3.820 | 16.070 | 20.831 | 41.576 | 29.380 | 10327.1 |

## native_pagecache / point-mixed / 65536 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 6.725 | 20.211 | 23.770 | 39.266 | 23.606 | 451.7 |
| Native | missing_read / success | 2/2 | 0.960 | 4.490 | 13.220 | 26.811 | 24.210 | 445.4 |
| Native | put / success | 2/2 | 74.292 | 834.959 | 1040.385 | 884.126 | 84.882 | 4157.7 |
| Native | read / success | 2/2 | 14.236 | 49.211 | 63.477 | 129.023 | 85.332 | 3954.9 |

## native_pagecache / read-many32 / 64 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 56.346 | 101.763 | 130.192 | 164.563 | 65.582 | 115251.0 |

## native_pagecache / read-many32 / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 43.706 | 119.598 | 187.790 | 227.541 | 102.523 | 107516.8 |

## native_pagecache / read-many32 / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 56.486 | 126.018 | 200.079 | 222.876 | 97.197 | 108848.0 |

## native_pagecache / read-many32 / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 122.383 | 261.111 | 473.251 | 464.226 | 225.771 | 51679.6 |

## native_pagecache / read-many32 / 65536 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 1449.591 | 52108.834 | 54386.120 | 55919.675 | 53593.211 | 2286.2 |

## owner_cache_configured / owner-cache / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TH TCP | read / success | 2/2 | 153.489 | 252.831 | 289.287 | 270.722 | 31.515 | 4811.8 |
| TH EFA | read / success | 2/2 | 47.156 | 83.597 | 112.573 | 98.838 | 26.271 | 11813.5 |
| TH EFA-direct | read / success | 2/2 | 45.446 | 78.866 | 101.738 | 93.612 | 25.641 | 12280.6 |
| TH EFA-native | read / success | 2/2 | 43.591 | 75.837 | 104.942 | 89.352 | 23.265 | 13143.6 |

## owner_cache_configured / owner-cache / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TH TCP | read / success | 2/2 | 166.174 | 297.942 | 371.314 | 319.043 | 34.615 | 4266.5 |
| TH EFA | read / success | 2/2 | 56.556 | 101.997 | 151.263 | 119.503 | 29.386 | 9853.6 |
| TH EFA-direct | read / success | 2/2 | 55.572 | 97.828 | 138.944 | 115.688 | 29.840 | 10284.9 |
| TH EFA-native | read / success | 2/2 | 49.977 | 93.498 | 125.578 | 111.583 | 28.781 | 10731.1 |

## primary_rf1 / offered-1000 / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | put / success | 2/2 | 418.035 | 585.429 | 766.928 | 1709.565 | 1210.761 | 305.0 |
| Native | read / success | 2/2 | 6.500 | 16.191 | 41.441 | 1225.372 | 1217.038 | 695.1 |
| TH TCP | put / rejected | 1/2 | — | — | — | — | — | — |
| TH TCP | put / success | 2/2 | 722.868 | 136060.667 | 145316.486 | 141549.181 | 77655.126 | 304.7 |
| TH TCP | read / rejected | 2/2 | — | — | — | 1135.882 | — | 0.4 |
| TH TCP | read / success | 2/2 | 269.376 | 472.287 | 890.086 | 86719.811 | 86371.048 | 694.6 |
| TH EFA | put / rejected | 1/2 | — | — | — | — | — | — |
| TH EFA | put / success | 2/2 | 513.203 | 130445.867 | 143553.763 | 139210.314 | 72427.825 | 304.7 |
| TH EFA | read / rejected | 1/2 | — | — | — | — | — | — |
| TH EFA | read / success | 2/2 | 119.018 | 230.546 | 514.178 | 83594.361 | 83465.433 | 694.6 |
| TH EFA-direct | put / rejected | 1/2 | — | — | — | — | — | — |
| TH EFA-direct | put / success | 2/2 | 508.957 | 134628.093 | 144404.540 | 140051.266 | 75022.715 | 304.7 |
| TH EFA-direct | read / rejected | 1/2 | — | — | — | — | — | — |
| TH EFA-direct | read / success | 2/2 | 107.092 | 215.526 | 427.635 | 85597.618 | 85494.061 | 694.5 |
| TH EFA-native | put / rejected | 1/2 | — | — | — | — | — | — |
| TH EFA-native | put / success | 2/2 | 489.112 | 134941.807 | 144080.292 | 139653.030 | 75104.963 | 304.7 |
| TH EFA-native | read / rejected | 1/2 | — | — | — | — | — | — |
| TH EFA-native | read / success | 2/2 | 97.338 | 208.580 | 487.172 | 85491.124 | 85391.567 | 694.7 |
| TiKV | put / success | 2/2 | 889.437 | 1325.032 | 1428.850 | 2634.443 | 1511.119 | 305.0 |
| TiKV | read / success | 2/2 | 454.986 | 652.625 | 727.808 | 2068.273 | 1518.459 | 695.0 |
| Cassandra | put / success | 2/2 | 540.404 | 1113.902 | 2686.285 | 2251.434 | 1417.127 | 305.0 |
| Cassandra | read / success | 2/2 | 375.679 | 539.938 | 677.961 | 1860.799 | 1420.900 | 695.0 |
| FoundationDB | put / success | 2/2 | 1237.640 | 2028.553 | 2318.771 | 3415.667 | 1632.153 | 304.9 |
| FoundationDB | read / success | 2/2 | 612.255 | 4846.812 | 8024.759 | 6098.413 | 1635.629 | 694.9 |

## primary_rf1 / offered-10000 / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | put / success | 2/2 | 447.991 | 1007.394 | 1277.816 | 2648.121 | 1999.529 | 3014.3 |
| Native | read / success | 2/2 | 5.636 | 21.666 | 48.786 | 2009.157 | 2002.794 | 6984.5 |
| TH TCP | put / rejected | 2/2 | — | — | — | 1950.713 | — | 863.7 |
| TH TCP | put / success | 2/2 | 838.745 | 173688.030 | 182305.394 | 180703.658 | 171622.060 | 2150.4 |
| TH TCP | read / rejected | 2/2 | — | — | — | 1946.970 | — | 2032.0 |
| TH TCP | read / success | 2/2 | 235.776 | 690.372 | 1037.060 | 171212.446 | 170954.170 | 4952.2 |
| TH EFA | put / rejected | 2/2 | — | — | — | 1906.992 | — | 826.3 |
| TH EFA | put / success | 2/2 | 592.350 | 171505.291 | 181008.022 | 179300.056 | 168191.593 | 2187.9 |
| TH EFA | read / rejected | 2/2 | — | — | — | 1916.008 | — | 1938.8 |
| TH EFA | read / success | 2/2 | 123.013 | 396.350 | 693.532 | 169488.332 | 169314.206 | 5045.7 |
| TH EFA-direct | put / rejected | 2/2 | — | — | — | 1919.321 | — | 827.8 |
| TH EFA-direct | put / success | 2/2 | 588.604 | 171756.605 | 181391.518 | 179668.364 | 168521.214 | 2186.4 |
| TH EFA-direct | read / rejected | 2/2 | — | — | — | 1914.445 | — | 1955.2 |
| TH EFA-direct | read / success | 2/2 | 114.833 | 395.680 | 686.422 | 169343.127 | 169224.503 | 5029.2 |
| TH EFA-native | put / rejected | 2/2 | — | — | — | 1936.216 | — | 858.5 |
| TH EFA-native | put / success | 2/2 | 592.374 | 171796.357 | 181577.246 | 179466.726 | 169238.196 | 2155.5 |
| TH EFA-native | read / rejected | 2/2 | — | — | — | 1937.583 | — | 2015.2 |
| TH EFA-native | read / success | 2/2 | 104.488 | 384.179 | 637.400 | 169885.792 | 169728.598 | 4968.8 |
| TiKV | put / success | 2/2 | 784.184 | 1414.229 | 1605.048 | 2961.087 | 1959.412 | 3013.9 |
| TiKV | read / success | 2/2 | 403.889 | 787.179 | 1145.428 | 2435.831 | 1960.586 | 6983.7 |
| Cassandra | put / rejected | 1/2 | — | — | — | — | — | — |
| Cassandra | put / success | 2/2 | 470.511 | 863.896 | 1563.468 | 2575.261 | 1909.745 | 3012.7 |
| Cassandra | read / rejected | 1/2 | — | — | — | — | — | — |
| Cassandra | read / success | 2/2 | 288.551 | 484.772 | 615.160 | 2291.082 | 1914.997 | 6981.3 |
| FoundationDB | put / rejected | 1/2 | — | — | — | — | — | — |
| FoundationDB | put / success | 2/2 | 1031.499 | 4377.085 | 6010.430 | 7771.551 | 3624.231 | 3008.4 |
| FoundationDB | read / rejected | 1/2 | — | — | — | — | — | — |
| FoundationDB | read / success | 2/2 | 681.596 | 4430.012 | 4894.373 | 6272.977 | 3312.416 | 6971.3 |

## primary_rf1 / offered-50000 / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | put / rejected | 2/2 | — | — | — | 1689.205 | — | 3551.4 |
| Native | put / success | 2/2 | 1309.151 | 8066.084 | 11209.769 | 15462.485 | 11928.129 | 11435.6 |
| Native | read / rejected | 2/2 | — | — | — | 1679.170 | — | 8294.0 |
| Native | read / success | 2/2 | 3.430 | 16.140 | 41.316 | 11913.518 | 11908.350 | 26700.5 |
| TH TCP | put / rejected | 2/2 | — | — | — | 10507.048 | — | 10523.1 |
| TH TCP | put / success | 2/2 | 1065.916 | 183188.386 | 196789.133 | 196825.585 | 184603.087 | 4444.2 |
| TH TCP | read / rejected | 2/2 | — | — | — | 10761.779 | — | 24495.7 |
| TH TCP | read / success | 2/2 | 212.620 | 867.115 | 13684.144 | 184919.364 | 184542.062 | 10452.5 |
| TH EFA | put / rejected | 2/2 | — | — | — | 1961.238 | — | 10130.7 |
| TH EFA | put / success | 2/2 | 1019.520 | 182595.603 | 199275.045 | 196885.317 | 184365.911 | 4463.3 |
| TH EFA | read / rejected | 2/2 | — | — | — | 1956.856 | — | 23641.4 |
| TH EFA | read / success | 2/2 | 115.368 | 464.691 | 668.106 | 184599.457 | 184425.445 | 10436.4 |
| TH EFA-direct | put / rejected | 2/2 | — | — | — | 1954.005 | — | 10308.9 |
| TH EFA-direct | put / success | 2/2 | 1018.949 | 183882.480 | 196789.092 | 196263.335 | 185187.812 | 4530.3 |
| TH EFA-direct | read / rejected | 2/2 | — | — | — | 1954.193 | — | 24048.8 |
| TH EFA-direct | read / success | 2/2 | 111.943 | 443.655 | 635.446 | 185334.160 | 185177.206 | 10600.9 |
| TH EFA-native | put / rejected | 2/2 | — | — | — | 1957.284 | — | 10165.3 |
| TH EFA-native | put / success | 2/2 | 999.034 | 182951.731 | 197648.791 | 196371.258 | 184667.089 | 4465.7 |
| TH EFA-native | read / rejected | 2/2 | — | — | — | 1964.793 | — | 23663.3 |
| TH EFA-native | read / success | 2/2 | 105.043 | 420.701 | 598.375 | 184432.160 | 184297.076 | 10500.1 |
| TiKV | put / rejected | 2/2 | — | — | — | 770.649 | — | 4509.7 |
| TiKV | put / success | 2/2 | 1120.302 | 1802.179 | 2072.481 | 6469.179 | 5194.608 | 10475.1 |
| TiKV | read / rejected | 2/2 | — | — | — | 768.385 | — | 10544.9 |
| TiKV | read / success | 2/2 | 801.215 | 1402.209 | 1612.089 | 6104.337 | 5195.623 | 24444.4 |
| Cassandra | put / rejected | 1/2 | — | — | — | — | — | — |
| Cassandra | put / success | 2/2 | 522.043 | 946.643 | 1877.290 | 2663.766 | 2013.490 | 14988.8 |
| Cassandra | read / rejected | 2/2 | — | — | — | 339.315 | — | 0.8 |
| Cassandra | read / success | 2/2 | 263.887 | 401.460 | 477.211 | 2309.664 | 2014.451 | 34999.0 |
| FoundationDB | put / rejected | 2/2 | — | — | — | 1458.693 | — | 6497.5 |
| FoundationDB | put / success | 2/2 | 1475.301 | 2289.360 | 2617.369 | 9388.461 | 7605.275 | 8479.4 |
| FoundationDB | read / rejected | 2/2 | — | — | — | 1460.057 | — | 15226.8 |
| FoundationDB | read / success | 2/2 | 882.661 | 2283.745 | 6015.425 | 8742.404 | 7589.278 | 19743.9 |

## primary_rf1 / point-mixed / 64 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 171.399 | 360.629 | 415.279 | 421.910 | 29.641 | 353.1 |
| Native | missing_read / success | 2/2 | 1.090 | 2.895 | 4.771 | 30.455 | 28.420 | 349.3 |
| Native | put / success | 2/2 | 171.680 | 348.089 | 428.175 | 370.818 | 29.701 | 3260.3 |
| Native | read / success | 2/2 | 2.245 | 5.675 | 15.215 | 34.675 | 29.951 | 3103.1 |
| TH TCP | delete / success | 2/2 | 556.463 | 741.983 | 786.389 | 772.734 | 27.466 | 98.6 |
| TH TCP | missing_read / success | 2/2 | 1.115 | 2.935 | 17.116 | 30.120 | 27.381 | 97.4 |
| TH TCP | put / success | 2/2 | 559.254 | 775.324 | 143734.770 | 789.899 | 27.776 | 909.5 |
| TH TCP | read / success | 2/2 | 227.840 | 397.924 | 436.281 | 412.440 | 27.461 | 865.5 |
| TH EFA | delete / success | 2/2 | 344.618 | 483.856 | 2199.709 | 498.812 | 31.251 | 146.2 |
| TH EFA | missing_read / success | 2/2 | 1.140 | 3.570 | 12.505 | 28.220 | 25.226 | 144.9 |
| TH EFA | put / success | 2/2 | 340.668 | 497.933 | 143013.200 | 523.062 | 29.120 | 1352.4 |
| TH EFA | read / success | 2/2 | 84.902 | 167.524 | 199.195 | 188.010 | 29.480 | 1287.6 |
| TH EFA-direct | delete / success | 2/2 | 329.498 | 453.341 | 2243.565 | 472.507 | 27.346 | 150.8 |
| TH EFA-direct | missing_read / success | 2/2 | 1.030 | 3.065 | 14.460 | 27.146 | 24.680 | 149.1 |
| TH EFA-direct | put / success | 2/2 | 331.038 | 480.272 | 143180.577 | 495.092 | 28.806 | 1391.9 |
| TH EFA-direct | read / success | 2/2 | 79.752 | 153.629 | 181.909 | 170.749 | 28.601 | 1324.7 |
| TH EFA-native | delete / success | 2/2 | 332.118 | 459.831 | 1400.034 | 473.841 | 24.661 | 150.3 |
| TH EFA-native | missing_read / success | 2/2 | 1.025 | 4.310 | 12.105 | 26.950 | 24.270 | 148.6 |
| TH EFA-native | put / success | 2/2 | 333.323 | 489.987 | 143053.939 | 513.832 | 27.175 | 1386.8 |
| TH EFA-native | read / success | 2/2 | 74.797 | 153.394 | 175.169 | 171.139 | 27.240 | 1319.7 |
| TiKV | delete / success | 2/2 | 771.893 | 1105.322 | 1226.085 | 1128.462 | 40.851 | 74.3 |
| TiKV | missing_read / success | 2/2 | 412.811 | 605.220 | 711.372 | 634.415 | 42.336 | 73.7 |
| TiKV | put / success | 2/2 | 769.814 | 1147.777 | 1295.781 | 1170.148 | 45.225 | 688.0 |
| TiKV | read / success | 2/2 | 411.330 | 624.775 | 683.127 | 654.331 | 44.082 | 655.3 |
| Cassandra | delete / success | 2/2 | 464.392 | 761.873 | 1311.102 | 791.034 | 56.882 | 108.6 |
| Cassandra | missing_read / success | 2/2 | 319.423 | 447.571 | 485.247 | 472.836 | 44.246 | 107.2 |
| Cassandra | put / success | 2/2 | 466.851 | 858.366 | 1196.749 | 882.026 | 49.736 | 1000.4 |
| Cassandra | read / success | 2/2 | 333.077 | 467.311 | 564.888 | 498.058 | 53.547 | 951.7 |
| FoundationDB | delete / success | 2/2 | 1107.277 | 1840.085 | 1974.278 | 1903.162 | 30.276 | 36.5 |
| FoundationDB | missing_read / success | 2/2 | 611.240 | 4820.256 | 4949.324 | 4832.086 | 35.371 | 36.1 |
| FoundationDB | put / success | 2/2 | 1105.872 | 1804.918 | 1921.506 | 1834.849 | 38.446 | 336.6 |
| FoundationDB | read / success | 2/2 | 652.996 | 4924.429 | 5254.352 | 4942.845 | 38.766 | 320.3 |

## primary_rf1 / point-mixed / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 165.039 | 339.718 | 379.964 | 357.779 | 22.775 | 414.7 |
| Native | missing_read / success | 2/2 | 1.070 | 2.200 | 8.565 | 29.515 | 27.451 | 410.8 |
| Native | put / success | 2/2 | 165.094 | 334.393 | 480.676 | 353.608 | 27.030 | 3834.3 |
| Native | read / success | 2/2 | 2.065 | 3.764 | 16.175 | 29.616 | 26.186 | 3650.2 |
| TH TCP | delete / success | 2/2 | 613.985 | 752.763 | 1416.284 | 776.718 | 27.281 | 93.0 |
| TH TCP | missing_read / success | 2/2 | 1.180 | 4.520 | 9.271 | 28.140 | 26.536 | 92.0 |
| TH TCP | put / success | 2/2 | 622.675 | 784.474 | 143391.459 | 804.809 | 29.215 | 858.8 |
| TH TCP | read / success | 2/2 | 231.206 | 404.600 | 435.541 | 416.525 | 28.680 | 817.4 |
| TH EFA | delete / success | 2/2 | 394.519 | 549.058 | 2092.110 | 572.729 | 27.460 | 135.2 |
| TH EFA | missing_read / success | 2/2 | 1.155 | 5.495 | 21.671 | 33.020 | 29.026 | 133.9 |
| TH EFA | put / success | 2/2 | 395.170 | 555.024 | 142573.440 | 580.149 | 30.365 | 1249.5 |
| TH EFA | read / success | 2/2 | 85.642 | 162.889 | 187.179 | 184.799 | 30.925 | 1189.5 |
| TH EFA-direct | delete / success | 2/2 | 390.725 | 496.072 | 2078.226 | 517.497 | 26.406 | 138.3 |
| TH EFA-direct | missing_read / success | 2/2 | 1.075 | 5.060 | 11.055 | 27.210 | 25.375 | 136.8 |
| TH EFA-direct | put / success | 2/2 | 392.125 | 538.342 | 142927.901 | 561.093 | 28.226 | 1276.6 |
| TH EFA-direct | read / success | 2/2 | 79.732 | 154.174 | 184.419 | 170.199 | 27.981 | 1215.0 |
| TH EFA-native | delete / success | 2/2 | 386.700 | 541.533 | 2066.289 | 565.759 | 24.320 | 140.3 |
| TH EFA-native | missing_read / success | 2/2 | 0.975 | 2.450 | 7.945 | 25.716 | 23.975 | 138.8 |
| TH EFA-native | put / success | 2/2 | 388.680 | 526.498 | 143103.003 | 552.468 | 25.681 | 1295.0 |
| TH EFA-native | read / success | 2/2 | 74.731 | 149.329 | 172.069 | 162.889 | 25.941 | 1232.4 |
| TiKV | delete / success | 2/2 | 749.773 | 1122.202 | 1360.233 | 1153.578 | 31.771 | 80.7 |
| TiKV | missing_read / success | 2/2 | 411.825 | 625.215 | 663.262 | 646.855 | 40.225 | 80.0 |
| TiKV | put / success | 2/2 | 759.023 | 1186.109 | 1313.722 | 1202.555 | 35.111 | 746.6 |
| TiKV | read / success | 2/2 | 397.450 | 612.044 | 674.767 | 639.200 | 37.696 | 710.8 |
| Cassandra | delete / success | 2/2 | 441.566 | 770.728 | 938.212 | 829.315 | 43.591 | 108.4 |
| Cassandra | missing_read / success | 2/2 | 320.363 | 446.706 | 469.216 | 468.952 | 39.836 | 107.3 |
| Cassandra | put / success | 2/2 | 459.926 | 899.572 | 1099.537 | 924.993 | 51.276 | 1001.2 |
| Cassandra | read / success | 2/2 | 334.998 | 483.462 | 547.444 | 513.863 | 47.536 | 952.9 |
| FoundationDB | delete / success | 2/2 | 1010.544 | 1604.089 | 1678.661 | 1623.364 | 33.151 | 36.1 |
| FoundationDB | missing_read / success | 2/2 | 649.361 | 5096.078 | 5397.666 | 5110.184 | 41.666 | 35.7 |
| FoundationDB | put / success | 2/2 | 1009.124 | 1599.249 | 1730.897 | 1632.289 | 42.951 | 332.9 |
| FoundationDB | read / success | 2/2 | 740.858 | 5063.227 | 5406.736 | 5083.104 | 47.996 | 316.9 |

## primary_rf1 / point-mixed / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 167.444 | 379.589 | 393.610 | 403.100 | 30.436 | 383.5 |
| Native | missing_read / success | 2/2 | 1.071 | 2.830 | 14.651 | 31.825 | 26.956 | 378.4 |
| Native | put / success | 2/2 | 169.124 | 387.429 | 520.513 | 411.440 | 31.236 | 3531.5 |
| Native | read / success | 2/2 | 2.305 | 6.015 | 17.861 | 37.246 | 31.676 | 3359.7 |
| TH TCP | delete / success | 2/2 | 591.699 | 773.839 | 1494.051 | 791.344 | 31.105 | 90.8 |
| TH TCP | missing_read / success | 2/2 | 1.355 | 6.775 | 13.795 | 32.711 | 30.950 | 89.8 |
| TH TCP | put / success | 2/2 | 612.615 | 1010.440 | 143798.646 | 1031.575 | 31.486 | 838.2 |
| TH TCP | read / success | 2/2 | 235.525 | 402.700 | 441.250 | 421.180 | 31.776 | 797.6 |
| TH EFA | delete / success | 2/2 | 392.995 | 561.099 | 1562.763 | 586.594 | 27.030 | 135.1 |
| TH EFA | missing_read / success | 2/2 | 1.120 | 3.260 | 16.800 | 30.601 | 28.570 | 133.8 |
| TH EFA | put / success | 2/2 | 402.570 | 721.803 | 142712.405 | 738.428 | 30.466 | 1248.5 |
| TH EFA | read / success | 2/2 | 87.242 | 168.024 | 193.889 | 189.730 | 29.645 | 1188.4 |
| TH EFA-direct | delete / success | 2/2 | 374.054 | 505.527 | 1387.809 | 526.553 | 27.561 | 140.4 |
| TH EFA-direct | missing_read / success | 2/2 | 1.095 | 4.130 | 12.646 | 28.811 | 27.020 | 138.8 |
| TH EFA-direct | put / success | 2/2 | 389.240 | 694.152 | 142764.021 | 717.367 | 28.746 | 1295.8 |
| TH EFA-direct | read / success | 2/2 | 81.786 | 160.159 | 185.434 | 175.260 | 28.246 | 1233.3 |
| TH EFA-native | delete / success | 2/2 | 375.204 | 502.982 | 1449.320 | 523.643 | 26.696 | 142.8 |
| TH EFA-native | missing_read / success | 2/2 | 1.030 | 2.610 | 11.195 | 25.381 | 23.801 | 141.3 |
| TH EFA-native | put / success | 2/2 | 385.219 | 704.003 | 142168.981 | 714.987 | 27.346 | 1318.9 |
| TH EFA-native | read / success | 2/2 | 75.001 | 149.393 | 177.924 | 163.674 | 26.956 | 1255.2 |
| TiKV | delete / success | 2/2 | 777.089 | 1151.983 | 1276.646 | 1164.409 | 49.936 | 73.0 |
| TiKV | missing_read / success | 2/2 | 423.796 | 627.211 | 671.157 | 649.981 | 39.651 | 72.1 |
| TiKV | put / success | 2/2 | 823.904 | 1296.917 | 1414.044 | 1325.692 | 44.031 | 673.2 |
| TiKV | read / success | 2/2 | 410.605 | 631.176 | 703.347 | 662.087 | 46.672 | 640.6 |
| Cassandra | delete / success | 2/2 | 450.806 | 770.668 | 907.591 | 800.630 | 46.246 | 101.5 |
| Cassandra | missing_read / success | 2/2 | 321.183 | 446.921 | 495.707 | 478.137 | 46.401 | 100.3 |
| Cassandra | put / success | 2/2 | 489.777 | 924.092 | 1369.003 | 956.962 | 55.257 | 936.2 |
| Cassandra | read / success | 2/2 | 336.494 | 472.201 | 539.433 | 505.797 | 58.831 | 890.8 |
| FoundationDB | delete / success | 2/2 | 1006.609 | 1580.418 | 1801.774 | 1608.838 | 44.526 | 35.7 |
| FoundationDB | missing_read / success | 2/2 | 696.782 | 5113.818 | 5288.738 | 5130.494 | 53.816 | 35.3 |
| FoundationDB | put / success | 2/2 | 1013.185 | 1598.909 | 1723.501 | 1639.944 | 46.561 | 329.8 |
| FoundationDB | read / success | 2/2 | 734.257 | 5139.379 | 5587.440 | 5154.420 | 51.011 | 313.8 |

## primary_rf1 / point-mixed / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 174.489 | 323.078 | 450.996 | 340.413 | 25.191 | 323.3 |
| Native | missing_read / success | 2/2 | 1.109 | 2.016 | 9.810 | 29.035 | 27.851 | 319.8 |
| Native | put / success | 2/2 | 188.944 | 468.781 | 570.428 | 489.152 | 32.356 | 2985.0 |
| Native | read / success | 2/2 | 4.146 | 10.316 | 23.560 | 39.381 | 32.811 | 2841.1 |
| TH TCP | delete / success | 2/2 | 602.820 | 855.236 | 2514.271 | 867.147 | 28.681 | 80.3 |
| TH TCP | missing_read / success | 2/2 | 1.340 | 6.335 | 9.361 | 36.871 | 32.706 | 79.4 |
| TH TCP | put / success | 2/2 | 723.632 | 1229.155 | 144232.017 | 1249.345 | 39.261 | 740.7 |
| TH TCP | read / success | 2/2 | 257.957 | 427.926 | 492.288 | 451.461 | 38.786 | 704.9 |
| TH EFA | delete / success | 2/2 | 389.834 | 527.303 | 610.814 | 545.748 | 29.276 | 119.3 |
| TH EFA | missing_read / success | 2/2 | 1.180 | 3.645 | 21.556 | 31.866 | 27.491 | 117.9 |
| TH EFA | put / success | 2/2 | 467.346 | 831.886 | 143561.668 | 849.910 | 37.566 | 1100.1 |
| TH EFA | read / success | 2/2 | 103.573 | 187.224 | 217.725 | 214.611 | 38.151 | 1046.8 |
| TH EFA-direct | delete / success | 2/2 | 382.434 | 538.234 | 658.556 | 563.274 | 31.645 | 119.3 |
| TH EFA-direct | missing_read / success | 2/2 | 1.120 | 3.650 | 13.241 | 30.111 | 27.996 | 118.1 |
| TH EFA-direct | put / success | 2/2 | 463.067 | 836.375 | 143330.551 | 857.506 | 39.091 | 1102.2 |
| TH EFA-direct | read / success | 2/2 | 95.567 | 175.399 | 207.480 | 208.795 | 38.421 | 1049.1 |
| TH EFA-native | delete / success | 2/2 | 375.114 | 483.582 | 1339.028 | 497.452 | 26.140 | 125.4 |
| TH EFA-native | missing_read / success | 2/2 | 1.020 | 2.700 | 7.966 | 28.871 | 27.516 | 124.1 |
| TH EFA-native | put / success | 2/2 | 447.486 | 783.249 | 143687.244 | 801.430 | 33.671 | 1158.3 |
| TH EFA-native | read / success | 2/2 | 85.627 | 162.319 | 186.024 | 185.394 | 33.696 | 1102.5 |
| TiKV | delete / success | 2/2 | 794.199 | 1034.925 | 1155.058 | 1061.615 | 44.657 | 59.7 |
| TiKV | missing_read / success | 2/2 | 424.120 | 637.916 | 692.437 | 659.716 | 35.876 | 58.9 |
| TiKV | put / success | 2/2 | 1173.828 | 1463.050 | 1660.620 | 1494.631 | 55.647 | 550.0 |
| TiKV | read / success | 2/2 | 437.810 | 668.316 | 736.603 | 703.672 | 54.272 | 523.3 |
| Cassandra | delete / success | 2/2 | 469.636 | 732.937 | 903.617 | 760.188 | 54.916 | 80.1 |
| Cassandra | missing_read / success | 2/2 | 341.568 | 479.106 | 535.982 | 507.432 | 55.686 | 79.2 |
| Cassandra | put / success | 2/2 | 753.198 | 1054.336 | 2397.038 | 1095.046 | 69.531 | 738.9 |
| Cassandra | read / success | 2/2 | 368.754 | 520.803 | 633.260 | 558.938 | 68.987 | 703.2 |
| FoundationDB | delete / success | 2/2 | 1017.125 | 1641.945 | 1766.798 | 1709.126 | 45.561 | 36.4 |
| FoundationDB | missing_read / success | 2/2 | 599.320 | 5036.047 | 5659.617 | 5050.852 | 43.391 | 35.9 |
| FoundationDB | put / success | 2/2 | 1060.255 | 1714.581 | 1929.057 | 1757.743 | 49.626 | 335.4 |
| FoundationDB | read / success | 2/2 | 642.026 | 4976.471 | 5402.501 | 4995.641 | 48.881 | 319.2 |

## primary_rf1 / point-mixed / 65536 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | delete / success | 2/2 | 315.737 | 532.982 | 644.875 | 550.168 | 27.430 | 158.0 |
| Native | missing_read / success | 2/2 | 1.360 | 3.210 | 8.350 | 29.186 | 26.986 | 156.7 |
| Native | put / success | 2/2 | 506.797 | 852.856 | 1061.706 | 914.342 | 72.272 | 1462.0 |
| Native | read / success | 2/2 | 15.325 | 34.620 | 60.852 | 106.042 | 71.552 | 1392.0 |
| TH TCP | delete / success | 2/2 | 631.430 | 785.644 | 1704.681 | 805.809 | 32.995 | 48.1 |
| TH TCP | missing_read / success | 2/2 | 1.500 | 6.705 | 13.940 | 34.492 | 31.306 | 47.6 |
| TH TCP | put / success | 2/2 | 1481.196 | 1991.373 | 137830.178 | 2052.979 | 86.842 | 443.9 |
| TH TCP | read / success | 2/2 | 401.495 | 627.794 | 695.106 | 679.141 | 85.928 | 422.4 |
| TH EFA | delete / success | 2/2 | 391.529 | 527.148 | 1320.526 | 577.039 | 35.621 | 70.6 |
| TH EFA | missing_read / success | 2/2 | 1.255 | 3.500 | 9.045 | 33.350 | 30.791 | 69.8 |
| TH EFA | put / success | 2/2 | 1041.680 | 1380.279 | 136100.333 | 1461.105 | 87.647 | 651.6 |
| TH EFA | read / success | 2/2 | 162.559 | 259.856 | 323.343 | 339.979 | 88.862 | 620.2 |
| TH EFA-direct | delete / success | 2/2 | 395.425 | 526.553 | 777.984 | 560.073 | 29.971 | 67.1 |
| TH EFA-direct | missing_read / success | 2/2 | 1.230 | 3.475 | 22.170 | 31.881 | 28.990 | 66.3 |
| TH EFA-direct | put / success | 2/2 | 1088.617 | 1436.800 | 136343.902 | 1506.716 | 93.332 | 619.1 |
| TH EFA-direct | read / success | 2/2 | 200.880 | 284.127 | 323.618 | 356.943 | 88.612 | 589.2 |
| TH EFA-native | delete / success | 2/2 | 402.905 | 558.744 | 781.514 | 583.004 | 30.276 | 66.4 |
| TH EFA-native | missing_read / success | 2/2 | 1.190 | 3.450 | 13.965 | 34.480 | 32.325 | 65.7 |
| TH EFA-native | put / success | 2/2 | 1095.936 | 1449.615 | 136332.794 | 1521.466 | 95.632 | 613.4 |
| TH EFA-native | read / success | 2/2 | 181.880 | 266.162 | 296.027 | 344.093 | 94.718 | 583.9 |
| TiKV | delete / success | 2/2 | 800.319 | 1155.083 | 1385.719 | 1177.048 | 43.301 | 46.3 |
| TiKV | missing_read / success | 2/2 | 438.101 | 679.392 | 805.089 | 701.662 | 47.046 | 45.7 |
| TiKV | put / success | 2/2 | 1491.531 | 1886.910 | 2923.900 | 1974.728 | 107.118 | 426.9 |
| TiKV | read / success | 2/2 | 587.159 | 940.638 | 1076.751 | 1018.380 | 108.822 | 406.2 |
| Cassandra | delete / success | 2/2 | 468.712 | 735.658 | 1013.195 | 759.388 | 43.951 | 59.7 |
| Cassandra | missing_read / success | 2/2 | 339.178 | 485.252 | 538.983 | 509.327 | 42.981 | 59.1 |
| Cassandra | put / success | 2/2 | 1003.149 | 1507.341 | 3024.537 | 1589.119 | 115.507 | 551.3 |
| Cassandra | read / success | 2/2 | 470.486 | 708.037 | 909.517 | 787.383 | 114.172 | 524.7 |
| FoundationDB | delete / success | 2/2 | 972.193 | 1583.513 | 1698.127 | 1651.790 | 42.376 | 34.3 |
| FoundationDB | missing_read / success | 2/2 | 689.367 | 5118.908 | 8765.817 | 5129.144 | 36.121 | 33.9 |
| FoundationDB | put / success | 2/2 | 1096.537 | 1793.024 | 3187.287 | 1897.256 | 110.178 | 316.6 |
| FoundationDB | read / success | 2/2 | 815.044 | 5247.128 | 5942.383 | 5292.588 | 113.563 | 301.3 |

## primary_rf1 / read-concurrent / 64 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | read / success | 2/2 | 1.930 | 3.229 | 17.170 | 27.050 | 23.916 | 116992.2 |
| TH TCP | read / success | 2/2 | 178.704 | 648.931 | 10942.805 | 670.016 | 33.551 | 60773.7 |
| TH EFA | read / success | 2/2 | 90.937 | 312.207 | 445.231 | 327.218 | 28.581 | 109873.0 |
| TH EFA-direct | read / success | 2/2 | 91.012 | 307.117 | 448.571 | 322.233 | 29.371 | 110381.5 |
| TH EFA-native | read / success | 2/2 | 84.377 | 292.096 | 469.901 | 307.212 | 28.710 | 114419.2 |
| TiKV | read / success | 2/2 | 403.575 | 736.257 | 918.317 | 749.678 | 31.210 | 36227.0 |
| Cassandra | read / success | 2/2 | 231.361 | 348.408 | 811.300 | 363.603 | 32.591 | 59627.3 |
| FoundationDB | read / success | 2/2 | 592.080 | 842.135 | 1127.572 | 855.035 | 27.360 | 25293.4 |

## primary_rf1 / read-concurrent / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | read / success | 2/2 | 1.735 | 3.450 | 19.655 | 27.145 | 24.456 | 117106.2 |
| TH TCP | read / success | 2/2 | 182.919 | 671.677 | 21158.452 | 691.891 | 39.081 | 53910.6 |
| TH EFA | read / success | 2/2 | 87.987 | 322.132 | 533.279 | 341.843 | 28.701 | 107301.3 |
| TH EFA-direct | read / success | 2/2 | 91.392 | 315.393 | 446.281 | 330.588 | 28.811 | 108515.3 |
| TH EFA-native | read / success | 2/2 | 82.582 | 287.996 | 401.340 | 301.683 | 29.035 | 114527.1 |
| TiKV | read / success | 2/2 | 345.943 | 604.959 | 912.862 | 619.190 | 32.196 | 41081.8 |
| Cassandra | read / success | 2/2 | 234.406 | 348.603 | 826.695 | 363.769 | 31.625 | 58432.5 |
| FoundationDB | read / success | 2/2 | 602.330 | 854.721 | 1390.339 | 871.111 | 30.821 | 24712.2 |

## primary_rf1 / read-concurrent / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | read / success | 2/2 | 1.815 | 3.530 | 14.735 | 26.606 | 23.575 | 139798.8 |
| TH TCP | read / success | 2/2 | 189.820 | 664.721 | 1053.300 | 694.887 | 34.320 | 58258.9 |
| TH EFA | read / success | 2/2 | 93.912 | 321.598 | 484.522 | 336.928 | 28.755 | 107013.6 |
| TH EFA-direct | read / success | 2/2 | 93.127 | 306.537 | 420.610 | 321.197 | 28.405 | 111681.4 |
| TH EFA-native | read / success | 2/2 | 86.862 | 299.147 | 437.541 | 317.168 | 29.075 | 111679.8 |
| TiKV | read / success | 2/2 | 373.750 | 626.856 | 874.621 | 642.065 | 35.851 | 38890.3 |
| Cassandra | read / success | 2/2 | 237.321 | 366.569 | 12608.908 | 384.869 | 33.386 | 53516.5 |
| FoundationDB | read / success | 2/2 | 635.640 | 919.477 | 1305.966 | 935.183 | 29.986 | 23543.1 |

## primary_rf1 / read-concurrent / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | read / success | 2/2 | 3.925 | 8.245 | 24.005 | 33.770 | 28.495 | 133780.0 |
| TH TCP | read / success | 2/2 | 220.995 | 796.504 | 10272.324 | 820.875 | 39.801 | 50105.5 |
| TH EFA | read / success | 2/2 | 107.017 | 379.539 | 568.284 | 397.659 | 31.801 | 96581.9 |
| TH EFA-direct | read / success | 2/2 | 106.493 | 364.499 | 564.439 | 381.134 | 31.781 | 96207.5 |
| TH EFA-native | read / success | 2/2 | 95.447 | 325.757 | 572.225 | 343.354 | 32.361 | 98707.8 |
| TiKV | read / success | 2/2 | 441.981 | 798.159 | 1258.295 | 814.290 | 36.196 | 32830.1 |
| Cassandra | read / success | 2/2 | 315.947 | 471.000 | 1295.606 | 488.192 | 39.502 | 45482.4 |
| FoundationDB | read / success | 2/2 | 701.913 | 1036.660 | 1689.046 | 1055.195 | 35.451 | 21074.2 |

## primary_rf1 / read-concurrent / 65536 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | read / success | 2/2 | 15.776 | 33.671 | 69.721 | 79.182 | 56.691 | 114691.9 |
| TH TCP | read / success | 2/2 | 371.879 | 1438.835 | 18253.306 | 1513.691 | 102.953 | 27427.5 |
| TH EFA | read / success | 2/2 | 192.560 | 964.088 | 10492.373 | 1008.949 | 58.031 | 44023.6 |
| TH EFA-direct | read / success | 2/2 | 240.266 | 1004.083 | 1475.700 | 1043.405 | 59.522 | 38366.4 |
| TH EFA-native | read / success | 2/2 | 219.196 | 899.591 | 8325.986 | 938.048 | 56.761 | 41497.5 |
| TiKV | read / success | 2/2 | 666.437 | 1256.915 | 1638.995 | 1297.516 | 62.872 | 21453.7 |
| Cassandra | read / success | 2/2 | 419.580 | 696.877 | 1340.952 | 740.503 | 78.987 | 32917.1 |
| FoundationDB | read / success | 2/2 | 1010.314 | 1805.469 | 4397.596 | 1873.215 | 115.448 | 14209.4 |

## primary_rf1 / read-many32 / 64 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 54.631 | 88.462 | 158.819 | 133.499 | 59.017 | 113503.2 |
| TH TCP | get_many / success | 2/2 | 1495.456 | 1918.506 | 2233.664 | 1962.568 | 74.821 | 10240.4 |
| TH EFA | get_many / success | 2/2 | 1149.202 | 1558.668 | 1925.197 | 1605.049 | 70.132 | 13103.8 |
| TH EFA-direct | get_many / success | 2/2 | 1421.255 | 1668.211 | 2052.994 | 1711.637 | 60.871 | 10812.8 |
| TH EFA-native | get_many / success | 2/2 | 1260.550 | 1668.010 | 1821.303 | 1714.876 | 58.877 | 12018.7 |
| TiKV | get_many / success | 2/2 | 6986.083 | 11307.467 | 12895.655 | 11344.258 | 70.387 | 2269.8 |
| Cassandra | get_many / success | 2/2 | 2470.970 | 3620.707 | 4252.006 | 3701.019 | 62.902 | 5695.9 |
| FoundationDB | get_many / success | 2/2 | 8939.646 | 10046.943 | 13814.940 | 10121.795 | 73.822 | 1769.9 |

## primary_rf1 / read-many32 / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 62.427 | 95.792 | 170.789 | 145.203 | 58.351 | 129858.4 |
| TH TCP | get_many / success | 2/2 | 1774.633 | 2331.236 | 2539.856 | 2368.408 | 71.172 | 8623.4 |
| TH EFA | get_many / success | 2/2 | 1345.707 | 1696.706 | 1936.207 | 1745.148 | 76.752 | 11398.1 |
| TH EFA-direct | get_many / success | 2/2 | 1498.581 | 1822.495 | 2170.938 | 1864.755 | 73.202 | 10230.0 |
| TH EFA-native | get_many / success | 2/2 | 1292.761 | 1693.211 | 1861.530 | 1734.677 | 66.951 | 11521.3 |
| TiKV | get_many / success | 2/2 | 6300.153 | 10214.127 | 11508.064 | 10280.353 | 80.837 | 2526.4 |
| Cassandra | get_many / success | 2/2 | 2322.235 | 2755.276 | 3795.691 | 2803.477 | 64.767 | 6554.1 |
| FoundationDB | get_many / success | 2/2 | 10006.847 | 12826.961 | 13496.887 | 12863.221 | 84.802 | 1541.0 |

## primary_rf1 / read-many32 / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 59.321 | 106.978 | 177.834 | 199.524 | 88.632 | 111231.9 |
| TH TCP | get_many / success | 2/2 | 2067.874 | 2790.988 | 2965.457 | 2848.173 | 101.342 | 7343.9 |
| TH EFA | get_many / success | 2/2 | 1811.319 | 2385.863 | 2571.712 | 2430.528 | 96.532 | 8502.7 |
| TH EFA-direct | get_many / success | 2/2 | 1801.073 | 2496.581 | 2804.782 | 2544.122 | 100.768 | 8551.5 |
| TH EFA-native | get_many / success | 2/2 | 1435.374 | 2040.844 | 2354.162 | 2102.796 | 86.972 | 10289.0 |
| TiKV | get_many / success | 2/2 | 6965.728 | 10229.517 | 11449.106 | 10290.543 | 87.238 | 2372.3 |
| Cassandra | get_many / success | 2/2 | 2830.624 | 3396.322 | 4061.028 | 3447.778 | 79.127 | 5326.1 |
| FoundationDB | get_many / success | 2/2 | 10335.180 | 11435.311 | 12135.793 | 11495.758 | 133.108 | 1529.4 |

## primary_rf1 / read-many32 / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 120.097 | 246.711 | 496.382 | 455.576 | 239.180 | 51914.7 |
| TH TCP | get_many / success | 2/2 | 4667.512 | 6624.615 | 7198.153 | 6771.223 | 277.496 | 3277.2 |
| TH EFA | get_many / success | 2/2 | 3728.920 | 5060.962 | 5675.611 | 5226.066 | 325.457 | 4368.6 |
| TH EFA-direct | get_many / success | 2/2 | 4209.162 | 6059.492 | 6380.815 | 6225.821 | 330.478 | 3660.7 |
| TH EFA-native | get_many / success | 2/2 | 2980.752 | 4204.176 | 4553.634 | 4380.395 | 327.618 | 5060.0 |
| TiKV | get_many / success | 2/2 | 8910.636 | 13209.870 | 14537.592 | 13339.787 | 252.356 | 1750.8 |
| Cassandra | get_many / success | 2/2 | 4054.523 | 14389.197 | 18826.200 | 14540.896 | 287.716 | 3206.2 |
| FoundationDB | get_many / success | 2/2 | 12779.200 | 15492.326 | 16041.460 | 15626.610 | 283.172 | 1225.9 |

## primary_rf1 / read-many32 / 65536 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | get_many / success | 2/2 | 1693.651 | 52297.279 | 54127.923 | 56011.693 | 53676.572 | 2106.0 |
| TH TCP | get_many / success | 2/2 | 26585.837 | 67243.369 | 87871.886 | 68125.110 | 1371.463 | 527.2 |
| TH EFA | get_many / success | 2/2 | 20471.919 | 51764.700 | 68630.976 | 52694.962 | 1414.095 | 678.7 |
| TH EFA-direct | get_many / success | 2/2 | 22823.837 | 55620.615 | 74978.948 | 56524.518 | 1390.189 | 618.5 |
| TH EFA-native | get_many / success | 2/2 | 14683.634 | 35599.954 | 45431.061 | 36538.216 | 1439.185 | 939.4 |
| TiKV | get_many / success | 2/2 | 16217.071 | 24418.020 | 27308.425 | 25334.512 | 1636.484 | 909.3 |
| Cassandra | get_many / success | 2/2 | 10022.701 | 32318.374 | 42089.758 | 33320.848 | 1802.523 | 1371.6 |
| FoundationDB | get_many / success | 2/2 | 25529.833 | 35495.369 | 40193.082 | 36634.846 | 1895.486 | 612.4 |

## primary_rf1 / write-concurrent / 64 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | put / success | 2/2 | 857.816 | 3160.256 | 4423.481 | 3172.586 | 35.886 | 15258.3 |
| TH TCP | put / success | 2/2 | 1197.380 | 144638.489 | 154708.372 | 144652.759 | 39.221 | 4187.9 |
| TH EFA | put / success | 2/2 | 892.937 | 142925.918 | 144541.681 | 142944.182 | 34.846 | 4494.8 |
| TH EFA-direct | put / success | 2/2 | 883.117 | 143492.590 | 144723.671 | 143508.921 | 33.196 | 4154.5 |
| TH EFA-native | put / success | 2/2 | 873.646 | 143579.156 | 153527.554 | 143602.768 | 34.141 | 4108.5 |
| TiKV | put / success | 2/2 | 516.397 | 855.725 | 1636.389 | 870.305 | 30.171 | 28163.0 |
| Cassandra | put / success | 2/2 | 356.379 | 1112.097 | 2242.309 | 1162.018 | 34.096 | 37225.5 |
| FoundationDB | put / success | 2/2 | 964.508 | 1719.687 | 2039.450 | 1738.772 | 37.331 | 14910.3 |

## primary_rf1 / write-concurrent / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | put / success | 2/2 | 832.895 | 3818.538 | 5199.716 | 3838.233 | 38.861 | 15119.6 |
| TH TCP | put / success | 2/2 | 1197.113 | 143947.149 | 146355.932 | 143960.175 | 36.166 | 3902.4 |
| TH EFA | put / success | 2/2 | 958.069 | 143229.666 | 144186.285 | 143243.617 | 34.086 | 4134.0 |
| TH EFA-direct | put / success | 2/2 | 957.698 | 143212.441 | 144406.905 | 143231.036 | 34.740 | 4129.7 |
| TH EFA-native | put / success | 2/2 | 924.022 | 143441.230 | 144343.132 | 143457.095 | 33.896 | 4123.5 |
| TiKV | put / success | 2/2 | 499.927 | 904.192 | 1693.331 | 916.503 | 33.245 | 28815.1 |
| Cassandra | put / success | 2/2 | 361.449 | 686.951 | 1556.803 | 698.226 | 32.446 | 37269.6 |
| FoundationDB | put / success | 2/2 | 964.683 | 1584.113 | 1853.225 | 1629.155 | 38.361 | 15477.3 |

## primary_rf1 / write-concurrent / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | put / success | 2/2 | 910.692 | 4481.262 | 6680.306 | 4515.168 | 38.195 | 12748.8 |
| TH TCP | put / success | 2/2 | 1308.132 | 143719.595 | 145953.143 | 143740.365 | 40.121 | 3802.2 |
| TH EFA | put / success | 2/2 | 968.353 | 143377.036 | 151735.878 | 143393.057 | 35.366 | 4092.9 |
| TH EFA-direct | put / success | 2/2 | 988.269 | 142871.663 | 144143.504 | 142884.339 | 36.011 | 4116.3 |
| TH EFA-native | put / success | 2/2 | 993.813 | 143006.850 | 151164.527 | 143020.845 | 34.946 | 4079.4 |
| TiKV | put / success | 2/2 | 628.216 | 1069.386 | 2145.872 | 1084.716 | 34.546 | 23680.2 |
| Cassandra | put / success | 2/2 | 494.697 | 869.295 | 1817.074 | 881.451 | 33.366 | 30450.7 |
| FoundationDB | put / success | 2/2 | 960.293 | 1703.646 | 2229.654 | 1732.172 | 37.471 | 15237.9 |

## primary_rf1 / write-concurrent / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | put / success | 2/2 | 1027.995 | 3411.297 | 4155.165 | 3428.773 | 43.066 | 12593.3 |
| TH TCP | put / success | 2/2 | 1869.165 | 143423.563 | 146419.046 | 143441.859 | 46.286 | 3828.2 |
| TH EFA | put / success | 2/2 | 1449.460 | 143242.665 | 151779.260 | 143263.111 | 41.476 | 3676.6 |
| TH EFA-direct | put / success | 2/2 | 1490.816 | 143856.857 | 144989.546 | 143885.113 | 46.337 | 3649.5 |
| TH EFA-native | put / success | 2/2 | 1386.568 | 143620.552 | 144965.023 | 143638.877 | 47.975 | 3726.4 |
| TiKV | put / success | 2/2 | 785.194 | 1569.557 | 2204.084 | 1586.538 | 41.206 | 18308.0 |
| Cassandra | put / success | 2/2 | 698.096 | 1217.320 | 2507.399 | 1254.165 | 40.021 | 20872.2 |
| FoundationDB | put / success | 2/2 | 1103.127 | 2094.356 | 2454.465 | 2117.841 | 56.731 | 12231.4 |

## primary_rf1 / write-concurrent / 65536 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | put / success | 2/2 | 3657.654 | 7757.968 | 10233.967 | 7818.559 | 114.547 | 4119.5 |
| TH TCP | put / success | 2/2 | 4306.095 | 8990.288 | 143990.503 | 9044.714 | 124.603 | 2889.8 |
| TH EFA | put / success | 2/2 | 4183.941 | 7956.852 | 143956.383 | 8005.409 | 119.043 | 3102.8 |
| TH EFA-direct | put / success | 2/2 | 4629.497 | 8549.317 | 143130.905 | 8596.668 | 123.668 | 2752.7 |
| TH EFA-native | put / success | 2/2 | 4329.975 | 8205.238 | 145887.967 | 8265.824 | 122.633 | 2878.1 |
| TiKV | put / success | 2/2 | 1233.885 | 4859.257 | 8320.111 | 4897.928 | 76.412 | 10956.0 |
| Cassandra | put / success | 2/2 | 1313.716 | 3283.240 | 17827.185 | 3329.199 | 91.227 | 10555.6 |
| FoundationDB | put / success | 2/2 | 3042.043 | 5643.411 | 7076.796 | 5707.883 | 115.012 | 4848.2 |

## primary_rf1 / zipf-read / 1024 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Native | read / success | 2/2 | 1.995 | 3.545 | 20.671 | 27.581 | 25.050 | 134662.1 |
| TH TCP | read / success | 2/2 | 177.539 | 599.479 | 873.390 | 619.380 | 33.246 | 65775.6 |
| TH EFA | read / success | 2/2 | 84.522 | 296.607 | 417.705 | 311.648 | 29.296 | 112421.9 |
| TH EFA-direct | read / success | 2/2 | 83.082 | 289.982 | 425.175 | 305.553 | 29.836 | 114877.3 |
| TH EFA-native | read / success | 2/2 | 76.082 | 273.266 | 396.659 | 287.327 | 28.201 | 119326.8 |
| TiKV | read / success | 2/2 | 389.325 | 698.992 | 1375.918 | 713.332 | 34.606 | 36786.3 |
| Cassandra | read / success | 2/2 | 330.398 | 534.688 | 1580.117 | 552.242 | 31.806 | 45043.7 |
| FoundationDB | read / success | 2/2 | 634.796 | 1050.055 | 1258.035 | 1064.490 | 32.411 | 23268.4 |

## registered_ram_configured / registered-ram / 256 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TH EFA-native | read / success | 2/2 | 46.761 | 416.770 | 494.147 | 429.640 | 23.536 | 7285.9 |

## registered_ram_configured / registered-ram / 8192 bytes
| System | Operation / outcome | Repetitions | API p50 µs | API p99 µs | API p999 µs | Response p99 µs | Scheduling p99 µs | Population calls/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| TH EFA-native | read / success | 2/2 | 52.116 | 427.970 | 459.246 | 441.880 | 23.625 | 6976.0 |

## Planned-case coverage

| System | Completed | Failed | Not run |
|---|---:|---:|---:|
| Native | 70 | 0 | 35 |
| TH TCP | 58 | 0 | 29 |
| TH EFA | 86 | 1 | 0 |
| TH EFA-direct | 58 | 0 | 29 |
| TH EFA-native | 62 | 0 | 31 |
| TiKV | 50 | 0 | 25 |
| Cassandra | 50 | 0 | 25 |
| FoundationDB | 75 | 0 | 0 |
