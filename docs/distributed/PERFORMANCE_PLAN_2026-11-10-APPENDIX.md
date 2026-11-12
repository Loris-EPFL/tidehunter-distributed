# Performance plan appendix

Run 8, fleet `bench-0a8a6af114a64287`; the operation table lists contracts and repetitions in each row. Values are medians of per-repetition statistics, not pooled calls. Native uses one process and one SSD; Tidehunter uses five owners. Native barriers sync every registered Value-WAL file and the directory per put; owners acknowledge after a covering sync. The 64 KiB c16 Native barrier result is consistent with a one-barrier-per-put model [INFERENCE], but its cause is not isolated. Native cold reads use mapped recent fragments and positional reads for older fragments; the 8 KiB case passes the device-count gate but has memory-like latency. Window counters do not prove individual NAND access.

## Full operation table

| ID | Route | Shape | Size | c | Contract | Cache | n | Reps | Mean µs | p50 µs | p99 µs | p99.9 µs | p50 range µs | Reads/call | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R852b2f71 | hybrid SPDK | put | 1 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 130.8 | 125.3 | 271.0 | 456.2 | not measured | not measured | qualified 1/1 |
| R08b1e259 | hybrid SPDK | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 129.1 | 122.8 | 243.8 | 427.7 | 120.7 to 124.9 | not measured | qualified 2/2 |
| R8c6b50b8 | hybrid SPDK | read | 1 KiB | 1 | synchronous; read profile | cold | 2048 | 1 | 101.9 | 108.9 | 132.3 | 249.3 | not measured | 1.18 | qualified 1/1 |
| Re7acf834 | hybrid kernel DIRECT | put | 1 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 132.8 | 128.2 | 272.8 | 442.2 | not measured | not measured | qualified 1/1 |
| Rd6fbe26d | hybrid kernel DIRECT | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 125.1 | 119.3 | 228.5 | 365.5 | 119.1 to 119.5 | not measured | qualified 2/2 |
| Ra8cafbba | hybrid kernel DIRECT | read | 1 KiB | 1 | synchronous; read profile | cold | 2048 | 1 | 112.7 | 120.4 | 145.7 | 317.1 | not measured | 1.18 | qualified 1/1 |
| R9c9bedb1 | hybrid kernel buffered | read | 1 KiB | 1 | synchronous; read profile | memory | 4096 | 1 | 42.6 | 42.1 | 52.0 | 183.4 | not measured | 0.00 | qualified 1/1 |
| R0daebf6d | native | delete | 1 KiB | 1 | page_cache; unprofiled | not measured | 4096 | 1 | 2.0 | 1.9 | 4.2 | 8.4 | not measured | not measured | qualified 1/1 |
| R409e1b71 | native | delete | 1 KiB | 1 | value_wal_barrier; write profile | not measured | 4096 | 1 | 29.8 | 26.5 | 79.8 | 103.0 | not measured | not measured | qualified 1/1 |
| R92629de8 | native | delete | 1 KiB | 1 | value_wal_barrier; unprofiled | not measured | 4096 | 1 | 30.8 | 27.5 | 81.9 | 90.5 | not measured | not measured | qualified 1/1 |
| R12350055 | native | put | 1 KiB | 1 | page_cache; write profile | not measured | 4096 | 1 | 2.3 | 2.2 | 4.5 | 8.9 | not measured | not measured | qualified 1/1 |
| R5c5502ba | native | put | 1 KiB | 1 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 2.3 | 2.2 | 4.5 | 9.5 | 2.2 to 2.2 | not measured | qualified 2/2 |
| R0f7a4fa4 | native | put | 1 KiB | 1 | value_wal_barrier; new; unprofiled | not measured | 4096 + 4096 | 2 | 31.3 | 27.2 | 84.1 | 99.3 | 26.8 to 27.6 | not measured | qualified 2/2 |
| Ra056169f | native | put | 1 KiB | 1 | value_wal_barrier; write profile | not measured | 4096 | 1 | 31.6 | 27.7 | 83.2 | 95.0 | not measured | not measured | qualified 1/1 |
| Rc15b54b0 | native | put | 1 KiB | 1 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 32.7 | 27.3 | 174.7 | 192.9 | 27.0 to 27.5 | not measured | qualified 2/2 |
| R10f237bf | native | put_batch | 1 KiB | 1 | page_cache; unprofiled | not measured | 1024 | 1 | 38.7 | 37.9 | 50.6 | 53.4 | not measured | not measured | qualified 1/1 |
| R12e5f6b2 | native | put_batch | 1 KiB | 1 | value_wal_barrier; unprofiled | not measured | 1024 | 1 | 114.5 | 105.6 | 167.4 | 315.8 | not measured | not measured | qualified 1/1 |
| Rc2cbb87b | native | read | 1 KiB | 1 | value_wal_barrier; unprofiled | cold | 2048 + 2048 | 2 | not measured | not measured | not measured | not measured | not measured | not measured | unqualified 2/2 |
| Re116d570 | owner TCP | delete | 1 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 131.8 | 128.1 | 207.5 | 426.9 | not measured | not measured | qualified 1/1 |
| R361477d0 | owner TCP | put | 1 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 85.5 | 81.5 | 128.8 | 331.3 | 81.2 to 81.8 | not measured | qualified 2/2 |
| R119f59bc | owner TCP | put | 1 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 144.3 | 139.0 | 310.2 | 603.0 | not measured | not measured | qualified 1/1 |
| R27cd3fe5 | owner TCP | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 130.8 | 128.5 | 176.0 | 304.5 | 127.8 to 129.1 | not measured | qualified 2/2 |
| Rc87b6aea | owner TCP | read | 1 KiB | 1 | synchronous; read profile | cold | 2048 | 1 | 115.8 | 122.5 | 148.3 | 337.6 | not measured | 0.96 | qualified 1/1 |
| R1ec39309 | owner TCP | read | 1 KiB | 1 | synchronous; read profile | memory | 4096 | 1 | 46.8 | 46.5 | 56.9 | 122.4 | not measured | 0.00 | qualified 1/1 |
| Rc0228526 | owner verbs | delete | 1 KiB | 1 | native; unprofiled | not measured | 4096 | 1 | 71.2 | 66.7 | 132.7 | 380.0 | not measured | not measured | qualified 1/1 |
| R2a0028ea | owner verbs | delete | 1 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 124.1 | 120.2 | 194.1 | 442.1 | not measured | not measured | qualified 1/1 |
| Rf6f6c07f | owner verbs | delete | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 | 1 | 122.4 | 114.2 | 287.2 | 426.8 | not measured | not measured | qualified 1/1 |
| Radbfd0ca | owner verbs | put | 1 KiB | 1 | native; write timeline | not measured | 4096 | 1 | 84.2 | 80.3 | 132.8 | 346.8 | not measured | not measured | qualified 1/1 |
| Rb14099c4 | owner verbs | put | 1 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 78.2 | 74.4 | 127.4 | 317.0 | 73.5 to 75.3 | not measured | qualified 2/2 |
| R08941181 | owner verbs | put | 1 KiB | 1 | synchronous; new; unprofiled | not measured | 4096 + 4096 | 2 | 124.6 | 119.4 | 229.7 | 379.7 | 116.8 to 122.0 | not measured | qualified 2/2 |
| R40880520 | owner verbs | put | 1 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 134.1 | 125.5 | 306.1 | 470.9 | not measured | not measured | qualified 1/1 |
| R0a5ba9da | owner verbs | put | 1 KiB | 1 | synchronous; write profile | not measured | 4096 | 1 | 129.8 | 127.9 | 199.3 | 362.5 | not measured | not measured | qualified 1/1 |
| R63036dd4 | owner verbs | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 126.6 | 121.5 | 221.4 | 440.2 | 120.8 to 122.2 | not measured | qualified 2/2 |
| Rf279f18f | owner verbs | put_batch | 1 KiB | 1 | native; unprofiled | not measured | 1024 | 1 | 284.7 | 273.5 | 402.4 | 564.0 | not measured | not measured | qualified 1/1 |
| R8a2dc242 | owner verbs | put_batch | 1 KiB | 1 | synchronous; unprofiled | not measured | 1024 | 1 | 413.2 | 407.3 | 495.0 | 703.7 | not measured | not measured | qualified 1/1 |
| Re3876655 | owner verbs | read | 1 KiB | 1 | synchronous; read profile | cold | 2048 | 1 | 109.3 | 116.0 | 142.0 | 332.0 | not measured | 0.96 | qualified 1/1 |
| R035b1e82 | owner verbs | read | 1 KiB | 1 | synchronous; read profile | memory | 4096 | 1 | 38.1 | 37.5 | 49.1 | 106.1 | not measured | 0.00 | qualified 1/1 |
| R4ce90626 | verbs mTLS | put | 1 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 132.9 | 130.5 | 180.9 | 370.9 | not measured | not measured | qualified 1/1 |
| Rc2292fda | verbs mTLS | put | 1 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 125.3 | 122.8 | 174.7 | 345.0 | 122.5 to 123.0 | not measured | qualified 2/2 |
| R4b6b43bd | verbs mTLS | read | 1 KiB | 1 | synchronous; read profile | cold | 2048 | 1 | 109.8 | 116.0 | 139.5 | 262.7 | not measured | 0.96 | qualified 1/1 |
| R79431f2a | verbs mTLS | read | 1 KiB | 1 | synchronous; read profile | memory | 4096 | 1 | 38.4 | 37.4 | 49.6 | 105.2 | not measured | 0.00 | qualified 1/1 |
| Rffe00a2c | hybrid SPDK | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 333.4 | 335.0 | 605.8 | 830.9 | 332.8 to 337.2 | not measured | qualified 2/2 |
| Re69f96c6 | hybrid SPDK | read | 1 KiB | 16 | synchronous; read profile | cold | 2048 | 1 | 117.0 | 114.9 | 317.8 | 1247.3 | not measured | 1.18 | qualified 1/1 |
| Ra2ce76c9 | hybrid kernel DIRECT | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 336.1 | 330.2 | 631.6 | 751.3 | 324.5 to 336.0 | not measured | qualified 2/2 |
| R957ce20c | hybrid kernel DIRECT | read | 1 KiB | 16 | synchronous; read profile | cold | 2048 | 1 | 118.6 | 117.7 | 409.3 | 727.6 | not measured | 1.18 | qualified 1/1 |
| R7a263dd8 | native | delete | 1 KiB | 16 | page_cache; unprofiled | not measured | 4096 | 1 | 3.0 | 2.8 | 5.8 | 23.4 | not measured | not measured | qualified 1/1 |
| R4697e418 | native | delete | 1 KiB | 16 | value_wal_barrier; unprofiled | not measured | 4096 | 1 | 96.2 | 89.5 | 218.2 | 276.5 | not measured | not measured | qualified 1/1 |
| Ree7b782d | native | put | 1 KiB | 16 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 3.3 | 3.1 | 6.9 | 26.2 | 3.1 to 3.1 | not measured | qualified 2/2 |
| R58ebf028 | native | put | 1 KiB | 16 | value_wal_barrier; new; unprofiled | not measured | 4096 + 4096 | 2 | 71.0 | 64.0 | 184.4 | 286.7 | 63.9 to 64.2 | not measured | qualified 2/2 |
| R8d29f5b7 | native | put | 1 KiB | 16 | value_wal_barrier; write profile | not measured | 4096 | 1 | 72.4 | 65.3 | 180.3 | 389.3 | not measured | not measured | qualified 1/1 |
| Rf1572aad | native | put | 1 KiB | 16 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 70.5 | 64.1 | 169.5 | 279.6 | 63.2 to 65.1 | not measured | qualified 2/2 |
| R166d37d0 | native | put_batch | 1 KiB | 16 | page_cache; unprofiled | not measured | 1024 | 1 | 182.1 | 178.2 | 388.6 | 455.6 | not measured | not measured | qualified 1/1 |
| R6ed9c0dc | native | put_batch | 1 KiB | 16 | value_wal_barrier; unprofiled | not measured | 1024 | 1 | 1107.4 | 1112.1 | 1305.8 | 1334.8 | not measured | not measured | qualified 1/1 |
| R565f3b20 | native | read | 1 KiB | 16 | value_wal_barrier; unprofiled | cold | 2048 | 1 | not measured | not measured | not measured | not measured | not measured | not measured | unqualified 1/1 |
| R2301b08a | owner TCP | put | 1 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 423.2 | 326.0 | 645.5 | 26944.0 | 315.3 to 336.8 | not measured | qualified 2/2 |
| Rc9cfaa51 | owner TCP | put | 1 KiB | 16 | synchronous; write timeline | not measured | 4096 | 1 | 461.9 | 461.8 | 806.7 | 1015.0 | not measured | not measured | qualified 1/1 |
| R64d5dda3 | owner TCP | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 336.1 | 344.1 | 556.9 | 743.6 | 342.0 to 346.1 | not measured | qualified 2/2 |
| R3b547898 | owner TCP | read | 1 KiB | 16 | synchronous; read profile | cold | 2048 | 1 | 152.4 | 142.4 | 476.8 | 1645.4 | not measured | 0.96 | qualified 1/1 |
| Ref712b7f | owner TCP | read | 1 KiB | 16 | synchronous; read profile | memory | 4096 | 1 | 63.6 | 58.8 | 138.5 | 433.8 | not measured | 0.00 | qualified 1/1 |
| R3ec6d58f | owner verbs | delete | 1 KiB | 16 | native; unprofiled | not measured | 4096 | 1 | 519.4 | 274.1 | 581.9 | 61548.9 | not measured | not measured | qualified 1/1 |
| R35c69c2b | owner verbs | delete | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 | 1 | 289.4 | 292.6 | 530.6 | 750.3 | not measured | not measured | qualified 1/1 |
| R4533c702 | owner verbs | put | 1 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 514.9 | 324.4 | 733.4 | 49292.8 | 317.7 to 331.1 | not measured | qualified 2/2 |
| R8967c0cd | owner verbs | put | 1 KiB | 16 | synchronous; new; unprofiled | not measured | 4096 + 4096 | 2 | 356.5 | 353.7 | 627.3 | 807.6 | 348.1 to 359.2 | not measured | qualified 2/2 |
| R51feece2 | owner verbs | put | 1 KiB | 16 | synchronous; new; write timeline | not measured | 200000 | 1 | 1399.9 | 595.3 | 1083.3 | 171028.5 | not measured | not measured | qualified 1/1 |
| R8bfe5b29 | owner verbs | put | 1 KiB | 16 | synchronous; new; write profile | not measured | 200000 | 1 | 1396.4 | 565.2 | 1029.8 | 177579.0 | not measured | not measured | qualified 1/1 |
| R0e7cd84c | owner verbs | put | 1 KiB | 16 | synchronous; write profile | not measured | 4096 | 1 | 322.8 | 330.2 | 537.1 | 746.2 | not measured | not measured | qualified 1/1 |
| R1553ed29 | owner verbs | put | 1 KiB | 16 | synchronous; write timeline | not measured | 4096 | 1 | 420.9 | 420.0 | 774.3 | 918.7 | not measured | not measured | qualified 1/1 |
| R92c4c91e | owner verbs | put | 1 KiB | 16 | synchronous; write timeline | not measured | 200000 | 1 | 1086.6 | 575.3 | 1104.6 | 110802.1 | not measured | not measured | qualified 1/1 |
| R17a988c9 | owner verbs | put | 1 KiB | 16 | synchronous; write profile | not measured | 200000 | 1 | 1081.2 | 545.7 | 1033.5 | 114937.1 | not measured | not measured | qualified 1/1 |
| Rb42eacfc | owner verbs | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 361.4 | 360.4 | 634.0 | 869.5 | 355.0 to 365.7 | not measured | qualified 2/2 |
| R3067be9e | owner verbs | put_batch | 1 KiB | 16 | native; unprofiled | not measured | 1024 | 1 | 2208.3 | 2295.7 | 3148.4 | 3913.5 | not measured | not measured | qualified 1/1 |
| Ra39565f2 | owner verbs | put_batch | 1 KiB | 16 | synchronous; unprofiled | not measured | 1024 | 1 | 2453.2 | 2558.6 | 3887.2 | 4295.6 | not measured | not measured | qualified 1/1 |
| Rc11b3801 | owner verbs | read | 1 KiB | 16 | synchronous; read profile | cold | 2048 | 1 | 131.7 | 124.8 | 385.8 | 498.9 | not measured | 0.95 | qualified 1/1 |
| R97e5b4fa | owner verbs | read | 1 KiB | 16 | synchronous; read profile | memory | 4096 | 1 | 46.9 | 44.0 | 114.7 | 238.9 | not measured | 0.00 | qualified 1/1 |
| R5f6d119c | verbs mTLS | put | 1 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 334.3 | 336.5 | 543.8 | 816.5 | 333.2 to 339.7 | not measured | qualified 2/2 |
| R292ec93e | native | put | 8 KiB | 1 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 3.4 | 3.2 | 5.8 | 12.3 | 3.2 to 3.3 | not measured | qualified 2/2 |
| Ra466d20c | native | put | 8 KiB | 1 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 43.1 | 38.9 | 96.6 | 113.8 | 38.6 to 39.1 | not measured | qualified 2/2 |
| Rda9d8374 | native | put_batch | 8 KiB | 1 | page_cache; unprofiled | not measured | 1024 | 1 | 284.9 | 224.5 | 591.9 | 623.1 | not measured | not measured | qualified 1/1 |
| R49d0c784 | native | put_batch | 8 KiB | 1 | value_wal_barrier; unprofiled | not measured | 1024 | 1 | 449.8 | 432.7 | 614.8 | 632.5 | not measured | not measured | qualified 1/1 |
| R436ed9cf | native | read | 8 KiB | 1 | value_wal_barrier; unprofiled | cold | 4096 + 4096 | 2 | 35.7 | 4.7 | 963.9 | 1009.3 | 4.7 to 4.8 | 1.03 | passes device-count gate; memory-like latency |
| R6bad39f9 | native | read | 8 KiB | 1 | value_wal_barrier; unprofiled | memory | 8192 + 8192 | 2 | 1.8 | 1.9 | 2.8 | 8.3 | 1.8 to 1.9 | not measured | qualified 2/2 |
| Rac31c3a7 | owner TCP | put_batch | 8 KiB | 1 | synchronous; write timeline | not measured | 1024 | 1 | 1391.3 | 1372.4 | 1626.4 | 1827.6 | not measured | not measured | qualified 1/1 |
| Rac03e4ee | owner verbs | put | 8 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 104.8 | 100.7 | 152.6 | 356.2 | 100.3 to 101.1 | not measured | qualified 2/2 |
| Raf0d7639 | owner verbs | put | 8 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 229.1 | 226.8 | 291.2 | 566.7 | 220.8 to 232.7 | not measured | qualified 2/2 |
| R598ad5f1 | owner verbs | put_batch | 8 KiB | 1 | native; unprofiled | not measured | 1024 | 1 | 966.3 | 939.3 | 1345.6 | 2260.2 | not measured | not measured | qualified 1/1 |
| R3e9799a7 | owner verbs | put_batch | 8 KiB | 1 | synchronous; write timeline | not measured | 1024 | 1 | 1319.5 | 1265.0 | 1749.1 | 1920.0 | not measured | not measured | qualified 1/1 |
| R71f25999 | owner verbs | put_batch | 8 KiB | 1 | synchronous; unprofiled | not measured | 1024 | 1 | 1318.4 | 1274.3 | 1651.9 | 1949.2 | not measured | not measured | qualified 1/1 |
| R886717d2 | native | put | 8 KiB | 16 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 4.2 | 4.1 | 9.3 | 31.1 | 4.0 to 4.1 | not measured | qualified 2/2 |
| Re10469a9 | native | put | 8 KiB | 16 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 199.2 | 194.7 | 407.3 | 471.6 | 192.1 to 197.3 | not measured | qualified 2/2 |
| Rfaf05629 | native | put_batch | 8 KiB | 16 | page_cache; unprofiled | not measured | 1024 | 1 | 1091.0 | 876.7 | 2438.2 | 30149.3 | not measured | not measured | qualified 1/1 |
| Rd6321af3 | native | put_batch | 8 KiB | 16 | value_wal_barrier; unprofiled | not measured | 1024 | 1 | 10918.4 | 10865.2 | 13748.1 | 13789.8 | not measured | not measured | qualified 1/1 |
| R65a80e76 | native | read | 8 KiB | 16 | value_wal_barrier; unprofiled | memory | 8192 + 8192 | 2 | 2.6 | 2.6 | 4.1 | 12.9 | 2.5 to 2.6 | not measured | qualified 2/2 |
| R70852372 | owner verbs | put | 8 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 607.4 | 463.7 | 984.9 | 37063.5 | 463.3 to 464.1 | not measured | qualified 2/2 |
| R7e542275 | owner verbs | put | 8 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 482.3 | 463.2 | 960.1 | 1256.0 | 459.1 to 467.3 | not measured | qualified 2/2 |
| R64025107 | owner verbs | put_batch | 8 KiB | 16 | native; unprofiled | not measured | 1024 | 1 | 7399.9 | 7024.1 | 12691.9 | 12739.5 | not measured | not measured | qualified 1/1 |
| R752a0ff1 | owner verbs | put_batch | 8 KiB | 16 | synchronous; unprofiled | not measured | 1024 | 1 | 7507.4 | 6478.5 | 15888.4 | 15965.3 | not measured | not measured | qualified 1/1 |
| R9c0dae5c | hybrid SPDK | read | 64 KiB | 1 | synchronous; read profile | cold | 4096 | 1 | 139.8 | 140.6 | 157.7 | 259.0 | not measured | 1.09 | qualified 1/1 |
| R6d2332f6 | hybrid kernel DIRECT | read | 64 KiB | 1 | synchronous; read profile | cold | 4096 | 1 | 147.8 | 147.3 | 174.6 | 484.9 | not measured | 1.09 | qualified 1/1 |
| R121f66e4 | hybrid kernel buffered | read | 64 KiB | 1 | synchronous; read profile | memory | 4096 | 1 | 65.6 | 65.0 | 77.1 | 248.3 | not measured | 0.00 | qualified 1/1 |
| Rd4328490 | native | put | 64 KiB | 1 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 54.4 | 41.1 | 114.9 | 134.1 | 41.1 to 41.2 | not measured | qualified 2/2 |
| Rc7e58ea0 | native | put | 64 KiB | 1 | value_wal_barrier; write profile | not measured | 4096 | 1 | 159.5 | 154.8 | 217.6 | 277.8 | not measured | not measured | qualified 1/1 |
| R7f945823 | native | put | 64 KiB | 1 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 161.1 | 157.2 | 211.8 | 226.4 | 157.1 to 157.3 | not measured | qualified 2/2 |
| R78233185 | native | read | 64 KiB | 1 | value_wal_barrier; unprofiled | cold | 4096 + 4096 | 2 | 160.6 | 102.1 | 1004.9 | 1055.7 | 101.9 to 102.3 | 3.72 | qualified 2/2 |
| R09e69537 | native | read | 64 KiB | 1 | value_wal_barrier; unprofiled | memory | 8192 + 8192 | 2 | 10.6 | 11.0 | 16.0 | 19.9 | 11.0 to 11.1 | not measured | qualified 2/2 |
| Rbf0c94da | owner TCP | put | 64 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 328.2 | 319.3 | 466.0 | 1198.7 | 319.1 to 319.6 | not measured | qualified 2/2 |
| R0d1a0ee3 | owner TCP | put | 64 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 499.9 | 494.6 | 604.8 | 757.8 | not measured | not measured | qualified 1/1 |
| R13eea891 | owner TCP | put | 64 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 495.6 | 490.3 | 591.1 | 760.3 | 488.7 to 491.9 | not measured | qualified 2/2 |
| R63bb4082 | owner TCP | read | 64 KiB | 1 | synchronous; read profile | cold | 4096 | 1 | 206.1 | 207.1 | 237.9 | 729.3 | not measured | 1.01 | qualified 1/1 |
| R7e4bd14c | owner TCP | read | 64 KiB | 1 | synchronous; read profile | memory | 4096 | 1 | 110.5 | 109.2 | 128.2 | 364.8 | not measured | 0.00 | qualified 1/1 |
| R9a805863 | owner verbs | put | 64 KiB | 1 | native; unprofiled | not measured | 4096 + 4096 | 2 | 284.0 | 275.2 | 396.8 | 1017.9 | 273.6 to 276.8 | not measured | qualified 2/2 |
| R6dbb1b55 | owner verbs | put | 64 KiB | 1 | synchronous; write timeline | not measured | 4096 | 1 | 462.5 | 450.6 | 724.6 | 956.3 | not measured | not measured | qualified 1/1 |
| R4afa3c44 | owner verbs | put | 64 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 445.8 | 441.4 | 537.4 | 735.0 | 438.6 to 444.2 | not measured | qualified 2/2 |
| R870d07f8 | owner verbs | read | 64 KiB | 1 | synchronous; read profile | cold | 4096 | 1 | 159.5 | 161.4 | 193.2 | 387.5 | not measured | 1.01 | qualified 1/1 |
| R5cdd5f46 | owner verbs | read | 64 KiB | 1 | synchronous; read profile | memory | 4096 | 1 | 68.5 | 68.0 | 80.8 | 140.5 | not measured | 0.00 | qualified 1/1 |
| R0a1f9571 | verbs mTLS | put | 64 KiB | 1 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 471.9 | 466.6 | 572.3 | 701.0 | 463.5 to 469.7 | not measured | qualified 2/2 |
| R502435c7 | hybrid SPDK | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1774.0 | 1646.2 | 4382.9 | 4883.2 | 1619.7 to 1672.6 | not measured | qualified 2/2 |
| R9bbfa53e | hybrid kernel DIRECT | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1760.5 | 1642.5 | 3914.0 | 4591.8 | 1638.4 to 1646.6 | not measured | qualified 2/2 |
| R0315682c | native | put | 64 KiB | 16 | page_cache; unprofiled | not measured | 4096 + 4096 | 2 | 169.2 | 165.7 | 291.9 | 499.1 | 164.2 to 167.3 | not measured | qualified 2/2 |
| R532834ed | native | put | 64 KiB | 16 | value_wal_barrier; new; unprofiled | not measured | 4096 + 4096 | 2 | 2675.4 | 2913.3 | 3421.6 | 3497.6 | 2890.8 to 2935.8 | not measured | qualified 2/2 |
| R7ebbb903 | native | put | 64 KiB | 16 | value_wal_barrier; unprofiled | not measured | 4096 + 4096 | 2 | 2686.8 | 2899.2 | 3400.0 | 3491.0 | 2867.4 to 2930.9 | not measured | qualified 2/2 |
| Rf2eb0bc0 | native | read | 64 KiB | 16 | value_wal_barrier; unprofiled | cold | 4096 | 1 | 1034.0 | 345.9 | 9316.6 | 14959.2 | not measured | 3.76 | qualified 1/1 |
| R2eb25424 | native | read | 64 KiB | 16 | value_wal_barrier; unprofiled | memory | 8192 + 8192 | 2 | 15.3 | 15.4 | 25.4 | 34.9 | 15.3 to 15.4 | not measured | qualified 2/2 |
| R1f5a546a | owner TCP | put | 64 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 1642.6 | 1567.7 | 3080.5 | 3900.6 | 1537.1 to 1598.2 | not measured | qualified 2/2 |
| R9d90d5c8 | owner TCP | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1745.9 | 1666.0 | 3722.7 | 4962.6 | 1641.8 to 1690.1 | not measured | qualified 2/2 |
| Ra973e337 | owner verbs | put | 64 KiB | 16 | native; unprofiled | not measured | 4096 + 4096 | 2 | 1608.7 | 1501.2 | 2991.9 | 4021.4 | 1470.8 to 1531.7 | not measured | qualified 2/2 |
| Rbdb65963 | owner verbs | put | 64 KiB | 16 | synchronous; new; unprofiled | not measured | 4096 + 4096 | 2 | 1715.4 | 1634.3 | 3466.7 | 4164.1 | 1608.4 to 1660.2 | not measured | qualified 2/2 |
| R2ec0e6ef | owner verbs | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1707.6 | 1568.2 | 3593.4 | 5225.2 | 1564.1 to 1572.2 | not measured | qualified 2/2 |
| R75e726d0 | verbs mTLS | put | 64 KiB | 16 | synchronous; unprofiled | not measured | 4096 + 4096 | 2 | 1706.8 | 1622.8 | 3442.5 | 4281.2 | 1607.3 to 1638.4 | not measured | qualified 2/2 |

## Run 7 sustained puts by repetition

Fleet `bench-6e40ca83d74146cf`; route and durability are in each row.

| ID | Route | Durability | Repeat | n | p99.9 µs | First second puts/s | Last five complete one-second buckets puts/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| R167b0400 | owner TCP | native | 1 | 200000 | 127922.5 | 23035 | 10854, 11680, 10303, 10462, 8864 |
| R89f02779 | owner verbs | synchronous | 2 | 200000 | 118578.1 | 24988 | 11630, 10659, 10473, 10358, 9817 |
| R7b117213 | owner verbs | synchronous | 0 | 200000 | 116099.4 | 24819 | 11779, 10850, 10436, 10311, 9764 |
| R312d7193 | owner verbs | native | 1 | 200000 | 115816.5 | 24237 | 10597, 11029, 10641, 10557, 9207 |
| R1228e665 | owner verbs | synchronous | 1 | 200000 | 126480.4 | 25072 | 10438, 11730, 10411, 10216, 8795 |
| R27878092 | owner verbs | native | 2 | 200000 | 122090.5 | 23181 | 10962, 11771, 10009, 9989, 9181 |
| Rba0117af | owner verbs | native | 0 | 200000 | 118609.3 | 22634 | 10269, 10456, 10247, 10461, 10138 |
| R350a9e07 | owner TCP | native | 0 | 200000 | 127784.9 | 23508 | 10520, 10248, 10685, 10048, 10488 |
| R8bbcd2a8 | owner TCP | native | 2 | 200000 | 120054.0 | 23060 | 11573, 10543, 10080, 9477, 10227 |
| Re3e4a400 | owner TCP | synchronous | 1 | 200000 | 118610.1 | 25085 | 10434, 10504, 10133, 10559, 8987 |
| Rc5bc5a74 | owner TCP | synchronous | 0 | 200000 | 116591.1 | 24435 | 10473, 10244, 10457, 9293, 9338 |
| Ra95f9a3f | owner TCP | synchronous | 2 | 200000 | 115892.6 | 24302 | 10939, 10368, 9843, 9276, 10117 |

## Network floor repetitions

Run 8, fleet `bench-0a8a6af114a64287`; `ib_send_lat` and `ib_write_lat` record half round trip and are doubled for RTT; `ib_read_lat` records round trip. Missing p50 is not zero.

| Tool | Mode | Interface | Size | Repetition | p50 semantics | p50 µs | RTT p50 µs | Row ID |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ib_send_lat | event | rdma0 | 8 B | 1 | half_round_trip | 10.0 | 20.0 | R51acc123 |
| ib_send_lat | event | rdma0 | 8 B | 2 | half_round_trip | 10.2 | 20.5 | R64660f89 |
| ib_send_lat | event | rdma0 | 1 KiB | 0 | half_round_trip | 10.9 | 21.9 | R9375a8af |
| ib_send_lat | event | rdma0 | 1 KiB | 1 | half_round_trip | 11.0 | 21.9 | R51acc123 |
| ib_send_lat | event | rdma0 | 1 KiB | 2 | half_round_trip | 11.2 | 22.4 | R64660f89 |
| ib_write_lat | polled | rdma0 | 64 KiB | 2 | half_round_trip | 11.4 | 22.9 | Rda997408 |
| ib_write_lat | polled | rdma0 | 64 KiB | 1 | half_round_trip | 11.6 | 23.1 | R9359c862 |
| ib_send_lat | polled | rdma0 | 64 KiB | 1 | half_round_trip | 11.6 | 23.1 | Rcf4eec54 |
| fi_pingpong | not measured | rdma0 | 64 KiB | 2 | half_round_trip | not measured | not measured | Rb9e09ce5 |
| fi_pingpong | not measured | rdma0 | 64 KiB | 0 | half_round_trip | not measured | not measured | R8d5ab9c3 |
| fi_pingpong | not measured | rdma0 | 64 KiB | 1 | half_round_trip | not measured | not measured | Rcfbfd90c |
| ib_send_lat | polled | rdma0 | 64 KiB | 0 | half_round_trip | 11.7 | 23.4 | R5714eab9 |
| ib_write_lat | polled | rdma0 | 64 KiB | 0 | half_round_trip | 11.7 | 23.4 | Rd8e71d3a |
| ib_send_lat | polled | rdma0 | 64 KiB | 2 | half_round_trip | 11.7 | 23.4 | R1d11682b |
| ib_send_lat | event | rdma0 | 4 KiB | 0 | half_round_trip | 11.8 | 23.7 | R9375a8af |
| ib_send_lat | event | rdma0 | 4 KiB | 1 | half_round_trip | 11.9 | 23.7 | R51acc123 |
| ib_send_lat | event | rdma0 | 4 KiB | 2 | half_round_trip | 12.1 | 24.2 | R64660f89 |
| ib_read_lat | event | rdma0 | 8 B | 1 | round_trip | 12.3 | 12.3 | R74ba35b3 |
| ib_read_lat | event | rdma0 | 1 KiB | 1 | round_trip | 12.7 | 12.7 | R74ba35b3 |
| ib_read_lat | event | rdma0 | 8 B | 0 | round_trip | 12.8 | 12.8 | R4167fc5f |
| ib_read_lat | event | rdma0 | 8 B | 2 | round_trip | 12.9 | 12.9 | R340c8bef |
| ib_read_lat | event | rdma0 | 1 KiB | 0 | round_trip | 13.0 | 13.0 | R4167fc5f |
| ib_read_lat | event | rdma0 | 1 KiB | 2 | round_trip | 13.2 | 13.2 | R340c8bef |
| ib_read_lat | event | rdma0 | 4 KiB | 1 | round_trip | 13.5 | 13.5 | R74ba35b3 |
| tcp_rtt | busy_poll | eth0 | 8 B | 0 | round_trip | 13.8 | 13.8 | R1cab2a3e |
| ib_read_lat | event | rdma0 | 4 KiB | 2 | round_trip | 13.9 | 13.9 | R340c8bef |
| ib_read_lat | event | rdma0 | 4 KiB | 0 | round_trip | 14.1 | 14.1 | R4167fc5f |
| tcp_rtt | busy_poll | eth0 | 8 B | 1 | round_trip | 14.5 | 14.5 | R83f3ff66 |
| fi_pingpong | not measured | eth0 | 8 B | 1 | half_round_trip | not measured | not measured | Rd1b2e78a |
| fi_pingpong | not measured | eth0 | 8 B | 0 | half_round_trip | not measured | not measured | Re02830d3 |
| tcp_rtt | busy_poll | eth0 | 8 B | 2 | round_trip | 14.9 | 14.9 | Rbeaa90ea |
| tcp_rtt | busy_poll | rdma0 | 8 B | 2 | round_trip | 15.0 | 15.0 | R70342861 |
| tcp_rtt | busy_poll | rdma0 | 8 B | 1 | round_trip | 15.3 | 15.3 | R861de808 |
| fi_pingpong | not measured | eth0 | 8 B | 2 | half_round_trip | not measured | not measured | R79b11805 |
| tcp_rtt | busy_poll | rdma0 | 8 B | 0 | round_trip | 16.0 | 16.0 | R9fdcf4d8 |
| fi_pingpong | not measured | eth0 | 1 KiB | 0 | half_round_trip | not measured | not measured | Re02830d3 |
| ib_read_lat | polled | rdma0 | 64 KiB | 0 | round_trip | 16.0 | 16.0 | Rd8b3bed4 |
| ib_read_lat | polled | rdma0 | 64 KiB | 1 | round_trip | 16.0 | 16.0 | Rb0ac298a |
| ib_read_lat | polled | rdma0 | 64 KiB | 2 | round_trip | 16.0 | 16.0 | R80ca058b |
| fi_pingpong | not measured | eth0 | 1 KiB | 1 | half_round_trip | not measured | not measured | Rd1b2e78a |
| tcp_rtt | busy_poll | eth0 | 1 KiB | 2 | round_trip | 16.5 | 16.5 | Rbeaa90ea |
| fi_pingpong | not measured | eth0 | 1 KiB | 2 | half_round_trip | not measured | not measured | R79b11805 |
| tcp_rtt | busy_poll | rdma0 | 1 KiB | 0 | round_trip | 16.9 | 16.9 | R9fdcf4d8 |
| tcp_rtt | busy_poll | eth0 | 1 KiB | 0 | round_trip | 16.8 | 16.8 | R1cab2a3e |
| tcp_rtt | busy_poll | rdma0 | 1 KiB | 2 | round_trip | 16.9 | 16.9 | R70342861 |
| tcp_rtt | busy_poll | rdma0 | 1 KiB | 1 | round_trip | 17.0 | 17.0 | R861de808 |
| ib_send_lat | event | rdma0 | 64 KiB | 0 | half_round_trip | 17.2 | 34.3 | R9375a8af |
| ib_send_lat | event | rdma0 | 64 KiB | 1 | half_round_trip | 17.2 | 34.4 | R51acc123 |
| tcp_rtt | busy_poll | eth0 | 1 KiB | 1 | round_trip | 17.2 | 17.2 | R83f3ff66 |
| ib_send_lat | event | rdma0 | 64 KiB | 2 | half_round_trip | 17.5 | 34.9 | R64660f89 |
| fi_pingpong | not measured | rdma0 | 64 KiB | 2 | half_round_trip | not measured | not measured | R9d05aefa |
| fi_pingpong | not measured | rdma0 | 64 KiB | 0 | half_round_trip | not measured | not measured | R5b8b8413 |
| fi_pingpong | not measured | rdma0 | 64 KiB | 1 | half_round_trip | not measured | not measured | R0ea5b6ab |
| fi_pingpong | not measured | eth0 | 4 KiB | 0 | half_round_trip | not measured | not measured | Re02830d3 |
| fi_pingpong | not measured | eth0 | 4 KiB | 1 | half_round_trip | not measured | not measured | Rd1b2e78a |
| tcp_rtt | busy_poll | rdma0 | 4 KiB | 0 | round_trip | 19.0 | 19.0 | R9fdcf4d8 |
| tcp_rtt | busy_poll | rdma0 | 4 KiB | 2 | round_trip | 19.2 | 19.2 | R70342861 |
| tcp_rtt | busy_poll | rdma0 | 4 KiB | 1 | round_trip | 19.3 | 19.3 | R861de808 |
| ib_read_lat | event | rdma0 | 64 KiB | 1 | round_trip | 19.6 | 19.6 | R74ba35b3 |
| ib_read_lat | event | rdma0 | 64 KiB | 0 | round_trip | 20.0 | 20.0 | R4167fc5f |
| fi_pingpong | not measured | eth0 | 4 KiB | 2 | half_round_trip | not measured | not measured | R79b11805 |
| ib_read_lat | event | rdma0 | 64 KiB | 2 | round_trip | 20.1 | 20.1 | R340c8bef |
| tcp_rtt | spin | eth0 | 8 B | 0 | round_trip | 21.9 | 21.9 | R3d1f1b64 |
| tcp_rtt | spin | eth0 | 8 B | 2 | round_trip | 22.4 | 22.4 | R5cba1740 |
| tcp_rtt | spin | rdma0 | 8 B | 1 | round_trip | 22.6 | 22.6 | R943133d3 |
| tcp_rtt | spin | eth0 | 8 B | 1 | round_trip | 22.4 | 22.4 | Rf4211383 |
| tcp_rtt | block | eth0 | 8 B | 1 | round_trip | 23.3 | 23.3 | Rf1043759 |
| tcp_rtt | block | eth0 | 8 B | 2 | round_trip | 22.5 | 22.5 | Rdcf9557f |
| tcp_rtt | spin | rdma0 | 8 B | 0 | round_trip | 23.0 | 23.0 | R914f44dd |
| tcp_rtt | spin | rdma0 | 8 B | 2 | round_trip | 23.4 | 23.4 | R3a3cc902 |
| tcp_rtt | block | rdma0 | 8 B | 0 | round_trip | 23.9 | 23.9 | R00c81b01 |
| tcp_rtt | block | eth0 | 1 KiB | 1 | round_trip | 23.4 | 23.4 | Rf1043759 |
| tcp_rtt | block | rdma0 | 8 B | 1 | round_trip | 23.8 | 23.8 | Rb7aeb353 |
| tcp_rtt | spin | rdma0 | 1 KiB | 1 | round_trip | 23.9 | 23.9 | R943133d3 |
| tcp_rtt | spin | rdma0 | 1 KiB | 0 | round_trip | 24.0 | 24.0 | R914f44dd |
| tcp_rtt | spin | eth0 | 1 KiB | 2 | round_trip | 24.3 | 24.3 | R5cba1740 |
| tcp_rtt | block | rdma0 | 8 B | 2 | round_trip | 24.0 | 24.0 | Rd08cffd7 |
| tcp_rtt | spin | rdma0 | 1 KiB | 2 | round_trip | 24.5 | 24.5 | R3a3cc902 |
| tcp_rtt | spin | eth0 | 1 KiB | 0 | round_trip | 24.8 | 24.8 | R3d1f1b64 |
| tcp_rtt | spin | eth0 | 1 KiB | 1 | round_trip | 25.0 | 25.0 | Rf4211383 |
| tcp_rtt | block | eth0 | 8 B | 0 | round_trip | 25.2 | 25.2 | R481fbc29 |
| tcp_rtt | block | rdma0 | 1 KiB | 2 | round_trip | 25.4 | 25.4 | Rd08cffd7 |
| tcp_rtt | spin | rdma0 | 4 KiB | 1 | round_trip | 25.5 | 25.5 | R943133d3 |
| tcp_rtt | block | rdma0 | 1 KiB | 1 | round_trip | 24.8 | 24.8 | Rb7aeb353 |
| tcp_rtt | spin | rdma0 | 4 KiB | 0 | round_trip | 26.4 | 26.4 | R914f44dd |
| tcp_rtt | spin | rdma0 | 4 KiB | 2 | round_trip | 26.8 | 26.8 | R3a3cc902 |
| tcp_rtt | block | rdma0 | 1 KiB | 0 | round_trip | 27.2 | 27.2 | R00c81b01 |
| tcp_rtt | block | eth0 | 1 KiB | 0 | round_trip | 27.0 | 27.0 | R481fbc29 |
| tcp_rtt | block | rdma0 | 4 KiB | 1 | round_trip | 27.1 | 27.1 | Rb7aeb353 |
| tcp_rtt | spin | eth0 | 4 KiB | 0 | round_trip | 27.2 | 27.2 | R3d1f1b64 |
| tcp_rtt | block | eth0 | 1 KiB | 2 | round_trip | 27.9 | 27.9 | Rdcf9557f |
| tcp_rtt | block | rdma0 | 4 KiB | 0 | round_trip | 28.2 | 28.2 | R00c81b01 |
| tcp_rtt | block | rdma0 | 4 KiB | 2 | round_trip | 29.2 | 29.2 | Rd08cffd7 |
| ib_write_lat | polled | rdma0 | 8 B | 1 | half_round_trip | 3.8 | 7.5 | R9359c862 |
| ib_write_lat | polled | rdma0 | 8 B | 0 | half_round_trip | 3.8 | 7.5 | Rd8e71d3a |
| ib_write_lat | polled | rdma0 | 8 B | 2 | half_round_trip | 3.8 | 7.5 | Rda997408 |
| tcp_rtt | block | eth0 | 4 KiB | 2 | round_trip | 31.2 | 31.2 | Rdcf9557f |
| tcp_rtt | block | eth0 | 4 KiB | 0 | round_trip | 30.5 | 30.5 | R481fbc29 |
| tcp_rtt | block | eth0 | 4 KiB | 1 | round_trip | 34.7 | 34.7 | Rf1043759 |
| tcp_rtt | spin | eth0 | 4 KiB | 2 | round_trip | 35.4 | 35.4 | R5cba1740 |
| tcp_rtt | spin | eth0 | 4 KiB | 1 | round_trip | 36.1 | 36.1 | Rf4211383 |
| ib_send_lat | polled | rdma0 | 8 B | 1 | half_round_trip | 4.4 | 8.9 | Rcf4eec54 |
| ib_send_lat | polled | rdma0 | 8 B | 2 | half_round_trip | 4.5 | 8.9 | R1d11682b |
| ib_send_lat | polled | rdma0 | 8 B | 0 | half_round_trip | 4.5 | 9.0 | R5714eab9 |
| fi_pingpong | not measured | rdma0 | 8 B | 2 | half_round_trip | not measured | not measured | Rb9e09ce5 |
| fi_pingpong | not measured | rdma0 | 8 B | 0 | half_round_trip | not measured | not measured | R8d5ab9c3 |
| fi_pingpong | not measured | rdma0 | 8 B | 1 | half_round_trip | not measured | not measured | Rcfbfd90c |
| ib_write_lat | polled | rdma0 | 1 KiB | 1 | half_round_trip | 4.7 | 9.4 | R9359c862 |
| ib_write_lat | polled | rdma0 | 1 KiB | 0 | half_round_trip | 4.7 | 9.4 | Rd8e71d3a |
| ib_write_lat | polled | rdma0 | 1 KiB | 2 | half_round_trip | 4.8 | 9.5 | Rda997408 |
| fi_pingpong | not measured | eth0 | 64 KiB | 0 | half_round_trip | not measured | not measured | Re02830d3 |
| fi_pingpong | not measured | eth0 | 64 KiB | 1 | half_round_trip | not measured | not measured | Rd1b2e78a |
| fi_pingpong | not measured | eth0 | 64 KiB | 2 | half_round_trip | not measured | not measured | R79b11805 |
| tcp_rtt | busy_poll | rdma0 | 64 KiB | 1 | round_trip | 46.4 | 46.4 | R861de808 |
| tcp_rtt | busy_poll | rdma0 | 64 KiB | 0 | round_trip | 47.9 | 47.9 | R9fdcf4d8 |
| ib_send_lat | polled | rdma0 | 1 KiB | 1 | half_round_trip | 5.4 | 10.7 | Rcf4eec54 |
| ib_send_lat | polled | rdma0 | 1 KiB | 2 | half_round_trip | 5.4 | 10.8 | R1d11682b |
| ib_send_lat | polled | rdma0 | 1 KiB | 0 | half_round_trip | 5.5 | 11.0 | R5714eab9 |
| fi_pingpong | not measured | rdma0 | 1 KiB | 1 | half_round_trip | not measured | not measured | Rcfbfd90c |
| fi_pingpong | not measured | rdma0 | 1 KiB | 0 | half_round_trip | not measured | not measured | R8d5ab9c3 |
| fi_pingpong | not measured | rdma0 | 1 KiB | 2 | half_round_trip | not measured | not measured | Rb9e09ce5 |
| ib_write_lat | polled | rdma0 | 4 KiB | 2 | half_round_trip | 5.5 | 11.1 | Rda997408 |
| ib_write_lat | polled | rdma0 | 4 KiB | 1 | half_round_trip | 5.6 | 11.1 | R9359c862 |
| ib_write_lat | polled | rdma0 | 4 KiB | 0 | half_round_trip | 5.6 | 11.1 | Rd8e71d3a |
| tcp_rtt | busy_poll | rdma0 | 64 KiB | 2 | round_trip | 53.9 | 53.9 | R70342861 |
| tcp_rtt | busy_poll | eth0 | 64 KiB | 0 | round_trip | 57.2 | 57.2 | R1cab2a3e |
| tcp_rtt | spin | rdma0 | 64 KiB | 2 | round_trip | 58.6 | 58.6 | R3a3cc902 |
| fi_pingpong | not measured | rdma0 | 4 KiB | 0 | half_round_trip | not measured | not measured | R8d5ab9c3 |
| fi_pingpong | not measured | rdma0 | 4 KiB | 1 | half_round_trip | not measured | not measured | Rcfbfd90c |
| ib_send_lat | polled | rdma0 | 4 KiB | 0 | half_round_trip | 6.3 | 12.6 | R5714eab9 |
| fi_pingpong | not measured | rdma0 | 4 KiB | 2 | half_round_trip | not measured | not measured | Rb9e09ce5 |
| ib_send_lat | polled | rdma0 | 4 KiB | 2 | half_round_trip | 6.3 | 12.7 | R1d11682b |
| ib_send_lat | polled | rdma0 | 4 KiB | 1 | half_round_trip | 6.3 | 12.7 | Rcf4eec54 |
| fi_pingpong | not measured | rdma0 | 8 B | 2 | half_round_trip | not measured | not measured | R9d05aefa |
| tcp_rtt | spin | rdma0 | 64 KiB | 1 | round_trip | 59.7 | 59.7 | R943133d3 |
| tcp_rtt | block | eth0 | 64 KiB | 1 | round_trip | 60.3 | 60.3 | Rf1043759 |
| tcp_rtt | block | eth0 | 64 KiB | 0 | round_trip | 60.6 | 60.6 | R481fbc29 |
| tcp_rtt | block | rdma0 | 64 KiB | 2 | round_trip | 60.5 | 60.5 | Rd08cffd7 |
| tcp_rtt | busy_poll | eth0 | 4 KiB | 0 | round_trip | 62.4 | 62.4 | R1cab2a3e |
| tcp_rtt | spin | eth0 | 64 KiB | 1 | round_trip | 61.3 | 61.3 | Rf4211383 |
| tcp_rtt | spin | eth0 | 64 KiB | 2 | round_trip | 61.8 | 61.8 | R5cba1740 |
| tcp_rtt | busy_poll | eth0 | 4 KiB | 1 | round_trip | 62.6 | 62.6 | R83f3ff66 |
| tcp_rtt | block | eth0 | 64 KiB | 2 | round_trip | 62.7 | 62.7 | Rdcf9557f |
| tcp_rtt | spin | eth0 | 64 KiB | 0 | round_trip | 62.4 | 62.4 | R3d1f1b64 |
| tcp_rtt | block | rdma0 | 64 KiB | 0 | round_trip | 61.8 | 61.8 | R00c81b01 |
| tcp_rtt | busy_poll | eth0 | 4 KiB | 2 | round_trip | 64.0 | 64.0 | Rbeaa90ea |
| tcp_rtt | block | rdma0 | 64 KiB | 1 | round_trip | 62.9 | 62.9 | Rb7aeb353 |
| tcp_rtt | spin | rdma0 | 64 KiB | 0 | round_trip | 63.0 | 63.0 | R914f44dd |
| tcp_rtt | busy_poll | eth0 | 64 KiB | 2 | round_trip | 60.4 | 60.4 | Rbeaa90ea |
| ib_read_lat | polled | rdma0 | 8 B | 2 | round_trip | 7.0 | 7.0 | R80ca058b |
| ib_read_lat | polled | rdma0 | 8 B | 1 | round_trip | 7.0 | 7.0 | Rb0ac298a |
| fi_pingpong | not measured | rdma0 | 8 B | 1 | half_round_trip | not measured | not measured | R0ea5b6ab |
| ib_read_lat | polled | rdma0 | 8 B | 0 | round_trip | 7.0 | 7.0 | Rd8b3bed4 |
| fi_pingpong | not measured | rdma0 | 8 B | 0 | half_round_trip | not measured | not measured | R5b8b8413 |
| ib_read_lat | polled | rdma0 | 1 KiB | 2 | round_trip | 7.2 | 7.2 | R80ca058b |
| ib_read_lat | polled | rdma0 | 1 KiB | 0 | round_trip | 7.2 | 7.2 | Rd8b3bed4 |
| ib_read_lat | polled | rdma0 | 1 KiB | 1 | round_trip | 7.2 | 7.2 | Rb0ac298a |
| fi_pingpong | not measured | rdma0 | 1 KiB | 1 | half_round_trip | not measured | not measured | R0ea5b6ab |
| fi_pingpong | not measured | rdma0 | 1 KiB | 0 | half_round_trip | not measured | not measured | R5b8b8413 |
| fi_pingpong | not measured | rdma0 | 1 KiB | 2 | half_round_trip | not measured | not measured | R9d05aefa |
| tcp_rtt | busy_poll | eth0 | 64 KiB | 1 | round_trip | 65.5 | 65.5 | R83f3ff66 |
| ib_read_lat | polled | rdma0 | 4 KiB | 0 | round_trip | 8.1 | 8.1 | Rd8b3bed4 |
| ib_read_lat | polled | rdma0 | 4 KiB | 1 | round_trip | 8.1 | 8.1 | Rb0ac298a |
| ib_read_lat | polled | rdma0 | 4 KiB | 2 | round_trip | 8.3 | 8.3 | R80ca058b |
| fi_pingpong | not measured | rdma0 | 4 KiB | 0 | half_round_trip | not measured | not measured | R5b8b8413 |
| fi_pingpong | not measured | rdma0 | 4 KiB | 1 | half_round_trip | not measured | not measured | R0ea5b6ab |
| fi_pingpong | not measured | rdma0 | 4 KiB | 2 | half_round_trip | not measured | not measured | R9d05aefa |
| ib_send_lat | event | rdma0 | 8 B | 0 | half_round_trip | 9.8 | 19.5 | R9375a8af |

## Case IDs by row ID

| ID | Case IDs |
| --- | --- |
| R00c81b01 | G11N-r0-2dc8f59a47790976f234 |
| R02d93fcd | 0361-garnet-1024-write-concurrent-primary_rf1-r0 |
| R0315682c | G12N-r0-145004c113c28107cf7a, G12N-r1-71d612a2e157e9bae6ba |
| R035b1e82 | G12D-r0-85bb4645de9ec6ac2c6b |
| R05b82bee | 0444-tidehunter_tcp-1024-write-concurrent-primary_rf1-r0 |
| R06a6387a | G11N-r0-885965231e8c0c13fdcc, G11N-r1-8708039783be98298c8c, G11N-r2-c93446317b4969dda6a4 |
| R07667d72 | G10R-r0-fa7602e2c4c40f163ab5, G10R-r1-24c0be527eefa3c0aa14, G10R-r2-327970ee38e08a1d17c7 |
| R08941181 | G12K-r0-05c0529e6b27a6312923, G12K-r1-fb4850c6fd59fd269b89 |
| R08b1e259 | G12H-r0-a30d2f69e58f6fa228c4, G12H-r1-5b7e88797de731fb9ee0 |
| R08dd66c6 | 0329-tidehunter_spdk_live_rdma-1024-read-concurrent-primary_rf1-r0 |
| R09333f62 | 0403-native-1024-write-concurrent-primary_rf1-r0 |
| R097779f8 | G10R-r0-438492ac0c712ee48e7c, G10R-r1-75afe47e7fcf6b622408, G10R-r2-24c1c953cb6a14a8834b |
| R09e69537 | G12N-r0-1bf7951de88ce438d429, G12N-r1-90e0461ad29c86740851 |
| R0a1f9571 | G12W-r0-2343de687170814f7380, G12W-r1-cd4d74f0af19df801878 |
| R0a5ba9da | G12D-r0-0ce5f8eaa8fde37f6566 |
| R0c540d09 | G11W-r0-2315eb0815b251976896 |
| R0d1a0ee3 | G12D-r0-e44182bb7585ecb016b5 |
| R0d9ef188 | G11W-r0-12c27f0c47b5d28b50f4, G11W-r1-f111d3ef78c6c8225d1f |
| R0daebf6d | G12N-r0-347203e8d8ef603207be |
| R0e7cd84c | G12D-r0-422ac0dd0e317cc0e05e |
| R0ea5b6ab | G11N-r1-dd62aa54584a8a0c7949 |
| R0f7a4fa4 | G12K-r0-6f30dd97b36d94529917, G12K-r1-0e517dbd5c62b4783abb |
| R10f237bf | G12N-r0-11d70a8fdb9e3ba961c6 |
| R119f59bc | G12D-r0-946e08ec43aceab58b29 |
| R121f66e4 | G12D-r0-a8efe04ee8fbee8fbe8a |
| R1228e665 | G10W-r1-ef513d2231f3617d4349 |
| R12350055 | G12D-r0-8d13a4dd9e71ae1e58fa |
| R12e5f6b2 | G12N-r0-09c3901cf9f4b61a68bb |
| R13eea891 | G12W-r0-4a7baa0fa67fb1e87d70, G12W-r1-f0be59e1995250f0ac7b |
| R1553ed29 | G12D-r0-891f83803fec3548795e |
| R166d37d0 | G12N-r0-c8cb15e352808a91df2d |
| R167b0400 | G10W-r1-b20bf19390d9153c02ce |
| R176f9741 | 0328-tidehunter_nvme_rdma-1024-read-concurrent-primary_rf1-r0 |
| R17a988c9 | G12S-r0-0c6b896872f4b75c615a |
| R18e6037a | G11N-r0-c4d96540d9a6a599f9a2, G11N-r1-95b9060972bd8aa38dcc, G11N-r2-1b3b29535349f04dd8a1 |
| R19c5b6d1 | G10R-r0-5fc1062105e6c4f81deb, G10R-r1-64f4e785bb3f7e4e8b39, G10R-r2-77ebb89eed8c21eef871 |
| R1a070b2f | G11W-r0-d8a8f22f7fd4ab789862, G11W-r1-a8089c4d8ac7bd43e915 |
| R1a6b90f4 | G10R-r0-795319b9e285d66eb64e, G10R-r1-2f55b0b8b1b65c70f226, G10R-r2-e168e2f73eca43ad8a77 |
| R1cab2a3e | G11N-r0-20496e7355debe6c0838 |
| R1ce5a7b9 | 0294-tidehunter_nvme_rdma-1024-read-many32-primary_rf1-r0 |
| R1d11682b | G11N-r2-5fecd3549ac5e3194d3d |
| R1da4ee60 | 0137-cassandra-1024-write-concurrent-primary_rf1-r0 |
| R1ec39309 | G12D-r0-f9afdb4474380265a761 |
| R1efddb98 | 0237-valkey_tcp-1024-write-concurrent-primary_rf1-r0 |
| R1f5a546a | G12W-r0-3996a4ab3798e1628bef, G12W-r1-94ce900079821b41376c |
| R1fe4f545 | G10R-r0-2f4dbf17ed1376937741, G10R-r1-603ff6cd011379709878, G10R-r2-98b5f82bc8ec80d015ae |
| R20a0678a | 0194-valkey_rdma-1024-read-many32-primary_rf1-r0 |
| R2237ccfa | G12N-r0-96eff37d8124f4635158 |
| R2255ba52 | 0166-foundationdb-1024-point-mixed-primary_rf1-r0 |
| R2301b08a | G12W-r0-f9052391c6a176e4a744, G12W-r1-00d61c49b42a86a4c2da |
| R25b9a596 | G10W-r0-a86df3a7ca387a6caa9c, G10W-r1-c2443f6827ddb642e430, G10W-r2-bd195a1f71770a95a042 |
| R27878092 | G10W-r2-20b2cbd33e2f777939e7 |
| R27cd3fe5 | G12W-r0-d8548ff9d440c93a1075, G12W-r1-349164f9847ba7daeaf0 |
| R28d618d1 | 0310-tidehunter_nvme_rdma-1024-point-mixed-primary_rf1-r0 |
| R292ec93e | G12N-r0-45759819898ecd7ef8e1, G12N-r1-43dd454cf0e039b08ce7 |
| R29bb53bb | 0162-foundationdb-1024-write-concurrent-primary_rf1-r0 |
| R2a0028ea | G12D-r0-1ee08d9526c843efdd5e |
| R2b7082a2 | G10W-r0-c3b61b5d32405fecf7ce, G10W-r1-c41a456754827fdd3b49, G10W-r2-86689df51d75d121ef21 |
| R2bd14b89 | 0380-native-1024-read-many32-primary_rf1-r0 |
| R2cd4dfed | 0212-kvrocks-1024-write-concurrent-primary_rf1-r0 |
| R2eb25424 | G12N-r0-37c86701d4c123bbc901, G12N-r1-571b72f1bed8fa8a574e |
| R2ec0e6ef | G12W-r0-abf4811f0c83f4c6ebdd, G12W-r1-08aed061df7032af3ba6 |
| R2f8ceb1a | G10R-r0-0b326285a96b9314a8a0, G10R-r1-ae64be7b622d2efe436a, G10R-r2-b7c38baf820554c68ff4 |
| R3067be9e | G12W-r0-df2056d640141420ba2a |
| R312d7193 | G10W-r1-fbcc78f4313adc1a1ff0 |
| R31b628ad | 0464-tikv-1024-point-mixed-primary_rf1-r0 |
| R33379631 | G11W-r0-85b8924f27ba377dee5a, G11W-r1-6da4a765f6c220c9ba1a |
| R33dc2ab6 | G11W-r0-e1e23dc8f5773134dae5, G11W-r1-ebc9834e4b2718ea6f2b |
| R340c8bef | G11N-r2-5778851ff71db9a118e3 |
| R350a9e07 | G10W-r0-8f091b88fd085ee9c16e |
| R3531542c | G10W-r0-f505cb61a39227649831, G10W-r1-4ada911868c20e64f0e5, G10W-r2-d7fcb6a5a3ca7dbe01ff |
| R35c69c2b | G12W-r0-50c57f19cfbe18adf402 |
| R360ccccc | G12N-r1-e534c728a89ae62be4d6 |
| R361477d0 | G12W-r0-1f7c5040bdd267152478, G12W-r1-98ec1d6a62b8eb834d6c |
| R36ffbce9 | G11S-r0-95794ff62de0ef77ee32, G11S-r0-cc61e45744d1662187f2, G11S-r1-2d62f9a7e7179e4fd163, G11S-r1-6c625fd2a7baf74a4693, G11S-r2-89956b09e0126db57a31, G11S-r2-a9fa51ac73a800c260ff |
| R3933c347 | 0273-tidehunter_verbs_native-1024-read-concurrent-primary_rf1-r0 |
| R3a3cc902 | G11N-r2-1b3b29535349f04dd8a1 |
| R3b547898 | G12D-r0-44955ac13d9c876d7e43 |
| R3cc10c42 | G10W-r0-271619f18edbc6c7474b, G10W-r1-ef513d2231f3617d4349, G10W-r2-8dede022dd83dd08bdb6 |
| R3d1f1b64 | G11N-r0-41591a863ae861bc75ea |
| R3df860f7 | G11W-r0-12c27f0c47b5d28b50f4 |
| R3e9799a7 | G12D-r0-16ab8000aa2df52a8c9c |
| R3ec6d58f | G12W-r0-8ceeb90477db60395fc9 |
| R3eca63c4 | G10W-r0-05c5e56efcc956f77cf7, G10W-r0-a33eed1b729deb2e03c1, G10W-r1-5bf4e70256079a3689d2, G10W-r1-9181253bedee29f31445, G10W-r2-41ab17af954803b38b62, G10W-r2-fad245da336ce029d41b |
| R3ef5fa63 | 0421-tidehunter_tcp-1024-read-many32-primary_rf1-r0 |
| R40880520 | G12D-r0-08a9a20ff91943a18559 |
| R409e1b71 | G12D-r0-85a66013cf34cf17a20b |
| R4167fc5f | G11N-r0-0e65019332d66c31922e |
| R41ecb4e4 | G11W-r0-2315eb0815b251976896, G11W-r1-ebe4433eec3d837e461c |
| R4231e6e7 | 0438-tidehunter_tcp-1024-read-concurrent-primary_rf1-r0 |
| R423d5b77 | G10W-r0-eaf10679999e2e41fcf7, G10W-r1-8ced39935ef9df694629, G10W-r2-adff0979acf8d5b70f69 |
| R436ed9cf | G12N-r0-c880681be199613967c8, G12N-r1-9c12d42e80801e87c013 |
| R4533c702 | G12W-r0-eabc0ecbc9c67a1319ef, G12W-r1-86e3eaeeeab45dda7255 |
| R4697e418 | G12N-r0-6ca6185ad3fd75f1bde9 |
| R46c72461 | 0429-tidehunter_tcp-1024-point-mixed-primary_rf1-r0 |
| R47632491 | 0214-kvrocks-1024-read-concurrent-primary_rf1-r0 |
| R481fbc29 | G11N-r0-316adbdfcbe4209e60a8 |
| R48d0f867 | 0363-garnet-1024-read-concurrent-primary_rf1-r0 |
| R48f1eb45 | G10R-r0-85912049caaa7fd88df3, G10R-r1-57f6e2da9c91e8421b63, G10R-r2-dda90a126b39cb2c4fab |
| R49d0c784 | G12N-r0-71c5d9b59a54d4b09104 |
| R4a9b876d | 0467-tikv-1024-read-many32-primary_rf1-r0 |
| R4afa3c44 | G12W-r0-1e754c58211b5f62a57b, G12W-r1-5c6917b77481e841c67e |
| R4b6b43bd | G12D-r0-a2dfb7244367ba447ecf |
| R4c5db0f2 | 0141-cassandra-1024-point-mixed-primary_rf1-r0 |
| R4ce90626 | G12D-r0-5ac5e1c809e1545074bf |
| R4d2eaa40 | 0244-valkey_tcp-1024-read-many32-primary_rf1-r0 |
| R4d61ab72 | 0279-tidehunter_verbs_native-1024-write-concurrent-primary_rf1-r0 |
| R4e7ce30a | G10W-r0-1166cc7e0cfe7328b085, G10W-r0-df90a59028a85612d0c2, G10W-r1-4cd73ff059f579d8bce6, G10W-r1-ee0656aecb8524e6fdc5, G10W-r2-00fdd3a6e96b8e915926, G10W-r2-8ca5ddadace54c4520d9 |
| R4f95aea4 | G10R-r0-4b834dc62338fc41492d, G10R-r1-b06d2967617b30d7e056, G10R-r2-9224014ac84f2caa195d |
| R502435c7 | G12H-r0-08107dd69408aa74cbb2, G12H-r1-641fdcc07e0277e25e7d |
| R51acc123 | G11N-r1-b79de82d9cedba10f980 |
| R51feece2 | G12S-r0-48d3c0ba8811ca5525fb |
| R526ec4c4 | G10W-r0-1d1959b5b5f00d49153c, G10W-r1-22f945dde174e078d13d, G10W-r2-1f46cf855e873899b0a6 |
| R532834ed | G12K-r0-fc2fd2a25da56cb25569, G12K-r1-ff2de7cf08a231dcedc2 |
| R565f3b20 | G12N-r0-32d7e80dd698536bde42 |
| R5714eab9 | G11N-r0-2843c0837b2bed148656 |
| R58624c3d | G11N-r0-6718600f8745417e6432 |
| R58ebf028 | G12K-r0-c345979e9d1e8d1cfd00, G12K-r1-cf29026d867edc4e4859 |
| R598ad5f1 | G12W-r0-ab890929d021ff1fc416 |
| R5b8b8413 | G11N-r0-760721487787ea83495c |
| R5bd21b57 | G11N-r0-2dc8f59a47790976f234, G11N-r1-1465131c5dce62117b3f, G11N-r2-16bb718053f0b1e3631b |
| R5c5502ba | G12N-r0-1afb6b4137ab0dd39ed4, G12N-r1-b6dc675adcdd8150bd16 |
| R5cba1740 | G11N-r2-2d0a7a64c4fb147bcddc |
| R5cdd5f46 | G12D-r0-ea307ca09176802afb52 |
| R5f6d119c | G12W-r0-18e56187151f733b5b6c, G12W-r1-f183a8f968a940749fed |
| R5fb21fa9 | G11N-r0-20496e7355debe6c0838, G11N-r1-a3a1c8d7b3d0c9db53c1, G11N-r2-10584f3ce667665db056 |
| R607237d9 | G11N-r0-41591a863ae861bc75ea, G11N-r1-fb003b3c7fce25548a22, G11N-r2-2d0a7a64c4fb147bcddc |
| R61acba84 | G11W-r0-39c70b4b1059975564d8, G11W-r1-446ea8a0690e05deecb7 |
| R63036dd4 | G12W-r0-f1ae3fd931b89f553348, G12W-r1-9f4df581e631e452598f |
| R63bb4082 | G12D-r0-25b3b0b71b73950ff12b |
| R64025107 | G12W-r0-fdd772fad858fdfa5a50 |
| R64240230 | G11W-r1-f111d3ef78c6c8225d1f |
| R64660f89 | G11N-r2-1dedb8d0f058d0d7e7b2 |
| R649e7f1f | G11W-r1-6da4a765f6c220c9ba1a |
| R64d5dda3 | G12W-r0-b027b0460d889d5a4f0d, G12W-r1-99583ed97376d88a2fea |
| R65a80e76 | G12N-r0-c4afdaf6e00659738c07, G12N-r1-16c0bb1c7b55908f21c3 |
| R667e1835 | 0139-cassandra-1024-read-concurrent-primary_rf1-r0 |
| R66b9e720 | 0189-valkey_rdma-1024-read-concurrent-primary_rf1-r0 |
| R6941aa5d | 0216-kvrocks-1024-point-mixed-primary_rf1-r0 |
| R6a798c0a | 0219-kvrocks-1024-read-many32-primary_rf1-r0 |
| R6bad39f9 | G12N-r0-4a9a856634c4e49248b2, G12N-r1-060d2cef5e9eb49a56c6 |
| R6d2332f6 | G12D-r0-ff2dc0eb7327c48c5b06 |
| R6dbb1b55 | G12D-r0-9eaa39f00ad063fef4cf |
| R6ed9c0dc | G12N-r0-39d81d573da5d8ff4b87 |
| R70342861 | G11N-r2-aded9f8babb13743176f |
| R70852372 | G12W-r0-fd19295e3d654e9d03c3, G12W-r1-8e8fa5b5d31d8f2c339c |
| R70bfe74b | G11S-r0-866e721336003d8a8961, G11S-r0-e9a10df9ef937625a650, G11S-r1-61dc06bc9a7dc9ce893e, G11S-r1-988dd28940e91138b3ac, G11S-r2-2ccc1ab60cf6db35b302, G11S-r2-b78dbb5e87bfea63086a |
| R71f25999 | G12W-r0-cd6df925e04f2d9302eb |
| R731c6dff | G11N-r2-162fdeab6870150bc689 |
| R73edea0a | G10W-r0-6826b27edc7b96544300, G10W-r1-fbcc78f4313adc1a1ff0, G10W-r2-20b2cbd33e2f777939e7 |
| R74a9fc72 | G10W-r0-04cf96c6749a890890e1, G10W-r1-5bed3da247f2530af3ac, G10W-r2-52c9e1d7491e5fa9743d |
| R74b6f05b | G10R-r0-30f3af95bf5b84e817ea, G10R-r1-ed02b5c8c0e890d7010e, G10R-r2-490e9c00ea624f42689a |
| R74ba35b3 | G11N-r1-48e9a0257de88bfa2cac |
| R752a0ff1 | G12W-r0-0c67db908b23da1d2eb0 |
| R75e726d0 | G12W-r0-ba7788e3e348e83cb1f6, G12W-r1-1eefe5d0f71c2c75eb9e |
| R78233185 | G12N-r0-d099d81d2a8be00bc2fe, G12N-r1-0536d31e0e2d6174c915 |
| R792ec997 | 0164-foundationdb-1024-read-concurrent-primary_rf1-r0 |
| R79431f2a | G12D-r0-c43d911a13fcb571c6dc |
| R79b11805 | G11N-r2-5bb0ec7990ae3c415890 |
| R79ce08f6 | G10W-r0-a86df3a7ca387a6caa9c, G10W-r0-f505cb61a39227649831, G10W-r1-4ada911868c20e64f0e5, G10W-r1-c2443f6827ddb642e430, G10W-r2-bd195a1f71770a95a042, G10W-r2-d7fcb6a5a3ca7dbe01ff |
| R7a263dd8 | G12N-r0-8dd82f3f9eb4f36f84e9 |
| R7a998b08 | G11W-r1-ebe4433eec3d837e461c |
| R7b01961d | G10W-r0-84ce2f8bb305315898ac, G10W-r0-f61d36513a35dafa578e, G10W-r1-383fefa6a510b15bc2f2, G10W-r1-540b3a35836db0587bbb, G10W-r2-47f8e6cf08853be540b2, G10W-r2-9ac8a0d6d54950733150 |
| R7b117213 | G10W-r0-271619f18edbc6c7474b |
| R7d78c3c3 | G11S-r0-8ba142910d8c25c825c7, G11S-r0-faf46a9a5aec4a9bf9fd, G11S-r1-2f3eee49e285060345e9, G11S-r1-eafb74bbb474d9221121, G11S-r2-50bd8b9670cfefd835c7, G11S-r2-7c6e5465c59984ca3b46 |
| R7e4bd14c | G12D-r0-680caf9e9ea2297754ee |
| R7e542275 | G12W-r0-1009acd330897c011ee8, G12W-r1-4942dacb53baa5f9addf |
| R7ebbb903 | G12N-r0-429695ee4a54fcf877ee, G12N-r1-9b63b51292e26b74aa86 |
| R7f945823 | G12N-r0-99cc3deeb08472647fd7, G12N-r1-42e27ac5a90c0c92837d |
| R80ab5a8b | G11W-r0-ac462284b350457c3773, G11W-r1-a59e8bdea8659c3f7b0c |
| R80ca058b | G11N-r2-aa9d7959c17f6126ac61 |
| R83f3ff66 | G11N-r1-a3a1c8d7b3d0c9db53c1 |
| R8452f893 | 0264-tidehunter_verbs_native-1024-point-mixed-primary_rf1-r0 |
| R852b2f71 | G12D-r0-f154e627c9d924039b33 |
| R853ff1b5 | G11N-r0-18c6ea3330a81fb36d78, G11N-r1-1dbbc3091d84bdb655a4, G11N-r2-aa9d7959c17f6126ac61 |
| R85c88053 | 0368-garnet-1024-read-many32-primary_rf1-r0 |
| R861de808 | G11N-r1-b70c318936c93335bf84 |
| R870d07f8 | G12D-r0-f77fd377f6fb79ae8ec4 |
| R886717d2 | G12N-r0-2c1c74100efa2b346a7e, G12N-r1-51efa51375736f209599 |
| R8967c0cd | G12K-r0-91a212ea792c698ae323, G12K-r1-ed5ffcab64942ff99647 |
| R89f02779 | G10W-r2-8dede022dd83dd08bdb6 |
| R8a2dc242 | G12W-r0-bf760021547b0b79c839 |
| R8a82b2b2 | G10R-r0-5cc0c8edaac1d53dcd93, G10R-r1-f8f7e61869871b26918d, G10R-r2-a043501bf2187d481a1b |
| R8bbcd2a8 | G10W-r2-48dd257cd5f21ead5cf0 |
| R8bfe5b29 | G12S-r0-5c51f229bf73f13e4b74 |
| R8c6b50b8 | G12D-r0-fdfd5c02439e7284baef |
| R8d29f5b7 | G12D-r0-cd8da7873f178cd0ac99 |
| R8d5ab9c3 | G11N-r0-178078fb522e8eeeea40 |
| R914f44dd | G11N-r0-c4d96540d9a6a599f9a2 |
| R91593185 | 0462-tikv-1024-read-concurrent-primary_rf1-r0 |
| R92629de8 | G12N-r0-edbcd4ba458de99edef6 |
| R92c4c91e | G12S-r0-08dd9c45f89e566a1471 |
| R9359c862 | G11N-r1-8708039783be98298c8c |
| R9375a8af | G11N-r0-4d538fd78dc970686c09 |
| R93c068f2 | 0397-native-1024-read-concurrent-primary_rf1-r0 |
| R943133d3 | G11N-r1-95b9060972bd8aa38dcc |
| R948f9096 | 0340-tidehunter_nvme_rdma-1024-write-concurrent-primary_rf1-r0 |
| R957ce20c | G12D-r0-e5d843928c49911c6e12 |
| R95ce919e | G10W-r0-e321abf3b6d5561fe00f, G10W-r1-2b939cb8a65a363ae916, G10W-r2-265c644c2300d706085f |
| R978d8580 | G11W-r0-65c267ba8734f65b1f21, G11W-r1-a8120fc3e7549fba0bce |
| R97e5b4fa | G12D-r0-e7a301bbafbe6ef68f5c |
| R9a805863 | G12W-r0-520e5128ba777a50a671, G12W-r1-082e4deb18832a3502d3 |
| R9ad6c8ef | G10R-r0-55d6611fb7d300c36903, G10R-r1-aae85e5855c41c6fffe0, G10R-r2-a2b399b64f42887286ae |
| R9bbfa53e | G12H-r0-4903e9f4eabb5789769e, G12H-r1-f90987af4a748cb7bf98 |
| R9c0dae5c | G12D-r0-51ce1f1a650ef6f7cb2a |
| R9c9bedb1 | G12D-r0-2d105120fe757255aaa3 |
| R9cdcd4f8 | G10W-r0-8f091b88fd085ee9c16e, G10W-r1-b20bf19390d9153c02ce, G10W-r2-48dd257cd5f21ead5cf0 |
| R9d05aefa | G11N-r2-522df410499500489008 |
| R9d90d5c8 | G12W-r0-f5d94f92494381714294, G12W-r1-d29a21c6f2a7661a151d |
| R9fdcf4d8 | G11N-r0-e6bde3296d0188b4bd20 |
| R9fed41d3 | G10W-r0-05c5e56efcc956f77cf7, G10W-r1-9181253bedee29f31445, G10W-r2-41ab17af954803b38b62 |
| Ra056169f | G12D-r0-be14bc607d9e72aafdea |
| Ra205001f | 0144-cassandra-1024-read-many32-primary_rf1-r0 |
| Ra26edc8a | G11S-r0-866e721336003d8a8961, G11S-r1-61dc06bc9a7dc9ce893e, G11S-r2-b78dbb5e87bfea63086a |
| Ra2ce76c9 | G12H-r0-f27159df58bfba731319, G12H-r1-210eb270e3bc67aae410 |
| Ra39565f2 | G12W-r0-f79b00d6b98355d39aed |
| Ra466d20c | G12N-r0-0d9970e0a85b4b9cf736, G12N-r1-b2a2bb40ed8d30a5ecc0 |
| Ra6460d3c | 0256-tidehunter_verbs_native-1024-read-many32-primary_rf1-r0 |
| Ra8a139b7 | G11N-r0-0e65019332d66c31922e, G11N-r1-48e9a0257de88bfa2cac, G11N-r2-5778851ff71db9a118e3 |
| Ra8cafbba | G12D-r0-6c4d9d94da989fe10037 |
| Ra95f9a3f | G10W-r2-27028fe049cf50cba9b6 |
| Ra973e337 | G12W-r0-875e03484434cae873ae, G12W-r1-977b5e9091e4795f3622 |
| Rab050378 | 0460-tikv-1024-write-concurrent-primary_rf1-r0 |
| Rab51db6a | G10R-r0-b87eaa607febd636eb39, G10R-r1-49f04fa8aad3add459ea, G10R-r2-517be587f2b18400f059 |
| Rac03e4ee | G12W-r0-6871d9af07b5c59e0980, G12W-r1-0f5a9b9e742c54f947e1 |
| Rac31c3a7 | G12D-r0-caeca8b356639464cb81 |
| Raca95b51 | G11S-r0-18d5f498e68050a64aaa, G11S-r0-3bc5fa1eb1abd8f640af, G11S-r1-3e4cc90021e7653dc1e8, G11S-r1-4ea2afcbc65a5f9e6409, G11S-r2-577b2e27d442c01511bb, G11S-r2-57e816fc35a8b2755b66 |
| Rad712f55 | G10R-r0-58cece69d889dfe4e2f0, G10R-r1-1e1eedb20a2b2f8ca1c5, G10R-r2-47c1b0449d99e04f9796 |
| Radb79813 | G10R-r0-15650e7a54b90a92ad08, G10R-r1-84ebc9c1fa06cc535d85, G10R-r2-3b324efa5c769d509db7 |
| Radbfd0ca | G12D-r0-27eca44fe7aec7711b68 |
| Radc0148c | G11N-r1-78f7f2f06890e78a9fd1 |
| Rae050ca3 | G10W-r0-df90a59028a85612d0c2, G10W-r1-4cd73ff059f579d8bce6, G10W-r2-00fdd3a6e96b8e915926 |
| Raf0d7639 | G12W-r0-fad2da9e81f649c5000a, G12W-r1-0a465b960a3e8cd200ba |
| Rb0ac298a | G11N-r1-1dbbc3091d84bdb655a4 |
| Rb0bdeee1 | G11N-r0-4d538fd78dc970686c09, G11N-r1-b79de82d9cedba10f980, G11N-r2-1dedb8d0f058d0d7e7b2 |
| Rb14099c4 | G12W-r0-ee7efc9784b68c46f3f4, G12W-r1-978ea6e0cff137c3b4e0 |
| Rb27b40da | G11N-r0-316adbdfcbe4209e60a8, G11N-r1-33c3114c74714cfe5782, G11N-r2-a07a30813a4053a69281 |
| Rb42eacfc | G12W-r0-4fa9db0eda5bd289f21a, G12W-r1-9429948eeb550ac4741e |
| Rb522390b | G10W-r0-a33eed1b729deb2e03c1, G10W-r1-5bf4e70256079a3689d2, G10W-r2-fad245da336ce029d41b |
| Rb59ca589 | G10R-r0-b67cb8c8e3d7985fa927, G10R-r1-8071032ef66213103679, G10R-r2-f7a3b18646642da09186 |
| Rb5f18cfd | 0311-tidehunter_spdk_live_rdma-1024-point-mixed-primary_rf1-r0 |
| Rb6e0a29f | G10W-r0-e321abf3b6d5561fe00f, G10W-r0-f6d4f36a8088207cf0e0, G10W-r1-2b939cb8a65a363ae916, G10W-r1-88b4e7895a54492f4b89, G10W-r2-265c644c2300d706085f, G10W-r2-c2f191d41f41b68083f2 |
| Rb7aeb353 | G11N-r1-1465131c5dce62117b3f |
| Rb9e09ce5 | G11N-r2-40bd08fb15e65b9eb45a |
| Rba0117af | G10W-r0-6826b27edc7b96544300 |
| Rbac70f98 | G10W-r0-f6d4f36a8088207cf0e0, G10W-r1-88b4e7895a54492f4b89, G10W-r2-c2f191d41f41b68083f2 |
| Rbb3c7489 | G10W-r0-533e3a76d2827ba388e3, G10W-r1-61ead5aaf7b71402f662, G10W-r2-0dd6da99b7ad00ff215e |
| Rbdb65963 | G12K-r0-e2bac9cb674080bed584, G12K-r1-024b5f3589ec79ed859d |
| Rbeaa90ea | G11N-r2-10584f3ce667665db056 |
| Rbf0c94da | G12W-r0-01510aac6fd82df46921, G12W-r1-d388079b5314c2434b45 |
| Rbf24384c | 0187-valkey_rdma-1024-write-concurrent-primary_rf1-r0 |
| Rc0228526 | G12W-r0-bf11fa11d1b8de2a9f55 |
| Rc11b3801 | G12D-r0-715e771584ac07ecd4dd |
| Rc15b54b0 | G12N-r0-d999a38ac149faf6bce0, G12N-r1-2dcae64c4a553d99b6c4 |
| Rc2292fda | G12W-r0-6c950f376852fa86bfda, G12W-r1-27baa096e94a5351b150 |
| Rc2cbb87b | G12N-r0-96eff37d8124f4635158, G12N-r1-e534c728a89ae62be4d6 |
| Rc40c02ab | G10R-r0-aa630e7fbe15a4d78f20, G10R-r1-70f55c159ed8ad971865, G10R-r2-2f387681b283b8642b38 |
| Rc443c105 | G11S-r0-e9a10df9ef937625a650, G11S-r1-988dd28940e91138b3ac, G11S-r2-2ccc1ab60cf6db35b302 |
| Rc5bc5a74 | G10W-r0-6089328c109505994202 |
| Rc7e58ea0 | G12D-r0-2d96d2b8eeab0e23aab1 |
| Rc87b6aea | G12D-r0-f3e5e748b856e9aafc6a |
| Rc9cfaa51 | G12D-r0-356b4518eb3e5ab9f8c5 |
| Rcb575be2 | 0388-native-1024-point-mixed-primary_rf1-r0 |
| Rcbcbe610 | 0169-foundationdb-1024-read-many32-primary_rf1-r0 |
| Rcc543507 | G10W-r0-f61d36513a35dafa578e, G10W-r1-540b3a35836db0587bbb, G10W-r2-9ac8a0d6d54950733150 |
| Rce5afd3a | G10W-r0-533e3a76d2827ba388e3, G10W-r0-c3b61b5d32405fecf7ce, G10W-r1-61ead5aaf7b71402f662, G10W-r1-c41a456754827fdd3b49, G10W-r2-0dd6da99b7ad00ff215e, G10W-r2-86689df51d75d121ef21 |
| Rced4e202 | 0241-valkey_tcp-1024-point-mixed-primary_rf1-r0 |
| Rcef2ba8b | G10W-r0-1d1959b5b5f00d49153c, G10W-r0-eaf10679999e2e41fcf7, G10W-r1-22f945dde174e078d13d, G10W-r1-8ced39935ef9df694629, G10W-r2-1f46cf855e873899b0a6, G10W-r2-adff0979acf8d5b70f69 |
| Rcf24342a | G10W-r0-6089328c109505994202, G10W-r1-9ece8d505b138776f51a, G10W-r2-27028fe049cf50cba9b6 |
| Rcf4eec54 | G11N-r1-04ac165a5afa4bbdf5b9 |
| Rcfbfd90c | G11N-r1-8da6a3807d9fe2e1c170 |
| Rd08cffd7 | G11N-r2-16bb718053f0b1e3631b |
| Rd1b2e78a | G11N-r1-e197fe7d26733b4bd4e8 |
| Rd367c647 | 0191-valkey_rdma-1024-point-mixed-primary_rf1-r0 |
| Rd4328490 | G12N-r0-c174ae3b27beb79e25e5, G12N-r1-adcff825e80eb5b7df9d |
| Rd608b458 | G10W-r0-1166cc7e0cfe7328b085, G10W-r1-ee0656aecb8524e6fdc5, G10W-r2-8ca5ddadace54c4520d9 |
| Rd6321af3 | G12N-r0-cd80c55e854c9621d42f |
| Rd6fbe26d | G12H-r0-d36e69105c83c581a2a9, G12H-r1-fd82b0885c7224f99a0a |
| Rd7e6aeeb | G11W-r0-85b8924f27ba377dee5a |
| Rd8b3bed4 | G11N-r0-18c6ea3330a81fb36d78 |
| Rd8e71d3a | G11N-r0-885965231e8c0c13fdcc |
| Rd9dd0e0a | 0295-tidehunter_spdk_live_rdma-1024-read-many32-primary_rf1-r0 |
| Rd9fbc4cb | G11N-r0-2843c0837b2bed148656, G11N-r1-04ac165a5afa4bbdf5b9, G11N-r2-5fecd3549ac5e3194d3d |
| Rda2b3087 | 0341-tidehunter_spdk_live_rdma-1024-write-concurrent-primary_rf1-r0 |
| Rda997408 | G11N-r2-c93446317b4969dda6a4 |
| Rda9d8374 | G12N-r0-039afc51dac29eab2452 |
| Rdcf9557f | G11N-r2-a07a30813a4053a69281 |
| Rde60222f | G10W-r0-39a667d7b4df4252ce7c, G10W-r1-c4ed8ba8a31a9e3d0d10, G10W-r2-9d1994a816306091c5ff |
| Rdf8bc338 | 0239-valkey_tcp-1024-read-concurrent-primary_rf1-r0 |
| Re02830d3 | G11N-r0-7b9f4eaae52f819ae94f |
| Re0433543 | G11N-r0-e6bde3296d0188b4bd20, G11N-r1-b70c318936c93335bf84, G11N-r2-aded9f8babb13743176f |
| Re10469a9 | G12N-r0-0192d407c94952220e5c, G12N-r1-21f39f554b8e77a543ef |
| Re116d570 | G12D-r0-695f97a66aaa2010c496 |
| Re141dcbd | G11W-r1-446ea8a0690e05deecb7 |
| Re3876655 | G12D-r0-321ba7273072eec9f5f4 |
| Re3e4a400 | G10W-r1-9ece8d505b138776f51a |
| Re69f96c6 | G12D-r0-94efa733eb5acc17b813 |
| Re6bb5a02 | G10W-r0-04cf96c6749a890890e1, G10W-r0-39a667d7b4df4252ce7c, G10W-r1-5bed3da247f2530af3ac, G10W-r1-c4ed8ba8a31a9e3d0d10, G10W-r2-52c9e1d7491e5fa9743d, G10W-r2-9d1994a816306091c5ff |
| Re7acf834 | G12D-r0-ca7078fc585acc3331ce |
| Re7dbaa50 | 0365-garnet-1024-point-mixed-primary_rf1-r0 |
| Rec990c3c | G10R-r0-76a7470a52a0fabf96d1, G10R-r1-57c2b58e084323a444ba, G10R-r2-0b5669693b5a72103cd8 |
| Red792580 | G10R-r0-4b45de9546ffddc726ba, G10R-r1-292778f4d58784b9c261, G10R-r2-77a7889e7c6d6abcb6c3 |
| Ree7b782d | G12N-r0-6acd3bd110c039acddac, G12N-r1-61ebda9713f25cffbd8c |
| Reec31ff2 | G11W-r0-48f36af14e66fbde6ade, G11W-r1-a3b93e714cfc091088df |
| Ref712b7f | G12D-r0-9fbbe6ec9059cca6b156 |
| Refab106d | G11W-r0-39c70b4b1059975564d8 |
| Rf1043759 | G11N-r1-33c3114c74714cfe5782 |
| Rf1572aad | G12N-r0-b398722460ff447a61e1, G12N-r1-f64741d4062af3d3f80e |
| Rf18ada8a | G10R-r0-a4e0b551082c3cd8e38e, G10R-r1-a5b89d170f1ac9448a4d, G10R-r2-490320f69832696c18d6 |
| Rf279f18f | G12W-r0-0eb65b37e91231084973 |
| Rf2eb0bc0 | G12N-r0-47008b9e4093aad34aac |
| Rf3321be1 | G10W-r0-84ce2f8bb305315898ac, G10W-r1-383fefa6a510b15bc2f2, G10W-r2-47f8e6cf08853be540b2 |
| Rf4211383 | G11N-r1-fb003b3c7fce25548a22 |
| Rf6f6c07f | G12W-r0-e90c510e264a38e22839 |
| Rf7bca305 | G10R-r0-866e801c10fb6b732d36, G10R-r1-6d2b9ff8320998888f8f, G10R-r2-ffd7ce53d7bba3bed8db |
| Rfaf05629 | G12N-r0-33278ba0a57f4c28b668 |
| Rffe00a2c | G12H-r0-e9595d9ed92bf6c4f5e0, G12H-r1-61cee23f032e337eaf25 |
