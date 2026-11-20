# v0.4 provisional results

Pulled 2026-11-17T01:47Z while the campaign was still running (at tinode latency-ladder crypt rep 3). dotwire image: ghcr.io/pasztor-mark/dotwire:0.4.0

Notes: tinode steps with sent=0 are overload (WAL fsync bound, ~390 fsync/s on the crypt volume), not a crash. dotwire reconnect-storm/connections are likely inflated by listen-queue overflow (SYN cookies on :8080, Kestrel backlog 512, tcp_max_syn_backlog 2048).

## dotwire · connections · plain · rep 1

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| level=10000 | 0 | 0 | 0 | 0 | 0 | 0 | 9.399 | 100.927 | 146.047 | false | no |
| level=25000 | 0 | 0 | 0 | 0 | 0 | 0 | 10.303 | 230.399 | 393.471 | false | no |
| level=50000 | 0 | 0 | 0 | 0 | 0 | 0 | 9.815 | 356.351 | 639.999 | false | no |

## dotwire · connections · plain · rep 2

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| level=10000 | 0 | 0 | 0 | 0 | 0 | 0 | 10.615 | 108.031 | 194.687 | false | no |
| level=25000 | 0 | 0 | 0 | 0 | 0 | 0 | 9.895 | 259.967 | 416.255 | false | no |
| level=50000 | 0 | 0 | 0 | 0 | 0 | 0 | 9.887 | 382.207 | 726.527 | false | no |

## dotwire · connections · plain · rep 3

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| level=10000 | 0 | 0 | 0 | 0 | 0 | 0 | 9.487 | 95.039 | 136.063 | false | no |
| level=25000 | 0 | 0 | 0 | 0 | 0 | 0 | 9.895 | 252.799 | 467.199 | false | no |
| level=50000 | 0 | 0 | 0 | 0 | 0 | 0 | 9.823 | 344.063 | 723.967 | false | no |

## dotwire · fanout · plain · rep 1

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| size=10 | 1200 | 10800 | 10800 | 0 | 0 | 0 | 6.539 | 9.359 | 23.727 | false | no |
| size=100 | 1200 | 118800 | 118800 | 0 | 0 | 0 | 6.711 | 9.935 | 23.359 | false | no |
| size=1000 | 1200 | 1198800 | 1198800 | 0 | 0 | 0 | 13.767 | 35.423 | 68.159 | false | no |
| size=5000 | 1200 | 5998800 | 5998800 | 0 | 0 | 0 | 39.295 | 162.943 | 359.679 | false | no |

## dotwire · fanout · plain · rep 2

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| size=10 | 1200 | 10800 | 10800 | 0 | 0 | 0 | 6.179 | 7.855 | 28.479 | false | no |
| size=100 | 1200 | 118800 | 118800 | 0 | 0 | 0 | 7.283 | 10.431 | 120.383 | false | no |
| size=1000 | 1200 | 1198800 | 1198800 | 0 | 0 | 0 | 14.759 | 36.735 | 69.887 | false | no |
| size=5000 | 1200 | 5998800 | 5998800 | 0 | 0 | 0 | 39.167 | 163.839 | 364.031 | false | no |

## dotwire · fanout · plain · rep 3

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| size=10 | 1200 | 10800 | 10800 | 0 | 0 | 0 | 6.391 | 8.407 | 16.751 | false | no |
| size=100 | 1200 | 118800 | 118800 | 0 | 0 | 0 | 6.595 | 8.359 | 23.711 | false | no |
| size=1000 | 1200 | 1198800 | 1198800 | 0 | 0 | 0 | 14.687 | 36.511 | 63.551 | false | no |
| size=5000 | 1200 | 5998800 | 5998800 | 0 | 0 | 0 | 39.103 | 162.687 | 360.959 | false | no |

## dotwire · history · plain · rep 1

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rate=100 | 12000 | 0 | 0 | 0 | 0 | 0 | 6.483 | 15.119 | 27.199 | false | no |
| rate=500 | 60000 | 0 | 0 | 0 | 0 | 0 | 5.727 | 9.191 | 11.231 | false | no |
| rate=1000 | 120000 | 0 | 0 | 0 | 0 | 0 | 3.681 | 6.031 | 11.551 | false | no |
| rate=2000 | 240000 | 0 | 0 | 0 | 0 | 0 | 5.403 | 11.935 | 23.535 | false | no |

## dotwire · history · plain · rep 2

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rate=100 | 12000 | 0 | 0 | 0 | 0 | 0 | 6.879 | 14.279 | 28.591 | false | no |
| rate=500 | 60000 | 0 | 0 | 0 | 0 | 0 | 6.067 | 9.415 | 11.263 | false | no |
| rate=1000 | 120000 | 0 | 0 | 0 | 0 | 0 | 4.027 | 6.191 | 8.863 | false | no |
| rate=2000 | 240000 | 0 | 0 | 0 | 0 | 0 | 5.599 | 12.871 | 125.695 | false | no |

## dotwire · history · plain · rep 3

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rate=100 | 12000 | 0 | 0 | 0 | 0 | 0 | 7.731 | 15.223 | 27.439 | false | no |
| rate=500 | 60000 | 0 | 0 | 0 | 0 | 0 | 6.039 | 9.359 | 11.543 | false | no |
| rate=1000 | 120000 | 0 | 0 | 0 | 0 | 0 | 4.175 | 6.711 | 22.111 | false | no |
| rate=2000 | 240000 | 0 | 0 | 0 | 0 | 0 | 5.215 | 11.399 | 18.207 | false | no |

## dotwire · latency-ladder · plain · rep 1 · max sustainable 5000.0 msg/s

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rate=100 | 12000 | 108000 | 108000 | 0 | 0 | 0 | 5.167 | 6.747 | 25.567 | false | no |
| rate=500 | 60000 | 540000 | 540000 | 0 | 0 | 0 | 3.093 | 6.243 | 22.847 | false | no |
| rate=1000 | 120000 | 1080000 | 1080000 | 0 | 0 | 0 | 2.837 | 9.127 | 18.943 | false | no |
| rate=2000 | 240000 | 2160000 | 2160000 | 0 | 0 | 0 | 3.039 | 16.543 | 48.895 | false | no |
| rate=5000 | 600000 | 5400000 | 5400000 | 0 | 0 | 0 | 18.911 | 119.743 | 263.167 | false | no |
| rate=10000 | 676826 | 6091434 | 6091434 | 0 | 0 | 0 | 1780.735 | 2271.231 | 2639.871 | true | no |
| rate=20000 | 688650 | 6197850 | 6197850 | 0 | 0 | 0 | 1755.135 | 2367.487 | 3207.167 | true | no |

## dotwire · latency-ladder · plain · rep 2 · max sustainable 5000.0 msg/s

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rate=100 | 12000 | 108000 | 108000 | 0 | 0 | 0 | 4.543 | 6.219 | 24.511 | false | no |
| rate=500 | 60000 | 540000 | 540000 | 0 | 0 | 0 | 3.625 | 5.931 | 114.623 | false | no |
| rate=1000 | 120000 | 1080000 | 1080000 | 0 | 0 | 0 | 2.903 | 9.711 | 28.271 | false | no |
| rate=2000 | 240000 | 2160000 | 2160000 | 0 | 0 | 0 | 2.923 | 14.831 | 70.015 | false | no |
| rate=5000 | 600000 | 5400000 | 5400000 | 0 | 0 | 0 | 17.199 | 100.991 | 242.943 | false | no |
| rate=10000 | 685505 | 6169545 | 6169545 | 0 | 0 | 0 | 1761.279 | 2177.023 | 2637.823 | true | no |
| rate=20000 | 616221 | 5545989 | 5545989 | 0 | 0 | 0 | 1780.735 | 6672.383 | 7630.847 | true | sched_lag_p99>5000us |

## dotwire · latency-ladder · plain · rep 3 · max sustainable 5000.0 msg/s

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rate=100 | 12000 | 108000 | 108000 | 0 | 0 | 0 | 4.631 | 5.887 | 24.511 | false | no |
| rate=500 | 60000 | 540000 | 540000 | 0 | 0 | 0 | 3.981 | 10.503 | 171.519 | false | no |
| rate=1000 | 120000 | 1080000 | 1080000 | 0 | 0 | 0 | 3.043 | 9.303 | 19.103 | false | no |
| rate=2000 | 240000 | 2160000 | 2160000 | 0 | 0 | 0 | 2.897 | 16.463 | 40.159 | false | no |
| rate=5000 | 600000 | 5400000 | 5400000 | 0 | 0 | 0 | 21.951 | 112.703 | 262.911 | false | no |
| rate=10000 | 644952 | 5804568 | 5804568 | 0 | 0 | 0 | 1882.111 | 2291.711 | 2803.711 | true | no |
| rate=20000 | 606476 | 5458284 | 5458284 | 0 | 0 | 0 | 1835.007 | 6148.095 | 7376.895 | true | sched_lag_p99>5000us |

## dotwire · reconnect-storm · plain · rep 1

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| restart | 0 | 0 | 0 | 0 | 0 | 0 | 3909.631 | 15867.903 | 16736.255 | false | no |

## tinode · latency-ladder · crypt · rep 1 · max sustainable 100.0 msg/s

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rate=100 | 12000 | 108000 | 108000 | 0 | 0 | 0 | 11.487 | 32.719 | 53.311 | false | no |
| rate=500 | 60000 | 540000 | 540000 | 0 | 0 | 0 | 21.951 | 762.879 | 919.039 | false | no |
| rate=1000 | 0 | 0 | 492786 | 0 | 0 | 120000 | 35553.279 | 39256.063 | 39321.599 | false | no |
| rate=2000 | 0 | 0 | 473724 | 0 | 0 | 240000 | 34177.023 | 36929.535 | 36962.303 | false | no |
| rate=5000 | 0 | 0 | 486852 | 0 | 0 | 600000 | 34537.471 | 35913.727 | 35979.263 | false | no |
| rate=10000 | 0 | 0 | 537499 | 0 | 0 | 1200000 | 31653.887 | 33439.743 | 33538.047 | false | no |
| rate=20000 | 0 | 0 | 512973 | 0 | 0 | 2400000 | 32342.015 | 34799.615 | 34865.151 | false | no |

## tinode · latency-ladder · crypt · rep 2 · max sustainable 100.0 msg/s

| step | sent | expected | received | lost | dup | err | p50 ms | p99 ms | p999 ms | saturated | invalid |
|---|---|---|---|---|---|---|---|---|---|---|---|
| rate=100 | 12000 | 108000 | 108000 | 0 | 0 | 0 | 9.951 | 28.543 | 42.591 | false | no |
| rate=500 | 60000 | 540000 | 540000 | 0 | 0 | 0 | 21.071 | 505.855 | 563.711 | false | no |
| rate=1000 | 0 | 0 | 489897 | 0 | 0 | 119992 | 35553.279 | 38535.167 | 38600.703 | true | no |
| rate=2000 | 0 | 0 | 478305 | 0 | 0 | 240000 | 34832.383 | 35684.351 | 35749.887 | false | no |
| rate=5000 | 0 | 0 | 511227 | 0 | 0 | 600000 | 32931.839 | 34078.719 | 34144.255 | false | no |
| rate=10000 | 0 | 0 | 530424 | 0 | 0 | 1200000 | 31637.503 | 32915.455 | 32964.607 | false | no |
| rate=20000 | 0 | 0 | 572697 | 0 | 0 | 2400000 | 30425.087 | 31571.967 | 31703.039 | false | no |

## Missing summaries (in progress or failed)

- dotwire-reconnect-storm-plain/rep-2
