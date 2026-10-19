# Disk-space comparison after restart

Compared with `disk_space_baseline_2026-10-01.md` (02:43:57 +03:00).
Current measurements taken 2026-10-01 around 02:52–02:53 +03:00.
Read-only investigation; no files were removed.

## Drive

| Metric | Before | After | Change |
|---|---:|---:|---:|
| C: total bytes | 1,022,902,718,464 | 1,022,902,718,464 | 0 |
| C: free bytes | 75,038,720 | 18,387,800,064 | +18,312,761,344 |
| C: free GiB | 0.070 | 17.125 | +17.055 |

## Selected folders

These are sums of accessible file lengths, matching the baseline method.
Live files can change during measurement. Nested rows must not be added to
their parent totals.

| Folder | Before bytes | After bytes | Change MiB |
|---|---:|---:|---:|
| User npm-cache | 172,651,032 | 334,406,623 | +154.26 |
| User Temp | 365,284,982 | 309,044,784 | -53.63 |
| Temp/mozilla-temp-files (included above) | 131,072 | 0 | -0.13 |
| Local Mozilla/Firefox | 2,999,700,931 | 2,985,077,871 | -13.95 |
| C:/tmp | 129,735,446 | 129,735,446 | 0 |
| .codex/.tmp (included below) | 147,082,261 | 147,082,261 | 0 |
| .codex/tmp (included below) | 848 | 1,146 | <+0.01 |
| .codex | 1,803,959,874 | 2,267,431,699 | +442.00 |

Windows/Temp and the Recycle Bin were inaccessible in this scan; their
contents cannot be compared reliably. Three access errors also occurred
within .codex, so its measured size is an accessible-file total.

## Details

- A new `.codex/packages/app-server-daemon` directory was created at
  02:50:43 +03:00. The packages folder now holds 448,279,457 bytes
  (427.51 MiB), accounting for most of the measured .codex increase.
  The baseline did not record a separate packages total.
- .codex sessions grew by about 0.25 MiB at the initial scan,
  .sandbox-bin by 7.83 MiB, and plugins by 0.22 MiB.
- npm-cache contains a 161,739,572-byte cache object modified around
  02:50:36 +03:00, accounting for nearly all its measured increase.
- Firefox's `12flnxll.default-release-1713615261816/cache2` shrank by
  14,623,060 bytes (13.95 MiB). The other three recorded profile caches
  have exactly the same byte totals as before.
- C:/tmp and .codex/.tmp retained their previous byte and file totals.

## What explains the recovered space?

The selected non-overlapping folder totals grew by approximately 528.68 MiB
overall. Their changes therefore do not explain the 17.055 GiB gain in free
space; the major reduction happened outside these measured folders.

Windows reports a boot time around 02:48 +03:00, between the two snapshots.
Current root system files are:

| File | Current bytes | Current GiB |
|---|---:|---:|
| pagefile.sys | 16,905,961,472 | 15.745 |
| hiberfil.sys | 6,762,381,312 | 6.298 |
| swapfile.sys | 16,777,216 | 0.016 |

These files have modification times around the restart. The earlier snapshot
did not measure them, so a change in their sizes cannot be established.
Restart-related system-file changes are a possible explanation, but the
available baseline cannot identify the exact source of the recovered space.
