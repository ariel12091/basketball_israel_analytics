# Disk-space baseline before restart

Captured 2026-10-01 02:43:57 +03:00 (Israel local time). No files were removed.

## Drive

| Drive | Total bytes | Used bytes | Free bytes | Free MiB |
|---|---:|---:|---:|---:|
| C: | 1,022,902,718,464 | 1,022,827,679,744 | 75,038,720 | 71.56 |

## Selected folders

These are comparison points, not a ranking or a complete inventory of C:.
Sizes are the sum of accessible files under each path; permissions and files
changing during the scan can affect the totals.

| Folder | Bytes | Approx. MiB | Files |
|---|---:|---:|---:|
| `C:\Users\ariel\AppData\Local\npm-cache` | 172,651,032 | 164.65 | 444 |
| `C:\Users\ariel\AppData\Local\Temp` | 365,284,982 | 348.36 | 9,615 |
| `C:\Users\ariel\AppData\Local\Temp\mozilla-temp-files` | 131,072 | 0.13 | 2 |
| `C:\Users\ariel\AppData\Local\Mozilla\Firefox` | 2,999,700,931 | 2,860.74 | 37,347 |
| `C:\tmp` | 129,735,446 | 123.73 | 4,932 |
| `C:\Users\ariel\.codex\.tmp` | 147,082,261 | 140.27 | 6,144 |
| `C:\Users\ariel\.codex\tmp` | 848 | <0.01 | 9 |
| `C:\Users\ariel\.codex` | 1,803,959,874 | 1,720.39 | 8,032 |
| `C:\Windows\Temp` | 0 accessible | 0 | 0 |
| `C:\$Recycle.Bin` | 129 accessible | <0.01 | 1 |

The measured `.codex` folder includes `sessions` (628,115,520 bytes),
`.sandbox-bin` (389,323,808), `plugins` (365,111,165), `.tmp` (147,082,261),
and `.sandbox` (53,615,770). These are included in the `.codex` total above.
Both `%TEMP%` and `%TMP%` point to `C:\Users\ariel\AppData\Local\Temp`.
The `.codex\.tmp` and `.codex\tmp` rows are included in the `.codex` total;
do not add them to it when comparing totals. Temporary files may persist
through a restart, so compare the measured bytes afterward.

Firefox was measured shortly after the drive baseline. Its four profile
`cache2` folders held 2,869,130,733 bytes (2,736.22 MiB) in total:

| Firefox profile | `cache2` bytes | Approx. MiB |
|---|---:|---:|
| `12flnxll.default-release-1713615261816` | 522,835,219 | 498.61 |
| `2GT1KrYU.Profile 1` | 213,322,719 | 203.44 |
| `4hppjz6d.default-release-1` | 1,068,859,859 | 1,019.34 |
| `uqv27rvy.default` | 1,064,112,936 | 1,014.82 |

The profile caches are included in the Firefox total; `mozilla-temp-files` is
included in the user Temp total. Firefox may retain its disk cache through a
restart, so compare these folders afterward rather than assuming they vanish.

## After-restart comparison

Run the same `Win32_LogicalDisk` check for C: and remeasure the named folders.
Compare free bytes first; folder totals can change as Codex and npm run.
