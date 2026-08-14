<#
.SYNOPSIS
  Two-phase fetch + stage for a whole competition-season. Never publishes.

.DESCRIPTION
  Implements the runbook's "Two-phase large load", separating provider traffic
  from local processing:

    Phase 1 (fetch) : --fetch-only, 20-game batches with a cooldown between
                      them, one collector worker. Caches box scores and PBP
                      under data/raw/. No staging, no database.
    Phase 2 (stage) : --skip-fetch, one stage worker. Rebuilds the combined PBP
                      CSV from the validated per-game cache and writes one
                      checkpoint per game under data/staging/. No network, no
                      database.

  NOTHING HERE PUBLISHES. --execute is deliberately absent from both phases;
  publication is a separate, explicitly approved step. Do not add it here.

  Both phases are restartable. Phase 1 skips games already cached, phase 2
  reuses valid checkpoints, so re-running after an interruption only redoes
  what is genuinely missing.

.PARAMETER Games
  Range spec passed straight through: '1-195', '1,2,3', or '1-10,25,40-42'.
  EuroCup 2025-26 is gamecodes 1-195, contiguous (180 RS + 15 playoff games).

.PARAMETER Phase
  'all'   - fetch, then stage (default)
  'fetch' - phase 1 only; use to restart an interrupted download
  'stage' - phase 2 only; use when everything is already cached

.PARAMETER AllowMissingInputs
  Let phase 2 skip games whose PBP is missing or invalid, with a warning,
  instead of failing. Off by default so a gap is loud the first time. Turn it
  on only after you have looked at what is missing and decided to proceed.

.EXAMPLE
  .\scripts\fetch_and_stage_season.ps1
  Fetch and stage all 195 EuroCup 2025-26 games.

.EXAMPLE
  .\scripts\fetch_and_stage_season.ps1 -Phase fetch
  Resume an interrupted download; cached games are skipped.

.EXAMPLE
  .\scripts\fetch_and_stage_season.ps1 -Phase stage -AllowMissingInputs
  Stage what is cached, tolerating games with no PBP.
#>
[CmdletBinding()]
param(
  [string] $Games       = '1-195',
  [int]    $Season      = 2025,
  [string] $Competition = 'U',
  [int]    $BatchSize   = 20,
  [int]    $BatchSleep  = 60,
  [ValidateSet('all', 'fetch', 'stage')]
  [string] $Phase       = 'all',
  [switch] $AllowMissingInputs
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Python   = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$Loader   = Join-Path $RepoRoot 'scripts\load_games.py'

if (-not (Test-Path $Python)) { throw "python not found at $Python -- is .venv created?" }
if (-not (Test-Path $Loader)) { throw "load_games.py not found at $Loader" }

Set-Location $RepoRoot

function Write-Banner {
  param([string] $Text)
  Write-Host ''
  Write-Host ('=' * 72) -ForegroundColor DarkGray
  Write-Host $Text -ForegroundColor Cyan
  Write-Host ('=' * 72) -ForegroundColor DarkGray
}

$compLabel = 'EuroLeague'
if ($Competition -eq 'U') { $compLabel = 'EuroCup' }

Write-Banner "$compLabel $Season -- games $Games -- phase: $Phase"
Write-Host "repo    : $RepoRoot"
Write-Host "python  : $Python"
Write-Host "batches : $BatchSize games, ${BatchSleep}s cooldown between them"
Write-Host "publish : NO -- this script never passes --execute"
$started = Get-Date

# ---------------------------------------------------------------------------
# Phase 1 -- fetch only. Provider traffic, no staging, no database.
# ---------------------------------------------------------------------------
if ($Phase -eq 'all' -or $Phase -eq 'fetch') {
  Write-Banner 'PHASE 1/2 -- fetching box scores and play-by-play'

  & $Python $Loader `
    --games $Games `
    --season $Season `
    --competition $Competition `
    --fetch-only `
    --collect-workers 1 `
    --fetch-batch-size $BatchSize `
    --fetch-batch-sleep $BatchSleep

  if ($LASTEXITCODE -ne 0) {
    Write-Host ''
    Write-Host "PHASE 1 FAILED (exit $LASTEXITCODE)." -ForegroundColor Red
    Write-Host 'Cached games are kept. Re-run to resume:' -ForegroundColor Yellow
    Write-Host "  .\scripts\fetch_and_stage_season.ps1 -Phase fetch -Games '$Games'" -ForegroundColor Yellow
    exit $LASTEXITCODE
  }

  Write-Host ''
  Write-Host 'Phase 1 complete -- all payloads cached under data/raw/.' -ForegroundColor Green
}

# ---------------------------------------------------------------------------
# Phase 2 -- stage from cache. No network, no database.
# ---------------------------------------------------------------------------
if ($Phase -eq 'all' -or $Phase -eq 'stage') {
  Write-Banner 'PHASE 2/2 -- staging checkpoints from cached payloads'

  $stageArgs = @(
    $Loader,
    '--games', $Games,
    '--season', $Season,
    '--competition', $Competition,
    '--skip-fetch',
    '--stage-workers', '1'
  )
  if ($AllowMissingInputs) {
    $stageArgs += '--allow-missing-inputs'
    Write-Host 'NOTE: --allow-missing-inputs is ON; games without PBP are skipped with a warning.' -ForegroundColor Yellow
  }

  & $Python $stageArgs

  if ($LASTEXITCODE -ne 0) {
    Write-Host ''
    Write-Host "PHASE 2 FAILED (exit $LASTEXITCODE)." -ForegroundColor Red
    Write-Host 'Valid checkpoints are kept. Common causes:' -ForegroundColor Yellow
    Write-Host '  * a game has no cached PBP     -> re-run -Phase fetch, or pass -AllowMissingInputs' -ForegroundColor Yellow
    Write-Host '  * "checkpoint failed integrity" -> delete that checkpoint dir and re-run -Phase stage' -ForegroundColor Yellow
    exit $LASTEXITCODE
  }

  Write-Host ''
  Write-Host 'Phase 2 complete -- checkpoints written under data/staging/.' -ForegroundColor Green
}

$elapsed = (Get-Date) - $started
Write-Banner ('DONE in {0:hh\:mm\:ss}' -f $elapsed)
Write-Host 'Nothing was written to the database.' -ForegroundColor Green
Write-Host 'Inspect the staging output, then publish as a separate, deliberate step.'
