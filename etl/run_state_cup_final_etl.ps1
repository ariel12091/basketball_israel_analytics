param(
  [int]$Year = 2026,
  [switch]$RunLive,
  [switch]$DryOnly,
  [string]$RepoRoot = 'C:/Users/ariel/Documents/on_off_israel_pbp',
  [string]$RscriptPath = 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe',
  [string]$EnvName = 'test'
)

$ErrorActionPreference = 'Stop'

function Get-StateCupFinalGameId {
  param([int]$SeasonYear)

  $url = "https://basket.co.il/more-games.asp?cYear=$SeasonYear&other_list_id=4"
  $html = (Invoke-WebRequest -UseBasicParsing -Uri $url).Content

  $rows = [regex]::Matches($html, '(?is)<tr class="row (?:odd|even)">.*?</tr>') | ForEach-Object { $_.Value }

  foreach ($row in $rows) {
    $td = [regex]::Matches($row, '(?is)<td[^>]*>(.*?)</td>') | ForEach-Object {
      ($_.Groups[1].Value -replace '(?is)<[^>]+>', ' ' -replace '&quot;', '"' -replace '&nbsp;', ' ' -replace '\s+', ' ').Trim()
    }
    if ($td.Count -lt 4) { continue }

    $stage = $td[0]
    $date = $td[1]

    # final row is 'גמר' (shown as mojibake here) and not '1/2 גמר' / '1/4 גמר'
    $isFinal = ($stage -match '×××¨' -and $stage -notmatch '1/2|1/4')
    if (-not $isFinal) { continue }

    $gameId = $null
    if ($row -match 'game_id=(\d+)') { $gameId = [int]$matches[1] }

    return [pscustomobject]@{
      source_url = $url
      stage_raw = $stage
      game_date_raw = $date
      game_id = $gameId
    }
  }

  return $null
}

function Run-EtlFullByGameId {
  param(
    [int]$GameId,
    [bool]$DryRun
  )

  $dry = if ($DryRun) { 'TRUE' } else { 'FALSE' }
  $cmd = "Sys.setenv(APP_ENV='$EnvName'); setwd('$RepoRoot'); source('etl/etl_full.R'); etl_full(game_ids = c($GameId), dry_run = $dry)"
  & $RscriptPath -e $cmd
}

$final = Get-StateCupFinalGameId -SeasonYear $Year
if ($null -eq $final) {
  throw "Final row was not found on State Cup page for cYear=$Year."
}

Write-Host "Final row found: date=$($final.game_date_raw), stage=$($final.stage_raw)"
Write-Host "Source: $($final.source_url)"

if ($null -eq $final.game_id) {
  throw "Final game_id is not published yet on the page. Re-run after stats link is available."
}

Write-Host "Detected final game_id=$($final.game_id). Running dry-run..."
Run-EtlFullByGameId -GameId $final.game_id -DryRun $true

if ($DryOnly) {
  Write-Host "Dry-run completed. Live ETL skipped (-DryOnly provided)."
}
else {
  Write-Host "Dry-run completed. Running live ETL..."
  Run-EtlFullByGameId -GameId $final.game_id -DryRun $false
}
