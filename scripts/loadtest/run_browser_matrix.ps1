param(
  [int[]]$Users = @(5, 10, 20),
  [int[]]$CadenceSeconds = @(10, 30, 60),
  [int]$ActionsPerUser = 4,
  [int]$Seed = 20260910,
  [string]$OutputRoot = "output/loadtest/browser-runs",
  [string]$SessionName = "loadtest"
)

$ErrorActionPreference = "Stop"
$npx = "C:/Program Files/nodejs/npx.cmd"
$scenarioFile = "scripts/loadtest/browser_scenario.js"
$runStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $OutputRoot $runStamp
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

& $npx --yes --package '@playwright/cli' playwright-cli "-s=$SessionName" close 2>$null | Out-Null
& $npx --yes --package '@playwright/cli' playwright-cli "-s=$SessionName" open about:blank | Out-Null

$summaryRows = @()
foreach ($cadence in $CadenceSeconds) {
  foreach ($userCount in $Users) {
    $scenarioSeed = $Seed + $cadence * 1000 + $userCount
    $scenario = "users-{0:D2}_cadence-{1:D2}s" -f $userCount, $cadence
    $configUrl = "about:blank#users=$userCount,cadence=$cadence,actions=$ActionsPerUser,seed=$scenarioSeed"
    Write-Host "Starting $scenario (seed $scenarioSeed)"

    & $npx --yes --package '@playwright/cli' playwright-cli "-s=$SessionName" goto $configUrl | Out-Null
    $rawOutput = @(& $npx --yes --package '@playwright/cli' playwright-cli --raw "-s=$SessionName" `
      run-code --filename $scenarioFile 2>&1)
    $rawPath = Join-Path $runDir "$scenario.raw.log"
    $rawOutput | Set-Content -LiteralPath $rawPath -Encoding UTF8

    $jsonLine = $rawOutput | Where-Object { $_ -match '^"\{\\"config\\"' } | Select-Object -Last 1
    if (-not $jsonLine) {
      throw "No result JSON returned for $scenario; see $rawPath"
    }
    $jsonText = $jsonLine | ConvertFrom-Json
    $result = $jsonText | ConvertFrom-Json
    $resultPath = Join-Path $runDir "$scenario.json"
    $result | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $resultPath -Encoding UTF8

    $summaryRows += [pscustomobject]@{
      users = $userCount
      cadence_seconds = $cadence
      seed = $scenarioSeed
      successful_users = $result.summary.successful_users
      failed_users = $result.summary.failed_users
      completed_actions = $result.summary.completed_actions
      expected_actions = $result.summary.expected_actions
      nav_median_ms = $result.summary.navigation_ms.median
      nav_p90_ms = $result.summary.navigation_ms.p90
      nav_max_ms = $result.summary.navigation_ms.max
      action_median_ms = $result.summary.action_latency_ms.median
      action_p90_ms = $result.summary.action_latency_ms.p90
      action_p95_ms = $result.summary.action_latency_ms.p95
      action_max_ms = $result.summary.action_latency_ms.max
      http_errors = $result.summary.http_error_count
      request_failures = $result.summary.request_failure_count
      console_errors = $result.summary.console_error_count
    }
    Write-Host "Finished $scenario`: $($result.summary.successful_users)/$userCount users, p95 $($result.summary.action_latency_ms.p95) ms"
  }
}

$summaryPath = Join-Path $runDir "summary.csv"
$summaryRows | Export-Csv -LiteralPath $summaryPath -NoTypeInformation -Encoding UTF8
& $npx --yes --package '@playwright/cli' playwright-cli "-s=$SessionName" close | Out-Null
Write-Output $runDir
