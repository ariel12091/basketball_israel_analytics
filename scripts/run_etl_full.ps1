param(
  [string]$BaseDir = "",
  [switch]$SkipDryRun,
  [switch]$DryRunOnly
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($BaseDir)) {
  $base = (Resolve-Path (Join-Path $scriptDir "..")).Path
} else {
  $base = (Resolve-Path $BaseDir).Path
}

$logDir = Join-Path $base 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$runStamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = Join-Path $logDir ("etl_full_wrapper_{0}.log" -f $runStamp)
$etlLogDir = Join-Path $base 'etl\logs'
$lockFile = Join-Path $logDir 'etl_full_wrapper.lock'

$exe = $env:RSCRIPT_PATH
if ([string]::IsNullOrWhiteSpace($exe)) {
  $cmd = Get-Command Rscript -ErrorAction SilentlyContinue
  if ($null -ne $cmd -and -not [string]::IsNullOrWhiteSpace($cmd.Source)) {
    $exe = $cmd.Source
  } else {
    throw "Rscript executable not found. Set RSCRIPT_PATH or add Rscript to PATH."
  }
}

$appEnv = if ([string]::IsNullOrWhiteSpace($env:APP_ENV)) { 'test' } else { $env:APP_ENV }
$etlFile = (Join-Path $base 'etl\etl_full.R').Replace('\\', '/')

if ($DryRunOnly.IsPresent) {
  $expr = "Sys.setenv(APP_ENV='$appEnv'); source('$etlFile'); etl_full(dry_run=TRUE)"
} elseif ($SkipDryRun.IsPresent) {
  $expr = "Sys.setenv(APP_ENV='$appEnv'); source('$etlFile'); etl_full(dry_run=FALSE)"
} else {
  $expr = "Sys.setenv(APP_ENV='$appEnv'); source('$etlFile'); etl_full(dry_run=TRUE); etl_full(dry_run=FALSE)"
}

$lockStream = $null
$stdoutFile = Join-Path $logDir ("etl_full_wrapper_stdout_{0}.tmp" -f $runStamp)
$stderrFile = Join-Path $logDir ("etl_full_wrapper_stderr_{0}.tmp" -f $runStamp)
$exitCode = 1

try {
  try {
    $lockStream = [System.IO.File]::Open($lockFile, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
  } catch {
    "$(Get-Date -Format s) Another ETL wrapper instance is already running. Exiting." | Out-File -FilePath $logFile -Encoding UTF8
    exit 99
  }

  $argString = "-e `"$expr`""
  $p = Start-Process -FilePath $exe -ArgumentList $argString -WorkingDirectory $base -NoNewWindow -PassThru -Wait -RedirectStandardOutput $stdoutFile -RedirectStandardError $stderrFile
  $exitCode = [int]$p.ExitCode

  $meta = @(
    "timestamp=$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "process_id=$($p.Id)",
    "exit_code=$exitCode",
    ""
  ) -join [Environment]::NewLine

  $stdout = if (Test-Path $stdoutFile) { Get-Content -Path $stdoutFile -Raw } else { '' }
  $stderr = if (Test-Path $stderrFile) { Get-Content -Path $stderrFile -Raw } else { '' }
  ($meta + $stdout + [Environment]::NewLine + $stderr) | Out-File -FilePath $logFile -Encoding UTF8

  if ($exitCode -eq 0) {
    $lastSuccess = Join-Path $etlLogDir 'last_success.txt'
    if (-not (Test-Path $etlLogDir)) { New-Item -ItemType Directory -Path $etlLogDir | Out-Null }
    (Get-Date).ToString('yyyy-MM-dd HH:mm:ss') | Set-Content -Path $lastSuccess -Encoding UTF8
  }

  $cutoff = (Get-Date).AddDays(-2)
  if (Test-Path $logDir) {
    Get-ChildItem $logDir -Filter 'etl_full_wrapper_*.log' | Where-Object { $_.LastWriteTime -lt $cutoff } | Remove-Item -Force
  }
  if (Test-Path $etlLogDir) {
    Get-ChildItem $etlLogDir -Filter 'etl_full_*.log' | Where-Object { $_.LastWriteTime -lt $cutoff } | Remove-Item -Force
  }

  exit $exitCode
}
finally {
  if ($lockStream -ne $null) {
    $lockStream.Close()
    $lockStream.Dispose()
  }
  Remove-Item -Path $lockFile -Force -ErrorAction SilentlyContinue
  Remove-Item -Path $stdoutFile -Force -ErrorAction SilentlyContinue
  Remove-Item -Path $stderrFile -Force -ErrorAction SilentlyContinue
}
