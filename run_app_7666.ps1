$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $PSScriptRoot

Get-NetTCPConnection `
  -LocalPort 7666 `
  -State Listen `
  -ErrorAction SilentlyContinue |
  ForEach-Object {
    Stop-Process -Id $_.OwningProcess -Force
  }

Start-Sleep -Seconds 1

$env:IBPL_CACHE_UI = "false"
$env:IBPL_CACHE_UI_HTML = "false"

& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" `
  -e "shiny::runApp('app', host='127.0.0.1', port=7666, launch.browser=TRUE)"
