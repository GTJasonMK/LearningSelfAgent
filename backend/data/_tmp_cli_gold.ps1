$ErrorActionPreference = 'Stop'
$repo = 'E:\Code\LearningSelfAgent'
$py = 'E:\Code\LearningSelfAgent\.venv-win\Scripts\python.exe'
$hostName = '127.0.0.1'
$port = 8135
$backendOut = 'E:\Code\LearningSelfAgent\backend\data\_tmp_backend_8135.out.log'
$backendErr = 'E:\Code\LearningSelfAgent\backend\data\_tmp_backend_8135.err.log'
$cliOut = 'E:\Code\LearningSelfAgent\backend\data\_tmp_cli_gold.out.log'
$cliErr = 'E:\Code\LearningSelfAgent\backend\data\_tmp_cli_gold.err.log'
foreach ($f in @($backendOut, $backendErr, $cliOut, $cliErr)) { if (Test-Path $f) { Remove-Item $f -Force } }
$backendArgs = @('-m','uvicorn','backend.src.main:app','--host',$hostName,'--port',"$port")
$backend = Start-Process -FilePath $py -ArgumentList $backendArgs -WorkingDirectory $repo -RedirectStandardOutput $backendOut -RedirectStandardError $backendErr -PassThru
try {
  $ok = $false
  for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Seconds 1
    try {
      $resp = Invoke-WebRequest -UseBasicParsing -Uri ("http://{0}:{1}/api/health" -f $hostName, $port) -TimeoutSec 2
      if ($resp.StatusCode -eq 200) { $ok = $true; break }
    } catch {}
  }
  if (-not $ok) {
    Write-Output 'BACKEND_START_FAILED'
    exit 2
  }
  $cliArgs = @('scripts/lsa.py','--host',$hostName,'--port',"$port",'ask','请你帮我收集最近三个月的黄金的价格数据，单位元/克，并保存为csv文件','--model','deepseek-chat')
  $cli = Start-Process -FilePath $py -ArgumentList $cliArgs -WorkingDirectory $repo -RedirectStandardOutput $cliOut -RedirectStandardError $cliErr -PassThru -Wait
  Write-Output ("CLI_EXIT_CODE={0}" -f $cli.ExitCode)
} finally {
  if ($backend -and -not $backend.HasExited) {
    Stop-Process -Id $backend.Id -Force
    Start-Sleep -Seconds 1
  }
}
Write-Output '=== CLI OUT ==='
if (Test-Path $cliOut) { Get-Content $cliOut -Tail 400 }
Write-Output '=== CLI ERR ==='
if (Test-Path $cliErr) { Get-Content $cliErr -Tail 400 }
Write-Output '=== BACKEND OUT ==='
if (Test-Path $backendOut) { Get-Content $backendOut -Tail 200 }
Write-Output '=== BACKEND ERR ==='
if (Test-Path $backendErr) { Get-Content $backendErr -Tail 200 }
