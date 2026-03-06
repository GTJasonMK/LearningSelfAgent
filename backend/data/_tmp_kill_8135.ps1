$targets = Get-CimInstance Win32_Process | Where-Object {
  ($_.CommandLine -like '*_tmp_cli_gold.ps1*') -or
  ($_.CommandLine -like '*backend.src.main:app*--port 8135*') -or
  ($_.CommandLine -like '*scripts/lsa.py*--port 8135*')
}
foreach ($proc in $targets) {
  try {
    Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
    Write-Output ("STOPPED {0} {1}" -f $proc.ProcessId, $proc.Name)
  } catch {}
}
