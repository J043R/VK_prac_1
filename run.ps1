param([ValidateSet('all','pipeline','benchmark','demo','report')][string]$Stage='all', [int]$SF=1, [int]$Repeats=5)
$ErrorActionPreference='Stop'
Set-Location -LiteralPath $PSScriptRoot
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    foreach ($candidate in @('C:\Program Files\Docker\Docker\resources\bin', (Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin'))) {
        if (Test-Path -LiteralPath (Join-Path $candidate 'docker.exe')) { $env:Path = "$candidate;$env:Path"; break }
    }
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Нужен Docker Desktop с WSL 2. После установки откройте новый PowerShell.' }
docker info --format '{{.OSType}}'
if ($LASTEXITCODE -ne 0) { throw 'Запустите Docker Desktop и включите Linux containers.' }
if ($SF -lt 1 -or $Repeats -lt 3) { throw 'SF >= 1; Repeats >= 3.' }
New-Item -ItemType Directory -Path results -Force | Out-Null
[pscustomobject]@{
    RAM_GB=[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB,1)
    CPU=(Get-CimInstance Win32_Processor | Select-Object -ExpandProperty Name)
    LogicalCPU=(Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
    FreeDiskGB=[math]::Round((Get-PSDrive C).Free/1GB,1)
} | ConvertTo-Json | Set-Content -LiteralPath results/host.json -Encoding utf8
docker compose up -d minio postgres lakekeeper trino
if ($LASTEXITCODE -ne 0) { throw 'Не удалось поднять сервисы. Проверьте docker compose logs.' }
docker compose ps --format json | Set-Content -LiteralPath results/docker-ps.jsonl -Encoding utf8
docker compose images --format json | Set-Content -LiteralPath results/docker-images.json -Encoding utf8
docker compose run --rm runner --stage $Stage --sf $SF --repeats $Repeats
if ($LASTEXITCODE -ne 0) { throw 'Выполнение остановлено. Причина в results/runs/.../FAILED.txt и логах.' }
if ($Stage -eq 'all') {
    # Trino и Spark работают последовательно: 15 ГБ ОЗУ достаточно для sf1.
    docker compose stop trino
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось освободить память Trino перед Spark.' }
    docker compose run --rm --build spark
    $sparkExit=$LASTEXITCODE
    docker compose start trino
    if ($sparkExit -ne 0) { throw 'Spark не завершился. Проверьте results/spark-bronze.json и журнал консоли.' }
    docker compose run --rm runner --stage report --sf $SF --repeats $Repeats
    if ($LASTEXITCODE -ne 0) { throw 'Не удалось сформировать итоговый отчёт.' }
}
