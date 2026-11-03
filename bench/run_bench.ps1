<#
.SYNOPSIS
  Lance les paliers de charge (latence + debit) pour UN moteur.
  Le conteneur de mesure demarre AVANT le producer et le script attend
  qu'il soit pret : le timing de demarrage de Docker n'a plus d'importance.
.EXAMPLE
  cd C:\Users\Fallou\Documents\veille-tech
  .\bench\run_bench.ps1 -Engine flink
  .\bench\run_bench.ps1 -Engine spark
  # Test court de verification (5 min) :
  .\bench\run_bench.ps1 -Engine flink -Rates 500 -ProducerDuration 200 -TagPrefix T
  # Evenements tardifs (optionnel) :
  .\bench\run_bench.ps1 -Engine flink -Rates 1000 -LateRatio 0.1 -WatermarkSeconds 30 -TagPrefix L
#>
param(
    [Parameter(Mandatory = $true)][ValidateSet("spark", "flink")][string]$Engine,
    [int[]]$Rates = @(500, 1000, 2000),
    [int]$ProducerDuration = 360,
    [double]$LateRatio = 0.0,
    [int]$WatermarkSeconds = 10,
    [string]$TagPrefix = "P",
    [int]$Warmup = 0,            # ignore (compatibilite avec l'ancienne version)
    [int]$MeasureDuration = 0    # ignore (compatibilite avec l'ancienne version)
)
. (Join-Path $PSScriptRoot "bench_lib.ps1")

Start-Stack
$n = 0
foreach ($rate in $Rates) {
    $n++
    $palier = "$TagPrefix$n"
    Write-Host ""
    Write-Host "=== $Engine / $palier : $rate ev/s ($(Get-Date -Format 'HH:mm:ss')) ==="
    Stop-AllJobs
    Submit-EngineJob -Engine $Engine -WatermarkSeconds $WatermarkSeconds

    $name = "measure_${Engine}_$palier"
    Start-MeasureDetached -Name $name -Engine $Engine -Palier $palier -Rate $rate -Duration ($ProducerDuration + 20)
    Wait-MeasureReady $name

    $log = Join-Path $LogDir "producer_${Engine}_${palier}.log"
    $prod = Start-Producer -Rate $rate -Duration $ProducerDuration -LateRatio $LateRatio -LogFile $log
    Write-Host "[producer] lance pour $ProducerDuration s ($(Get-Date -Format 'HH:mm:ss'))"
    $prod.WaitForExit()
    Wait-MeasureEnd $name
    Start-Sleep 5
    Write-Host "[producer] $(Get-Content $log -Tail 1)"
}
Stop-AllJobs
Write-Host ""
Write-Host "Termine. Lance ensuite : python bench\analyse.py"
