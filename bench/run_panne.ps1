<#
.SYNOPSIS
  Test de panne : arrete le conteneur d'execution (TaskManager Flink / worker Spark)
  pendant que le flux tourne, le redemarre, et enregistre la reprise.
.EXAMPLE
  cd C:\Users\Fallou\Documents\veille-tech
  .\bench\run_panne.ps1 -Engine flink
  .\bench\run_panne.ps1 -Engine spark
#>
param(
    [Parameter(Mandatory = $true)][ValidateSet("spark", "flink")][string]$Engine,
    [int]$Rate = 1000,
    [int]$ProducerDuration = 500,
    [int]$KillAfter = 240,      # secondes apres le demarrage du producer
    [int]$DownSeconds = 15      # duree d'arret du conteneur
)
. (Join-Path $PSScriptRoot "bench_lib.ps1")

$victim = if ($Engine -eq "flink") { "flink-taskmanager" } else { "spark-worker" }
$ckId = "panne-" + (Get-Date -Format "yyyyMMdd-HHmmss")
$events = Join-Path $LogDir "panne_${Engine}_events.txt"

Start-Stack
Stop-AllJobs
Submit-EngineJob -Engine $Engine -CheckpointId $ckId

$name = "measure_panne_$Engine"
Start-MeasureDetached -Name $name -Engine $Engine -Palier "panne" -Rate $Rate -Duration ($ProducerDuration + 20)
Wait-MeasureReady $name

$prodName = "producer_panne_$Engine"
$log = Join-Path $LogDir "producer_${Engine}_panne.log"
$tProd = Get-Epoch
Start-ProducerDetached -Name $prodName -Rate $Rate -Duration $ProducerDuration -LateRatio 0.0
Write-Host "[producer] lance pour $ProducerDuration s ($(Get-Date -Format 'HH:mm:ss')), arret de $victim dans $KillAfter s"

# attente basee sur l'horloge (et non sur un simple Start-Sleep)
while (((Get-Epoch) - $tProd) -lt $KillAfter) { Start-Sleep 5 }

$elapsed = [int]((Get-Epoch) - $tProd)
if (-not (Test-ContainerRunning $prodName)) {
    Write-Host "[ATTENTION] le producer est deja termine : test invalide, a relancer"
}
$tKill = Get-Epoch
docker kill $victim | Out-Null
Write-Host "[panne] $victim ARRETE a +$elapsed s apres le demarrage du producer ($(Get-Date -Format 'HH:mm:ss'))"
Start-Sleep $DownSeconds
docker start $victim | Out-Null
$tStart = Get-Epoch
Write-Host "[panne] $victim REDEMARRE"
"kill=$tKill" | Set-Content $events
"start=$tStart" | Add-Content $events

if ($Engine -eq "spark") {
    # Si le driver est mort (requete en echec), reprise MANUELLE depuis le meme checkpoint
    Start-Sleep 45
    $apps = 0
    try { $apps = @((Invoke-RestMethod "http://localhost:8080/json/").activeapps).Count } catch {}
    if ($apps -eq 0) {
        Write-Host "[spark] application arretee : resoumission depuis le meme checkpoint"
        "resubmit=$(Get-Epoch)" | Add-Content $events
        Submit-EngineJob -Engine "spark" -CheckpointId $ckId
    }
}

Wait-ProducerEnd -Name $prodName -LogFile $log
Wait-MeasureEnd $name
Stop-AllJobs
Write-Host ""
Write-Host "Termine. Lance ensuite : python bench\analyse.py"
