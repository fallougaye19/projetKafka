# Fonctions communes aux scripts de benchmark (charge par run_bench.ps1 / run_panne.ps1)

$Root    = Split-Path -Parent $PSScriptRoot          # ...\veille-tech
$Compose = Join-Path $Root "docker\docker-compose.yaml"
$Net     = "docker_streaming-net"
$LogDir  = Join-Path $PSScriptRoot "results\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# Heure lue DANS Docker (meme horloge que le conteneur de mesure : pas de decalage
# entre l'horloge Windows et celle de la VM Docker). Repli sur l'heure Windows.
function Get-Epoch {
    try { return [double](docker exec kafka date +%s 2>$null) }
    catch { return [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0 }
}

function Wait-Kafka {
    for ($i = 0; $i -lt 60; $i++) {
        $s = docker inspect -f "{{.State.Health.Status}}" kafka 2>$null
        if ($s -eq "healthy") { return }
        Start-Sleep 5
    }
    throw "Kafka n'est pas devenu 'healthy' (docker logs kafka)"
}

function Start-Stack {
    Write-Host "[stack] demarrage / verification des conteneurs"
    Build-MeasureImage
    docker compose -f $Compose up -d kafka spark-master spark-worker flink-jobmanager flink-taskmanager | Out-Null
    Wait-Kafka
    for ($i = 0; $i -lt 40; $i++) {
        try {
            $o = Invoke-RestMethod "http://localhost:8081/overview" -TimeoutSec 3
            if ($o.taskmanagers -ge 1) { break }
        } catch {}
        Start-Sleep 3
    }
    for ($i = 0; $i -lt 40; $i++) {
        try {
            $m = Invoke-RestMethod "http://localhost:8080/json/" -TimeoutSec 3
            if (@($m.workers | Where-Object { $_.state -eq "ALIVE" }).Count -ge 1) { break }
        } catch {}
        Start-Sleep 3
    }
}

function Build-MeasureImage {
    docker image inspect veille-measure 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[stack] construction de l'image de mesure (une seule fois, quelques minutes)"
        docker build -q -f (Join-Path $PSScriptRoot "Dockerfile.measure") -t veille-measure $PSScriptRoot | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Echec de la construction de l'image veille-measure" }
    }
}

function Stop-AllJobs {
    # Flink : annule tous les jobs actifs (API REST)
    try {
        $jobs = (Invoke-RestMethod "http://localhost:8081/jobs/overview" -TimeoutSec 5).jobs
        foreach ($j in $jobs) {
            if ($j.state -in @("RUNNING", "RESTARTING", "CREATED")) {
                Invoke-RestMethod -Method Patch "http://localhost:8081/jobs/$($j.jid)?mode=cancel" | Out-Null
            }
        }
    } catch {}
    # Spark : tue le driver (les crochets evitent que pkill se tue lui-meme)
    docker exec spark-master bash -c "pkill -f '[S]parkSubmit'; pkill -f '[s]park_job.py'; true" 2>$null | Out-Null
    # attendre que Flink n'ait plus aucun job actif (sinon il garde les slots)
    for ($i = 0; $i -lt 20; $i++) {
        try {
            if ((Invoke-RestMethod "http://localhost:8081/overview" -TimeoutSec 3).'jobs-running' -eq 0) { break }
        } catch { break }
        Start-Sleep 3
    }
    Start-Sleep 5
}

function Submit-EngineJob {
    param([string]$Engine, [int]$WatermarkSeconds = 10, [string]$CheckpointId = "")
    if ($Engine -eq "flink") {
        $wm = "WATERMARK_DELAY_S=$WatermarkSeconds"
        $out = docker exec -e $wm flink-jobmanager flink run -d -py /opt/job/flink_job.py 2>&1 | Out-String
        if (-not ($out -match 'JobID ([0-9a-f]{32})')) { throw "Soumission Flink echouee : $out" }
        $jid = $Matches[1]
        for ($i = 0; $i -lt 40; $i++) {
            Start-Sleep 3
            try {
                $d = Invoke-RestMethod "http://localhost:8081/jobs/$jid"
                $bad = @($d.vertices | Where-Object { $_.tasks.RUNNING -lt $_.parallelism }).Count
                if (($d.state -eq "RUNNING") -and ($bad -eq 0)) {
                    Start-Sleep 10
                    Write-Host "[flink] job $jid : toutes les taches RUNNING"
                    return
                }
            } catch {}
        }
        $ov = Invoke-RestMethod "http://localhost:8081/overview"
        Write-Host "[flink] ECHEC : slots-total=$($ov.'slots-total') slots-available=$($ov.'slots-available') jobs-running=$($ov.'jobs-running')"
        throw "Les taches Flink ne sont pas toutes RUNNING (un ancien job occupe peut-etre les slots) - voir http://localhost:8081"
    }
    else {
        if ($CheckpointId -eq "") { $CheckpointId = Get-Date -Format "yyyyMMdd-HHmmss" }
        $ck = "CHECKPOINT_PATH=/tmp/checkpoints/spark-$CheckpointId"
        $wm = "WATERMARK_DELAY=$WatermarkSeconds seconds"
        $cmd = "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/job/spark_job.py > /tmp/spark-$CheckpointId.log 2>&1"
        docker exec -d -e $ck -e $wm spark-master bash -c $cmd
        for ($i = 0; $i -lt 90; $i++) {
            Start-Sleep 3
            try {
                $m = Invoke-RestMethod "http://localhost:8080/json/"
                if (@($m.activeapps).Count -ge 1) {
                    Start-Sleep 25
                    Write-Host "[spark] application active"
                    return
                }
            } catch {}
        }
        throw "Le job Spark n'a pas demarre (docker exec spark-master bash -c 'tail -50 /tmp/spark-*.log')"
    }
}

function Start-Producer {
    param([int]$Rate, [int]$Duration, [double]$LateRatio, [string]$LogFile)
    $argList = @("compose", "-f", $Compose, "run", "--rm", "-T",
                 "-e", "EVENTS_PER_SECOND=$Rate",
                 "-e", "DURATION_SECONDS=$Duration",
                 "-e", "LATE_EVENT_RATIO=$LateRatio",
                 "producer")
    Start-Process -FilePath "docker" -ArgumentList $argList -PassThru -NoNewWindow `
        -RedirectStandardOutput $LogFile -RedirectStandardError "$LogFile.err"
}

# Producer lance en arriere-plan par Docker lui-meme : le script PowerShell n'est
# JAMAIS bloque par le producer (les runs precedents montraient que les etapes
# suivant Start-Process attendaient la fin du producer).
function Start-ProducerDetached {
    param([string]$Name, [int]$Rate, [int]$Duration, [double]$LateRatio)
    docker rm -f $Name 2>$null | Out-Null
    docker compose -f $Compose run -d --name $Name -e "EVENTS_PER_SECOND=$Rate" -e "DURATION_SECONDS=$Duration" -e "LATE_EVENT_RATIO=$LateRatio" producer | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Impossible de demarrer le producer" }
}

function Test-ContainerRunning {
    param([string]$Name)
    return ((docker inspect -f "{{.State.Running}}" $Name 2>$null) -eq "true")
}

function Wait-ProducerEnd {
    param([string]$Name, [string]$LogFile)
    docker wait $Name | Out-Null
    docker logs $Name 2>&1 | Out-File -Encoding utf8 $LogFile
    docker rm $Name | Out-Null
}

function Get-MeasureArgs {
    param([string]$Engine, [string]$Palier, [int]$Rate, [int]$Duration)
    $vol = "${Root}\bench:/bench"
    return @("--network", $Net, "-v", $vol, "-w", "/bench",
             "-e", "MOTEUR=$Engine", "-e", "PALIER=$Palier", "-e", "RATE=$Rate",
             "-e", "MEASURE_DURATION=$Duration",
             "veille-measure", "python", "measure_latency.py")
}

function Start-MeasureDetached {
    param([string]$Name, [string]$Engine, [string]$Palier, [int]$Rate, [int]$Duration)
    docker rm -f $Name 2>$null | Out-Null
    $a = Get-MeasureArgs $Engine $Palier $Rate $Duration
    docker run -d --name $Name @a | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Impossible de demarrer le conteneur de mesure" }
}

# Attend que le consommateur soit assigne et positionne (marqueur PRET dans ses logs)
function Wait-MeasureReady {
    param([string]$Name, [int]$TimeoutS = 300)
    $t0 = Get-Date
    while (((Get-Date) - $t0).TotalSeconds -lt $TimeoutS) {
        $log = docker logs $Name 2>&1 | Out-String
        if ($log -match "PRET") {
            Write-Host "[measure] pret apres $([int]((Get-Date) - $t0).TotalSeconds) s"
            Write-Host ($log.Trim())
            return
        }
        Start-Sleep 2
    }
    throw "Le conteneur de mesure n'est pas pret apres $TimeoutS s (docker logs $Name)"
}

function Wait-MeasureEnd {
    param([string]$Name)
    docker wait $Name | Out-Null
    docker logs $Name --tail 8
    docker rm $Name | Out-Null
}
