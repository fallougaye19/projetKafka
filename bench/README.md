# Benchmark Spark vs Flink : mode d'emploi

Depuis `veille-tech\` (PowerShell, Docker Desktop lancé, Kubernetes désactivé) :

    Set-ExecutionPolicy -Scope Process Bypass
    docker build -f .\bench\Dockerfile.measure -t veille-measure .\bench   # une fois
    .\bench\run_bench.ps1 -Engine flink -Rates 500 -ProducerDuration 200 -TagPrefix T   # test court (5 min)
    .\bench\run_bench.ps1 -Engine flink       # 3 paliers (~25 min)
    .\bench\run_bench.ps1 -Engine spark       # 3 paliers (~25 min)
    .\bench\run_panne.ps1 -Engine flink       # test de panne (~12 min)
    .\bench\run_panne.ps1 -Engine spark
    python bench\analyse.py                   # tableaux + graphiques

Sorties dans `bench\results\` : `raw\` (mesures brutes), `logs\`, `summary.md`,
`fig_latence_vs_charge.png`, `fig_latence_temps.png`, `panne_<moteur>.png`.

Principe : un seul moteur à la fois, job redémarré à chaque palier. Le conteneur de
mesure démarre AVANT le producer et le script attend son marqueur `PRET`.
Workers Spark et TaskManager Flink : mêmes ressources (2 cœurs, 3 Go).
Supprimer `raw\*_T1.csv` après le test court.
