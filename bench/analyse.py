"""
Analyse des mesures : lit bench/results/raw/*.csv et produit, dans bench/results/ :
  - summary.csv / summary.md       tableau des résultats (à coller dans le rapport)
  - fig_latence_vs_charge.png      p50 / p95 selon la charge, par moteur
  - fig_latence_temps.png          latence par fenêtre au cours du temps
  - panne_<moteur>.png / panne.md  comportement autour de la panne

Usage (depuis veille-tech/bench) :  python analyse.py
Dépendances : pandas, matplotlib
"""
import glob
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")
RAW = os.path.join(RES, "raw")
LOGS = os.path.join(RES, "logs")
COLORS = {"spark": "#e25a1c", "flink": "#2b6cb0"}
WINDOW_S = 60.0
WATERMARK_S = 10.0   # delai de watermark commun aux deux moteurs (plancher de latence)


def producer_rate(moteur, palier):
    """Débit effectif réellement atteint par le producer (lu dans son log)."""
    path = os.path.join(LOGS, f"producer_{moteur}_{palier}.log")
    if not os.path.exists(path):
        return np.nan
    m = re.search(r"effectif (\d+) ev/s", open(path, encoding="utf-8",
                                                      errors="ignore").read())
    return float(m.group(1)) if m else np.nan


def per_window(df):
    g = (df.groupby("window_start")
           .agg(stores=("store_id", "nunique"), tx=("tx_count", "sum"),
                lat_med=("latency_ms", "median"), lat_max=("latency_ms", "max"),
                recv_first=("recv_epoch", "min"))
           .reset_index().sort_values("window_start").reset_index(drop=True))
    return g


def complete_windows(g):
    """Ecarte la 1re fenetre (demarrage du job et du producer : partielle, et
    latence de mise en route) puis toute fenetre restant incomplete."""
    if len(g) > 2:
        g = g.iloc[1:]
    if g.empty:
        return g
    return g[g["tx"] >= 0.8 * g["tx"].median()].reset_index(drop=True)


def analyse_paliers():
    rows, series = [], {}
    for path in sorted(p for p in glob.glob(os.path.join(RAW, "*.csv"))
                       if not p.endswith("_panne.csv")):
        df = pd.read_csv(path)
        if df.empty:
            continue
        moteur, palier = df["moteur"].iloc[0], df["palier"].iloc[0]
        g_all = per_window(df)
        g = complete_windows(g_all)
        if g.empty:
            continue
        lat_first = g_all["lat_med"].iloc[0] / 1000
        keep = df[df["window_start"].isin(g["window_start"])]
        dup = int(keep.duplicated(["window_start", "store_id"]).sum())
        growth = (g["lat_med"].iloc[-1] - g["lat_med"].iloc[0]) / 1000 \
            if len(g) > 1 else np.nan
        rate = df["rate"].iloc[0]
        prod = producer_rate(moteur, palier)
        debit_traite = g["tx"].median() / WINDOW_S
        rows.append({
            "moteur": moteur, "palier": palier, "charge_cible_evs": rate,
            "debit_producer_evs": prod,
            "debit_traite_evs": round(debit_traite),
            "fenetres": len(g), "resultats": len(keep),
            "lat_p50_s": round(keep["latency_ms"].median() / 1000, 1),
            "lat_p95_s": round(keep["latency_ms"].quantile(0.95) / 1000, 1),
            "lat_p50_nette_s": round(keep["latency_ms"].median() / 1000 - WATERMARK_S, 1),
            "lat_1ere_fenetre_s": round(lat_first, 1),
            "derive_latence_s": round(growth, 1) if growth == growth else np.nan,
            "stable": ("oui" if growth == growth and abs(growth) < 5 else
                       ("non" if growth == growth else "n/a")),
            "perte_pct": (round((1 - debit_traite / prod) * 100, 1)
                          if prod == prod and prod > 0 else np.nan),
            "doublons": dup,
        })
        series[(moteur, palier)] = g
    return pd.DataFrame(rows), series


def plot_paliers(summary, series):
    if summary.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 4.2))
    for moteur, d in summary.groupby("moteur"):
        d = d.sort_values("charge_cible_evs")
        c = COLORS.get(moteur)
        ax.plot(d["charge_cible_evs"], d["lat_p50_s"], "o-", color=c,
                label=f"{moteur} p50")
        ax.plot(d["charge_cible_evs"], d["lat_p95_s"], "s--", color=c,
                alpha=.6, label=f"{moteur} p95")
    ax.set_xlabel("charge injectée (événements/s)")
    ax.set_ylabel("latence de bout en bout (s)")
    ax.set_title("Latence selon la charge")
    ax.grid(alpha=.3); ax.legend()
    fig.tight_layout(); fig.savefig(os.path.join(RES, "fig_latence_vs_charge.png"),
                                    dpi=150); plt.close(fig)

    palies = sorted({p for _, p in series})
    fig, axes = plt.subplots(1, len(palies), figsize=(4.2 * len(palies), 3.8),
                             sharey=True, squeeze=False)
    for ax, palier in zip(axes[0], palies):
        for (moteur, p), g in series.items():
            if p == palier:
                ax.plot(range(1, len(g) + 1), g["lat_med"] / 1000, "o-",
                        color=COLORS.get(moteur), label=moteur)
        ax.set_title(palier); ax.set_xlabel("fenêtre n°"); ax.grid(alpha=.3)
    axes[0][0].set_ylabel("latence médiane par fenêtre (s)")
    axes[0][-1].legend()
    fig.tight_layout(); fig.savefig(os.path.join(RES, "fig_latence_temps.png"),
                                    dpi=150); plt.close(fig)


def analyse_pannes():
    out = []
    for path in sorted(glob.glob(os.path.join(RAW, "*_panne.csv"))):
        df = pd.read_csv(path)
        if df.empty:
            continue
        moteur = df["moteur"].iloc[0]
        ev = {}
        evp = os.path.join(LOGS, f"panne_{moteur}_events.txt")
        if os.path.exists(evp):
            for line in open(evp, encoding="utf-8"):
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    try:
                        ev[k] = float(v)
                    except ValueError:
                        pass
        g = per_window(df)
        g["lat_s"] = g["lat_med"] / 1000
        kill, start = ev.get("kill"), ev.get("start")
        if kill is None:
            continue
        ref = g["tx"].median()
        avant = g[g["recv_first"] < kill]
        apres = g[g["recv_first"] >= kill]
        base = avant["lat_s"].median() if len(avant) else np.nan
        pic = apres["lat_s"].max() if len(apres) else np.nan
        # retour à la normale : 1re fenêtre reçue après la panne dont la
        # latence est revenue à <= 1,5 x la latence de référence
        retour = apres[apres["lat_s"] <= 1.5 * base] if base == base else apres.iloc[0:0]
        t_retour = (retour["recv_first"].iloc[0] - kill) if len(retour) else np.nan
        incompletes = int((apres["tx"] < 0.8 * ref).sum())  # après l'arrêt seulement
        dup = int(df.duplicated(["window_start", "store_id"]).sum())
        starts = np.sort(g["window_start"].values)
        trous = int(((np.diff(starts) > WINDOW_S * 1.5)).sum()) if len(starts) > 1 else 0
        out.append({
            "moteur": moteur,
            "panne_duree_s": round(start - kill) if start else np.nan,
            "lat_reference_s": round(base, 1), "lat_pic_s": round(pic, 1),
            "retour_normale_apres_kill_s": round(t_retour) if t_retour == t_retour else np.nan,
            "fenetres_manquantes": trous, "fenetres_incompletes": incompletes,
            "doublons": dup, "reprise_manuelle": "oui" if "resubmit" in ev else "non",
        })
        fig, ax = plt.subplots(figsize=(7, 3.8))
        ax.plot(g["recv_first"] - kill, g["lat_s"], "o-", color=COLORS.get(moteur))
        ax.axvspan(0, (start - kill) if start else 0, color="red", alpha=.15,
                   label="conteneur arrêté")
        ax.axvline(0, color="red", ls="--")
        ax.set_xlabel("temps depuis l'arrêt (s)")
        ax.set_ylabel("latence médiane par fenêtre (s)")
        ax.set_title(f"{moteur} : latence autour de la panne")
        ax.grid(alpha=.3); ax.legend()
        fig.tight_layout(); fig.savefig(os.path.join(RES, f"panne_{moteur}.png"),
                                        dpi=150); plt.close(fig)
    return pd.DataFrame(out)


def md_table(df):
    if df.empty:
        return "(aucune donnée)\n"
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, r in df.iterrows():
        lines.append("| " + " | ".join("" if (isinstance(v, float) and v != v)
                                       else str(v) for v in r) + " |")
    return "\n".join(lines) + "\n"


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if not os.path.isdir(RAW):
        raise SystemExit(f"Dossier introuvable : {RAW}")
    summary, series = analyse_paliers()
    pannes = analyse_pannes()
    plot_paliers(summary, series)
    if not summary.empty:
        summary.sort_values(["palier", "moteur"]).to_csv(
            os.path.join(RES, "summary.csv"), index=False)
    with open(os.path.join(RES, "summary.md"), "w", encoding="utf-8") as f:
        f.write("## Résultats par palier de charge\n\n")
        f.write(md_table(summary.sort_values(["palier", "moteur"]) if not summary.empty else summary))
        f.write("\n## Panne\n\n" + md_table(pannes))
    print(open(os.path.join(RES, "summary.md"), encoding="utf-8").read())
    print("Graphiques et tableaux écrits dans", RES)


if __name__ == "__main__":
    main()
