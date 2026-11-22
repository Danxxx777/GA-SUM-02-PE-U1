"""Análisis de latencia y rendimiento de la campaña oficial de PE-U1 (sockets TCP vs gRPC).

Uso (desde la raíz del repositorio):
    python analysis/analizar.py data/campana-oficial-20261121-223100

Lee solo los CSV reales de la campaña (latencias.csv y corridas.csv de cada mecanismo).
- Excluye las filas de calentamiento (calentamiento = 1) y cuenta los fallos (ok = 0).
- Por mecanismo, tamaño de mensaje y modo calcula: n, media, mediana, desviación estándar,
  p95, intervalo de confianza del 95 % de la media (bootstrap con semilla fija) y rendimiento.
- Rendimiento = solicitudes completadas / duración de pared, sobre las corridas registradas.
Escribe tablas en resultados/tablas y gráficos en resultados/graficos.
"""
import argparse
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

MECANISMOS = ["sockets", "grpc"]
ETIQUETA = {"sockets": "Sockets TCP", "grpc": "gRPC"}
COLOR = {"sockets": "#1f77b4", "grpc": "#d95f02"}
SEMILLA = 20261121
REMUESTREOS = 5000


def cargar(carpeta):
    lat, cor = [], []
    for m in MECANISMOS:
        l = carpeta / m / "latencias.csv"
        c = carpeta / m / "corridas.csv"
        if not l.exists() or not c.exists():
            sys.exit(f"Falta {l} o {c}")
        lat.append(pd.read_csv(l))
        cor.append(pd.read_csv(c))
    return pd.concat(lat, ignore_index=True), pd.concat(cor, ignore_index=True)


def ic_bootstrap(valores, rng):
    muestras = rng.choice(valores, size=(REMUESTREOS, len(valores)), replace=True).mean(axis=1)
    return float(np.percentile(muestras, 2.5)), float(np.percentile(muestras, 97.5))


def resumir(lat, cor):
    rng = np.random.default_rng(SEMILLA)
    filas = []
    claves = ["mecanismo", "tamano_bytes", "modo"]
    for (mec, tam, modo), g in lat[lat.calentamiento == 0].groupby(claves):
        total = len(g)
        buenos = g[g.ok == 1].latencia_ms.to_numpy()
        c = cor[(cor.mecanismo == mec) & (cor.tamano_bytes == tam) & (cor.modo == modo)
                & (cor.calentamiento == 0)]
        lo, hi = ic_bootstrap(buenos, rng)
        filas.append({
            "mecanismo": mec, "tamano_bytes": tam, "modo": modo,
            "n": total, "fallos": int(total - len(buenos)), "corridas": len(c),
            "media_ms": buenos.mean(), "mediana_ms": float(np.median(buenos)),
            "desv_ms": buenos.std(ddof=1), "p95_ms": float(np.percentile(buenos, 95)),
            "ic95_inf_ms": lo, "ic95_sup_ms": hi,
            "rendimiento_msg_s": c.solicitudes_completadas.sum() / c.duracion_s.sum(),
        })
    return pd.DataFrame(filas).sort_values(["modo", "tamano_bytes", "mecanismo"])


def tabla_tex(res, destino):
    lineas = [
        r"\begin{table}[htbp]", r"\centering", r"\footnotesize", r"\setlength{\tabcolsep}{3pt}",
        r"\caption{Latencia y rendimiento por mecanismo, tamaño y modo (calentamiento excluido)}",
        r"\label{tab:resumen-latencia}",
        r"\begin{tabular}{@{}llrrrrrrr@{}}", r"\toprule",
        r"\textbf{Modo} & \textbf{Mecanismo} & \textbf{Bytes} & \textbf{n} & \textbf{Media} & "
        r"\textbf{Med.} & \textbf{Desv.} & \textbf{p95} & \textbf{Msg/s} \\",
        r"\midrule",
    ]
    for _, r in res.iterrows():
        lineas.append(
            f"{r.modo} & {ETIQUETA[r.mecanismo]} & {r.tamano_bytes} & {r.n} & "
            f"{r.media_ms:.3f} & {r.mediana_ms:.3f} & {r.desv_ms:.3f} & {r.p95_ms:.3f} & "
            f"{r.rendimiento_msg_s:.0f} \\\\")
    lineas += [r"\bottomrule", r"\end{tabular}",
               r"\par\smallskip\footnotesize Latencias en milisegundos. n = envíos válidos de cinco "
               r"corridas registradas por combinación.", r"\end{table}"]
    destino.write_text("\n".join(lineas) + "\n", encoding="utf-8")


def grafico_caja(lat, destino):
    d = lat[(lat.calentamiento == 0) & (lat.ok == 1)]
    modos = ["secuencial", "concurrente"]
    tams = [100, 10240]
    fig, ejes = plt.subplots(1, 4, figsize=(12, 3.8), sharey=False)
    for i, (modo, tam) in enumerate([(m, t) for m in modos for t in tams]):
        ax = ejes[i]
        datos = [d[(d.mecanismo == m) & (d.modo == modo) & (d.tamano_bytes == tam)].latencia_ms
                 for m in MECANISMOS]
        bp = ax.boxplot(datos, tick_labels=[ETIQUETA[m] for m in MECANISMOS], patch_artist=True,
                        showfliers=True, flierprops={"markersize": 2})
        for caja, m in zip(bp["boxes"], MECANISMOS):
            caja.set_facecolor(COLOR[m]); caja.set_alpha(0.6)
        ax.set_title(f"{modo}, {tam} B", fontsize=10)
        ax.set_ylabel("Latencia (ms)" if i == 0 else "")
        ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(destino, dpi=200)
    plt.close(fig)


def grafico_media(res, destino):
    fig, ejes = plt.subplots(1, 2, figsize=(10, 3.8))
    for ax, modo in zip(ejes, ["secuencial", "concurrente"]):
        r = res[res.modo == modo]
        x = np.arange(2)
        ancho = 0.35
        for j, m in enumerate(MECANISMOS):
            s = r[r.mecanismo == m].sort_values("tamano_bytes")
            err = [s.media_ms - s.ic95_inf_ms, s.ic95_sup_ms - s.media_ms]
            ax.bar(x + (j - 0.5) * ancho, s.media_ms, ancho, yerr=err, capsize=4,
                   label=ETIQUETA[m], color=COLOR[m], alpha=0.8)
        ax.set_xticks(x, ["100 B", "10 240 B"])
        ax.set_title(f"Latencia media, modo {modo}", fontsize=10)
        ax.set_ylabel("ms (IC 95 %)")
        ax.grid(axis="y", alpha=0.3)
        ax.legend()
    fig.tight_layout()
    fig.savefig(destino, dpi=200)
    plt.close(fig)


def grafico_rendimiento(res, destino):
    fig, ejes = plt.subplots(1, 2, figsize=(10, 3.8))
    for ax, modo in zip(ejes, ["secuencial", "concurrente"]):
        r = res[res.modo == modo]
        x = np.arange(2)
        ancho = 0.35
        for j, m in enumerate(MECANISMOS):
            s = r[r.mecanismo == m].sort_values("tamano_bytes")
            ax.bar(x + (j - 0.5) * ancho, s.rendimiento_msg_s, ancho,
                   label=ETIQUETA[m], color=COLOR[m], alpha=0.8)
        ax.set_xticks(x, ["100 B", "10 240 B"])
        ax.set_title(f"Rendimiento, modo {modo}", fontsize=10)
        ax.set_ylabel("mensajes por segundo")
        ax.grid(axis="y", alpha=0.3)
        ax.legend()
    fig.tight_layout()
    fig.savefig(destino, dpi=200)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("campana", type=Path, help="carpeta de la campaña oficial")
    ap.add_argument("--salida", type=Path, default=Path("resultados"))
    a = ap.parse_args()
    lat, cor = cargar(a.campana)
    res = resumir(lat, cor)
    tablas, graficos = a.salida / "tablas", a.salida / "graficos"
    tablas.mkdir(parents=True, exist_ok=True)
    graficos.mkdir(parents=True, exist_ok=True)
    res.to_csv(tablas / "resumen_latencia.csv", index=False, float_format="%.4f")
    tabla_tex(res, tablas / "resumen_latencia.tex")
    grafico_caja(lat, graficos / "latencia-cajas.png")
    grafico_media(res, graficos / "latencia-media-ic95.png")
    grafico_rendimiento(res, graficos / "rendimiento.png")
    pd.set_option("display.width", 200)
    print(res.round(3).to_string(index=False))
    print(f"\nFallos totales (sin calentamiento): {int(res.fallos.sum())}")
    print(f"Salida en {a.salida}/")


if __name__ == "__main__":
    main()
