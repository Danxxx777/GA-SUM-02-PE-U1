# GA-SUM-02-PE-U1
GA-SUM-02 — PE-U1 — Práctica Experimental Unidad 1

## Preparación y campaña experimental

Desde la raíz del clon, en PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -B -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -B -m experiments.ejecutar --modo humo --mecanismo ambos --salida pruebas-humo
```

Los tres puertos 5000 a 5002 deben estar libres. El ejecutor inicia los nodos de
cada mecanismo por separado. Usa una ruta nueva para cada ejecución. Revisa
[el protocolo](experiments/PROTOCOLO.md) y [la generación gRPC](src/grpc/README.md).

Después de registrar la hipótesis consensuada y aprobar ambos transportes,
ejecuta la campaña en la máquina de medición. Sustituye 16 GB por la RAM real.

```powershell
.\.venv\Scripts\python.exe -B -m experiments.ejecutar --modo campana --mecanismo ambos --salida data/campana-oficial --memoria "16 GB"
```

El manifiesto SHA256SUMS usa rutas relativas a la carpeta de salida. La práctica
manual de Byron se conserva en su carpeta original. Pendientes de integración:
comando de análisis y enlace al video.

## Compilar el informe

Desde la raíz, ejecuta:

```powershell
Set-Location docs
pdflatex -halt-on-error informe.tex
biber informe
pdflatex -halt-on-error informe.tex
pdflatex -halt-on-error informe.tex
```

Se necesita pdfLaTeX, Biber, biblatex con el estilo IEEE, babel en español,
graphicx, hyperref, tabularx, booktabs y fvextra. El archivo principal es informe.tex.
