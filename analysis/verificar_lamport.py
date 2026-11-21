"""Verificación del reloj lógico de Lamport a partir de los CSV de eventos reales.

Uso:
    python analysis/verificar_lamport.py data/sockets/eventos.csv data/grpc/eventos.csv
    python analysis/verificar_lamport.py data            # busca eventos*.csv dentro de la carpeta

Cada CSV de eventos debe tener estas columnas (los nombres se pueden cambiar en COLUMNAS):
    id_mensaje, nodo, tipo_evento (envio | recepcion), reloj, hora_real

El script comprueba, SOLO con los datos del archivo (no genera valores):
  1. Cada mensaje tiene un envío y una recepción (reporta pares faltantes o duplicados).
  2. Para todo mensaje, reloj_recepcion > reloj_envio.
  3. En cada nodo el reloj crece de forma estricta de un evento al siguiente.
  4. Hay dos eventos de nodos distintos con el mismo valor de reloj que son concurrentes
     (se demuestra con relojes vectoriales calculados desde los mismos eventos).
Termina con código 0 si las comprobaciones 1 a 3 pasan, y con 1 si alguna falla.
"""
import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

COLUMNAS = {"id": "id_mensaje", "nodo": "nodo", "tipo": "tipo_evento",
            "reloj": "reloj", "hora": "hora_real"}
ENVIO = {"envio", "envío", "send", "sent"}
RECEPCION = {"recepcion", "recepción", "recv", "receive", "received"}


def a_numero(texto):
    """Convierte la hora real a número para ordenar (admite segundos o fecha ISO)."""
    try:
        return float(texto)
    except ValueError:
        return datetime.fromisoformat(texto).timestamp()


def leer_eventos(ruta):
    """Lee el CSV y devuelve la lista de eventos en el orden del archivo."""
    eventos = []
    with open(ruta, newline="", encoding="utf-8") as f:
        lector = csv.DictReader(f)
        faltan = [c for c in COLUMNAS.values() if c not in (lector.fieldnames or [])]
        if faltan:
            raise SystemExit(f"{ruta}: faltan las columnas {faltan}")
        for n, fila in enumerate(lector):
            tipo = fila[COLUMNAS["tipo"]].strip().lower()
            if tipo in ENVIO:
                tipo = "envio"
            elif tipo in RECEPCION:
                tipo = "recepcion"
            else:
                raise SystemExit(f"{ruta}, fila {n + 2}: tipo de evento desconocido '{tipo}'")
            eventos.append({
                "n": n, "id": fila[COLUMNAS["id"]].strip(), "nodo": fila[COLUMNAS["nodo"]].strip(),
                "tipo": tipo, "reloj": int(fila[COLUMNAS["reloj"]]),
                "hora": a_numero(fila[COLUMNAS["hora"]]),
            })
    return eventos


def emparejar(eventos):
    """Agrupa por id_mensaje y devuelve (pares, problemas)."""
    por_id, problemas, pares = {}, [], []
    for e in eventos:
        por_id.setdefault(e["id"], {"envio": [], "recepcion": []})[e["tipo"]].append(e)
    for id_, g in por_id.items():
        if len(g["envio"]) != 1 or len(g["recepcion"]) != 1:
            problemas.append(f"mensaje {id_}: {len(g['envio'])} envíos y {len(g['recepcion'])} recepciones (se esperaba 1 y 1)")
        else:
            pares.append((g["envio"][0], g["recepcion"][0]))
    return pares, problemas


def verificar_regla(pares):
    """Para todo mensaje, el reloj al recibir debe ser mayor que el reloj al enviar."""
    return [f"mensaje {s['id']}: reloj de recepción {r['reloj']} no es mayor que el de envío {s['reloj']}"
            for s, r in pares if not r["reloj"] > s["reloj"]]


def verificar_monotonia(eventos):
    """En cada nodo, ordenado por hora real, el reloj debe aumentar estrictamente."""
    errores = []
    for nodo in sorted({e["nodo"] for e in eventos}):
        propios = sorted((e for e in eventos if e["nodo"] == nodo), key=lambda e: (e["hora"], e["n"]))
        for a, b in zip(propios, propios[1:]):
            if not b["reloj"] > a["reloj"]:
                errores.append(f"{nodo}: el reloj pasó de {a['reloj']} a {b['reloj']} (mensajes {a['id']} y {b['id']})")
    return errores


def relojes_vectoriales(eventos, pares):
    """Calcula un reloj vectorial por evento usando solo el orden por nodo y los pares envío-recepción.

    Devuelve (vectores, ciclo) donde vectores es {n_evento: {nodo: contador}}.
    """
    nodos = sorted({e["nodo"] for e in eventos})
    envio_de = {r["n"]: s["n"] for s, r in pares}          # recepción -> su envío
    cola = {nodo: sorted((e for e in eventos if e["nodo"] == nodo), key=lambda e: (e["hora"], e["n"]))
            for nodo in nodos}
    actual = {nodo: {x: 0 for x in nodos} for nodo in nodos}
    vectores, posicion = {}, {nodo: 0 for nodo in nodos}
    pendientes = len(eventos)
    while pendientes:
        avanzo = False
        for nodo in nodos:
            while posicion[nodo] < len(cola[nodo]):
                e = cola[nodo][posicion[nodo]]
                if e["n"] in envio_de and envio_de[e["n"]] not in vectores:
                    break                                   # espera a que se procese su envío
                v = dict(actual[nodo])
                if e["n"] in envio_de:
                    origen = vectores[envio_de[e["n"]]]
                    v = {x: max(v[x], origen[x]) for x in nodos}
                v[nodo] += 1
                actual[nodo] = v
                vectores[e["n"]] = v
                posicion[nodo] += 1
                pendientes -= 1
                avanzo = True
        if not avanzo:
            return vectores, True                           # dependencias circulares: datos inconsistentes
    return vectores, False


def ocurre_antes(a, b):
    return all(a[x] <= b[x] for x in a) and any(a[x] < b[x] for x in a)


def buscar_concurrentes(eventos, vectores, maximo=3):
    """Eventos de nodos distintos con el mismo reloj de Lamport y sin relación de orden."""
    ejemplos = []
    for i, a in enumerate(eventos):
        for b in eventos[i + 1:]:
            if a["nodo"] != b["nodo"] and a["reloj"] == b["reloj"] and a["n"] in vectores and b["n"] in vectores:
                va, vb = vectores[a["n"]], vectores[b["n"]]
                if not ocurre_antes(va, vb) and not ocurre_antes(vb, va):
                    ejemplos.append((a, b))
                    if len(ejemplos) >= maximo:
                        return ejemplos
    return ejemplos


def revisar_archivo(ruta, salida):
    eventos = leer_eventos(ruta)
    pares, problemas_pares = emparejar(eventos)
    problemas_regla = verificar_regla(pares)
    problemas_mono = verificar_monotonia(eventos)
    vectores, ciclo = relojes_vectoriales(eventos, pares)
    ejemplos = [] if ciclo else buscar_concurrentes(eventos, vectores)

    def p(texto=""):
        print(texto)
        salida.append(texto)

    p(f"== {ruta} ==")
    p(f"Eventos: {len(eventos)}   Mensajes emparejados: {len(pares)}")
    p(f"1. Pares envío/recepción completos: {'OK' if not problemas_pares else 'FALLA'}")
    for t in problemas_pares: p("   - " + t)
    p(f"2. reloj_recepcion > reloj_envio en todos los mensajes: {'OK' if not problemas_regla else 'FALLA'}")
    for t in problemas_regla: p("   - " + t)
    p(f"3. Reloj estrictamente creciente en cada nodo: {'OK' if not problemas_mono else 'FALLA'}")
    for t in problemas_mono: p("   - " + t)
    if ciclo:
        p("4. Concurrencia: no se pudo calcular (los envíos y recepciones forman dependencias circulares).")
    elif ejemplos:
        p(f"4. Eventos concurrentes con el mismo reloj ({len(ejemplos)} ejemplos):")
        for a, b in ejemplos:
            p(f"   - {a['nodo']} (mensaje {a['id']}, {a['tipo']}) y {b['nodo']} (mensaje {b['id']}, {b['tipo']}) "
              f"tienen reloj {a['reloj']} y ninguno ocurre antes del otro.")
    else:
        p("4. No hay un caso demostrable de eventos concurrentes con el mismo reloj en esta corrida. "
          "Repetir una prueba con dos envíos iniciales independientes.")
    p()
    return not (problemas_pares or problemas_regla or problemas_mono)


def main():
    parser = argparse.ArgumentParser(description="Verifica el reloj de Lamport con los CSV de eventos.")
    parser.add_argument("rutas", nargs="+", help="CSV de eventos o carpetas que los contienen (eventos*.csv)")
    parser.add_argument("--salida", help="archivo de texto donde guardar el informe")
    args = parser.parse_args()

    archivos = []
    for r in args.rutas:
        ruta = Path(r)
        archivos += sorted(ruta.rglob("eventos*.csv")) if ruta.is_dir() else [ruta]
    if not archivos:
        sys.exit("No se encontró ningún CSV de eventos.")

    salida, todo_ok = [], True
    for a in archivos:
        todo_ok &= revisar_archivo(a, salida)
    if args.salida:
        Path(args.salida).parent.mkdir(parents=True, exist_ok=True)
        Path(args.salida).write_text("\n".join(salida), encoding="utf-8")
    sys.exit(0 if todo_ok else 1)


if __name__ == "__main__":
    main()
