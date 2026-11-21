import argparse
import csv
import hashlib
import json
import platform
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Queue
from threading import Barrier, Thread
from time import perf_counter

NODOS = {"nodo-A": 5000, "nodo-B": 5001, "nodo-C": 5002}
PARES = list(zip(NODOS, ("nodo-B", "nodo-C", "nodo-A")))
LATENCIAS = ("corrida", "mecanismo", "tamano_bytes", "modo", "calentamiento",
             "message_id", "emisor", "destino", "latencia_ms", "ok", "error")
RESUMEN = LATENCIAS[:5] + (
    "solicitudes_programadas", "solicitudes_completadas", "duracion_s"
)


class Nodo:
    def __init__(self, pila, mecanismo, nombre, corrida, eventos):
        comando = [sys.executable, "-u", "-m", f"src.{mecanismo}.nodo",
                   "--nombre", nombre, "--puerto", str(NODOS[nombre]), "--corrida", corrida]
        if eventos is not None:
            comando.extend(("--eventos", str(eventos / f"{nombre}.csv")))
        self.proceso = subprocess.Popen(
            comando, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            encoding="utf-8", bufsize=1
        )
        self.nombre = nombre
        self.respuestas = Queue()
        pila.callback(self.cerrar)
        Thread(target=self.leer, daemon=True).start()

    def leer(self):
        for linea in self.proceso.stdout:
            self.respuestas.put(linea)
        self.respuestas.put(None)

    def esperar(self, segundos):
        try:
            linea = self.respuestas.get(timeout=segundos)
        except Empty as exc:
            raise RuntimeError(f"{self.nombre} agotó el tiempo de espera") from exc
        if linea is None:
            raise RuntimeError(f"{self.nombre} cerró su salida")
        respuesta = json.loads(linea)
        if not isinstance(respuesta, dict):
            raise RuntimeError(f"{self.nombre} devolvió una respuesta inválida")
        return respuesta

    def enviar(self, destino, mensaje, identificador):
        orden = dict(destino=destino, puerto=NODOS[destino], message=mensaje,
                     message_id=identificador)
        self.proceso.stdin.write(json.dumps(orden) + "\n")
        self.proceso.stdin.flush()
        respuesta = self.esperar(15)
        if respuesta.get("message_id") != identificador or type(respuesta.get("ok")) is not bool:
            raise RuntimeError(f"{self.nombre} devolvió una respuesta fuera del contrato")
        if respuesta["ok"]:
            latencia = respuesta.get("latencia_ms")
            if type(latencia) not in (int, float) or not 0 <= latencia < float("inf"):
                raise RuntimeError(f"{self.nombre} devolvió una latencia inválida")
        return respuesta

    def cerrar(self):
        try:
            self.proceso.stdin.close()
            self.proceso.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            self.proceso.kill()
            self.proceso.wait()
        finally:
            self.proceso.stdout.close()


def iniciar_nodos(pila, mecanismo, corrida, eventos=None):
    nodos = {nombre: Nodo(pila, mecanismo, nombre, corrida, eventos) for nombre in NODOS}
    for nodo in nodos.values():
        if nodo.esperar(15).get("ok") is not True:
            raise RuntimeError(f"{nodo.nombre} no inició")
    return nodos


def verificar_intercambios(mecanismo, carpeta, cantidad):
    with ExitStack() as pila:
        nodos = iniciar_nodos(pila, mecanismo, "lamport", carpeta / "eventos")
        for indice in range(cantidad):
            emisor, destino = PARES[indice % len(PARES)]
            respuesta = nodos[emisor].enviar(destino, "Intercambio Lamport", f"L-{indice:03}")
            if not respuesta["ok"]:
                raise RuntimeError(f"Falló L-{indice:03}: {respuesta.get('error')}")


def medir(nodos, mecanismo, corrida, tamano, modo, calentamiento, escritor):
    contexto = (corrida, mecanismo, tamano, modo, int(calentamiento))
    mensaje = "x" * tamano
    ordenes = [(indice, *PARES[indice % len(PARES)]) for indice in range(100)]

    def enviar(orden):
        indice, emisor, destino = orden
        identificador = f"{corrida}-{indice:03}"
        respuesta = nodos[emisor].enviar(destino, mensaje, identificador)
        return (*contexto, identificador, emisor, destino,
                respuesta.get("latencia_ms", "") if respuesta["ok"] else "",
                int(respuesta["ok"]), respuesta.get("error", ""))

    completadas = 0
    if modo == "secuencial":
        inicio = perf_counter()
        filas = [enviar(orden) for orden in ordenes]
        duracion = perf_counter() - inicio
        escritor.writerows(filas)
        completadas = sum(fila[-2] for fila in filas)
    else:
        barrera = Barrier(len(NODOS) + 1)

        def enviar_desde(emisor):
            lote = [orden for orden in ordenes if orden[1] == emisor]
            barrera.wait(timeout=10)
            return [enviar(orden) for orden in lote]

        with ThreadPoolExecutor(max_workers=len(NODOS)) as hilos:
            trabajos = [hilos.submit(enviar_desde, emisor) for emisor in NODOS]
            inicio = perf_counter()
            barrera.wait(timeout=10)
            lotes = [trabajo.result() for trabajo in trabajos]
            duracion = perf_counter() - inicio
        for lote in lotes:
            escritor.writerows(lote)
            completadas += sum(fila[-2] for fila in lote)
    return (*contexto, len(ordenes), completadas, duracion)


def campana(mecanismo, carpeta):
    verificar_intercambios(mecanismo, carpeta, 20)
    with ExitStack() as pila, ThreadPoolExecutor(max_workers=2) as hilos:
        nodos = iniciar_nodos(pila, mecanismo, "empate", carpeta / "empate")
        barrera = Barrier(2)

        def enviar_independiente(emisor, identificador):
            barrera.wait(timeout=10)
            return nodos[emisor].enviar("nodo-C", "Evento independiente", identificador)

        envios = [hilos.submit(enviar_independiente, emisor, identificador)
                  for emisor, identificador in zip(NODOS, ("E-A", "E-B"))]
        if not all(envio.result()["ok"] for envio in envios):
            raise RuntimeError("Falló la corrida de eventos independientes")
    with ExitStack() as pila:
        nodos = iniciar_nodos(pila, mecanismo, "latencia")
        latencias = csv.writer(pila.enter_context(
            (carpeta / "latencias.csv").open("x", encoding="utf-8", newline="")
        ))
        resumen = csv.writer(pila.enter_context(
            (carpeta / "corridas.csv").open("x", encoding="utf-8", newline="")
        ))
        latencias.writerow(LATENCIAS)
        resumen.writerow(RESUMEN)
        for modo in ("secuencial", "concurrente"):
            for tamano in (100, 10240):
                for repeticion in range(6):
                    corrida = f"{modo}-{tamano}-{repeticion}"
                    resultado = medir(nodos, mecanismo, corrida, tamano, modo,
                                      repeticion == 0, latencias)
                    resumen.writerow(resultado)
                    print(f"{mecanismo} {corrida}: {resultado[-2]}/100 completadas", flush=True)


def guardar_entorno(carpeta, memoria):
    lineas = (
        f"Fecha UTC: {datetime.now(timezone.utc).isoformat()}",
        f"Sistema operativo: {platform.platform()}",
        f"Procesador: {platform.processor() or platform.machine()}",
        f"Memoria RAM: {memoria}",
        f"Python: {platform.python_version()}",
        "Nodos: procesos en localhost",
    )
    (carpeta / "entorno.md").write_text("\n".join(lineas) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mecanismo", choices=("sockets", "grpc", "ambos"), default="ambos")
    parser.add_argument("--modo", choices=("humo", "campana"), default="humo")
    parser.add_argument("--salida", type=Path, required=True)
    parser.add_argument("--memoria", help="Memoria RAM instalada, por ejemplo 16 GB")
    args = parser.parse_args()
    mecanismos = ("sockets", "grpc") if args.mecanismo == "ambos" else (args.mecanismo,)
    faltantes = [f"src/{mecanismo}/nodo.py" for mecanismo in mecanismos
                 if not Path(f"src/{mecanismo}/nodo.py").is_file()]
    if faltantes:
        parser.error("faltan los transportes: " + ", ".join(faltantes))
    if args.modo == "campana" and (not args.memoria or not args.memoria.strip()):
        parser.error("--memoria es obligatorio para la campaña")
    if args.salida.exists():
        parser.error("--salida debe ser una carpeta nueva")
    args.salida.mkdir(parents=True)
    if args.modo == "campana":
        guardar_entorno(args.salida, args.memoria)
    for mecanismo in mecanismos:
        carpeta = args.salida / mecanismo
        carpeta.mkdir()
        if args.modo == "humo":
            verificar_intercambios(mecanismo, carpeta, 1)
        else:
            campana(mecanismo, carpeta)
    sumas = [f"{hashlib.sha256(ruta.read_bytes()).hexdigest()}  "
             f"{ruta.relative_to(args.salida).as_posix()}"
             for ruta in sorted(args.salida.rglob("*.csv"))]
    (args.salida / "SHA256SUMS").write_text("\n".join(sumas) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

