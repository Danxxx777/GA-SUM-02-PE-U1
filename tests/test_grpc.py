"""Comprobaciones técnicas de Codex; no son evidencias personales ni campaña oficial."""

import csv
import io
import json
import socket
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from threading import Barrier, Event
from time import perf_counter

import grpc

from experiments.ejecutar import iniciar_nodos, medir, verificar_intercambios
from src.comun.registro import RegistroEventos
from src.grpc.cliente import ClienteGRPC
from src.grpc.generado import mensajes_pb2, mensajes_pb2_grpc
from src.grpc.servidor import EstadoNodo, ServidorGRPC

RAIZ = Path(__file__).resolve().parents[1]


class PruebasGRPC(unittest.TestCase):
    def temporal(self):
        temporal = tempfile.TemporaryDirectory(prefix="codex-grpc-")
        ruta = Path(temporal.name).resolve()
        if ruta.parent != Path(tempfile.gettempdir()).resolve():
            raise RuntimeError("Directorio temporal fuera del destino esperado")
        self.addCleanup(temporal.cleanup)
        return ruta

    def servidor(self, nombre="nodo-B", registro=None):
        servidor = ServidorGRPC(EstadoNodo(nombre, registro), 0)
        servidor.iniciar()
        self.addCleanup(servidor.cerrar)
        return servidor

    def cliente(self, nombre="nodo-A", registro=None, timeout=2):
        cliente = ClienteGRPC(EstadoNodo(nombre, registro), timeout)
        self.addCleanup(cliente.cerrar)
        return cliente

    def servicio_especial(self, operacion):
        class Servicio(mensajes_pb2_grpc.MensajeriaServicer):
            def EnviarMensaje(self, request, context):
                return operacion(request, context)

        hilos = ThreadPoolExecutor(max_workers=2)
        servidor = grpc.server(hilos)
        mensajes_pb2_grpc.add_MensajeriaServicer_to_server(Servicio(), servidor)
        puerto = servidor.add_insecure_port("127.0.0.1:0")
        servidor.start()
        self.addCleanup(hilos.shutdown, wait=True)
        self.addCleanup(lambda: servidor.stop(0).wait(timeout=2))
        return puerto

    def test_ida_vuelta_unicode_lamport_y_canal_persistente(self):
        servidor = self.servidor()
        cliente = self.cliente()
        primero = cliente.enviar("nodo-B", servidor.puerto, "á🙂\n" * 2048, "m1")
        canal = cliente._canales[("nodo-B", servidor.puerto)][0]
        segundo = cliente.enviar("nodo-B", servidor.puerto, "otro", "m2")
        self.assertTrue(primero["ok"], primero)
        self.assertTrue(segundo["ok"], segundo)
        self.assertEqual(primero["acuse"]["timestamp"], 3)
        self.assertEqual(segundo["acuse"]["timestamp"], 7)
        self.assertGreaterEqual(primero["latencia_ms"], 0)
        self.assertIs(canal, cliente._canales[("nodo-B", servidor.puerto)][0])

    def test_servidor_rechaza_campos_invalidos_y_sigue_disponible(self):
        servidor = self.servidor()
        with grpc.insecure_channel(f"127.0.0.1:{servidor.puerto}") as canal:
            stub = mensajes_pb2_grpc.MensajeriaStub(canal)
            for campos in ({"sender": ""}, {"message_id": " "}, {"timestamp": -1}):
                solicitud = dict(sender="nodo-A", timestamp=1, message="x", message_id="m")
                solicitud.update(campos)
                with self.subTest(campos=campos), self.assertRaises(grpc.RpcError) as error:
                    stub.EnviarMensaje(mensajes_pb2.Mensaje(**solicitud), timeout=2)
                self.assertEqual(error.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        resultado = self.cliente().enviar("nodo-B", servidor.puerto, "bien", "valido")
        self.assertTrue(resultado["ok"], resultado)

    def test_acuses_incorrectos_descartan_canal(self):
        for cambio in ({"ack_for": "otro"}, {"sender": "nodo-C"},
                       {"message_id": "otro-ack"}, {"timestamp": 1}, {"ok": False}):
            def responder(request, context):
                campos = dict(sender="nodo-B", timestamp=3, message="recibido",
                              message_id=request.message_id + "-ack",
                              ack_for=request.message_id, ok=True)
                campos.update(cambio)
                return mensajes_pb2.Acuse(**campos)

            puerto = self.servicio_especial(responder)
            cliente = self.cliente()
            with self.subTest(cambio=cambio):
                resultado = cliente.enviar("nodo-B", puerto, "hola", "m1")
                self.assertFalse(resultado["ok"])
                self.assertIn("acuse", resultado["error"])
                self.assertFalse(cliente._canales)

    def test_deadline_rpc_y_destino_no_disponible(self):
        recibida, liberar = Event(), Event()

        def demorar(request, context):
            recibida.set()
            liberar.wait(timeout=2)
            return mensajes_pb2.Acuse()

        puerto = self.servicio_especial(demorar)
        cliente = self.cliente(timeout=0.8)
        inicio = perf_counter()
        try:
            resultado = cliente.enviar("nodo-B", puerto, "hola", "lento")
            self.assertTrue(recibida.is_set())
            self.assertFalse(resultado["ok"])
            self.assertIn("DEADLINE_EXCEEDED", resultado["error"])
            self.assertFalse(cliente._canales)
            self.assertLess(perf_counter() - inicio, 2)
        finally:
            liberar.set()
        with socket.socket() as reserva:
            reserva.bind(("127.0.0.1", 0))
            resultado = self.cliente(timeout=0.1).enviar(
                "nodo-B", reserva.getsockname()[1], "x", "ausente"
            )
        self.assertFalse(resultado["ok"])
        self.assertIn("espera", resultado["error"])

    def test_validacion_local_y_timeout(self):
        for timeout in (0, -1, 11, float("nan"), float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                ClienteGRPC(EstadoNodo("nodo-A"), timeout)
        cliente = self.cliente()
        for cambio in ({"puerto": True}, {"destino": ""},
                       {"mensaje": None}, {"identificador": " "}):
            campos = dict(destino="nodo-B", puerto=5001, mensaje="x", identificador="m")
            campos.update(cambio)
            with self.subTest(cambio=cambio), self.assertRaises(ValueError):
                cliente.enviar(**campos)
        self.assertFalse(cliente._canales)

    def test_empate_concurrente_y_registros_reales(self):
        carpeta = self.temporal()
        nombres = ("nodo-A", "nodo-B", "nodo-C")
        registros = {n: RegistroEventos(carpeta / f"{n}.csv", "empate", "grpc", n)
                     for n in nombres}
        servidor = self.servidor("nodo-C", registros["nodo-C"])
        clientes = [self.cliente(n, registros[n]) for n in nombres[:2]]
        barrera = Barrier(2)

        def enviar(indice):
            barrera.wait(timeout=3)
            return clientes[indice].enviar("nodo-C", servidor.puerto, "independiente", f"E-{indice}")

        with ThreadPoolExecutor(max_workers=2) as hilos:
            resultados = list(hilos.map(enviar, (0, 1)))
        self.assertTrue(all(r["ok"] for r in resultados), resultados)
        por_id = defaultdict(dict)
        for ruta in carpeta.glob("*.csv"):
            with ruta.open(encoding="utf-8", newline="") as archivo:
                filas = list(csv.DictReader(archivo))
            relojes = [int(f["reloj"]) for f in filas]
            self.assertTrue(all(a < b for a, b in zip(relojes, relojes[1:])))
            for fila in filas:
                por_id[fila["message_id"]][fila["evento"]] = int(fila["reloj"])
        self.assertEqual(len(por_id), 4)
        for par in por_id.values():
            self.assertGreater(par["recepcion"], par["envio"])
        self.assertEqual(por_id["E-0"]["envio"], por_id["E-1"]["envio"])

    def test_tres_procesos_con_ejecutor_de_diego(self):
        carpeta = self.temporal()
        verificar_intercambios("grpc", carpeta, 20)
        with ExitStack() as pila:
            nodos = iniciar_nodos(pila, "grpc", "comprobacion-codex")
            for modo in ("secuencial", "concurrente"):
                for tamano in (100, 10240):
                    salida = io.StringIO()
                    resultado = medir(nodos, "grpc", f"{modo}-{tamano}", tamano,
                                      modo, False, csv.writer(salida))
                    self.assertEqual(resultado[-2], 100)
                    self.assertEqual(len(salida.getvalue().splitlines()), 100)
        filas = []
        for ruta in (carpeta / "eventos").glob("*.csv"):
            with ruta.open(encoding="utf-8", newline="") as archivo:
                filas.extend(csv.DictReader(archivo))
        self.assertEqual(len(filas), 80)
        por_id = defaultdict(dict)
        for fila in filas:
            por_id[fila["message_id"]][fila["evento"]] = int(fila["reloj"])
        self.assertEqual(len(por_id), 40)
        for par in por_id.values():
            self.assertGreater(par["recepcion"], par["envio"])

    def test_cli_errores_json_y_cierre_eof(self):
        with socket.socket() as reserva:
            reserva.bind(("127.0.0.1", 0))
            puerto = reserva.getsockname()[1]
        orden = json.dumps(dict(destino="nodo-A", puerto=puerto,
                                message="desde stdin", message_id="cli"))
        resultado = subprocess.run(
            [sys.executable, "-B", "-m", "src.grpc.nodo", "--nombre", "nodo-A",
             "--puerto", str(puerto), "--corrida", "comprobacion-codex"],
            input="no es json\n[]\n" + orden + "\n", text=True, encoding="utf-8",
            capture_output=True, timeout=10, cwd=RAIZ,
        )
        self.assertEqual(resultado.returncode, 0, resultado.stderr)
        respuestas = [json.loads(linea) for linea in resultado.stdout.splitlines()]
        self.assertEqual([r["ok"] for r in respuestas], [True, False, False, True])
        self.assertEqual(respuestas[-1]["message_id"], "cli")
        # gRPC puede emitir diagnósticos de cierre por stderr; stdout sigue siendo JSON.
        self.assertNotIn("Traceback", resultado.stderr)


if __name__ == "__main__":
    unittest.main()
