"""Pruebas de desarrollo TCP; sus datos temporales no son la campaña oficial."""

import csv
import hashlib
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from threading import Barrier, Thread
from time import perf_counter
from unittest.mock import Mock, patch

from experiments.ejecutar import iniciar_nodos, medir, verificar_intercambios
from src.comun.registro import RegistroEventos
from src.sockets.cliente import ClienteTCP
from src.sockets.servidor import CanalJSON, EstadoNodo, ServidorTCP, validar_mensaje

RAIZ = Path(__file__).resolve().parents[1]


class PruebasTCP(unittest.TestCase):
    def carpeta_temporal(self):
        temporal = tempfile.TemporaryDirectory(prefix=".prueba-tcp-", dir=RAIZ)
        ruta = Path(temporal.name).resolve()
        # Verificar el destino antes de registrar la limpieza recursiva.
        if ruta.parent != RAIZ:
            raise RuntimeError("Directorio temporal fuera del repositorio")
        self.addCleanup(temporal.cleanup)
        return ruta

    def servidor(self, nombre="nodo-B", registro=None):
        servidor = ServidorTCP(EstadoNodo(nombre, registro), 0)
        servidor.iniciar()
        self.addCleanup(servidor.cerrar)
        return servidor

    def cliente(self, timeout=1):
        cliente = ClienteTCP(EstadoNodo("nodo-A"), timeout)
        self.addCleanup(cliente.cerrar)
        return cliente

    def test_ida_vuelta_lamport_y_conexion_persistente(self):
        servidor = self.servidor()
        cliente = self.cliente()
        primero = cliente.enviar("nodo-B", servidor.puerto, "hola", "m1")
        canal = cliente._canales[("nodo-B", servidor.puerto)]
        segundo = cliente.enviar("nodo-B", servidor.puerto, "otra vez", "m2")
        self.assertTrue(primero["ok"])
        self.assertTrue(segundo["ok"])
        self.assertEqual(primero["acuse"]["timestamp"], 3)
        self.assertEqual(segundo["acuse"]["timestamp"], 7)
        self.assertGreaterEqual(primero["latencia_ms"], 0)
        self.assertIs(canal, cliente._canales[("nodo-B", servidor.puerto)])

    def test_fragmentacion_utf8_y_mensajes_agrupados(self):
        mensaje = dict(sender="nodo-A", timestamp=1, message="á🙂\nhola", message_id="m1")
        datos = json.dumps(mensaje, ensure_ascii=False).encode("utf-8") + b"\n"
        corte = datos.index("🙂".encode("utf-8")) + 1
        conexion = Mock()
        conexion.recv.side_effect = [datos[:corte], datos[corte:] + datos]
        canal = CanalJSON(conexion)
        self.assertEqual(canal.recibir(), mensaje)
        self.assertEqual(canal.recibir(), mensaje)
        self.assertEqual(conexion.recv.call_count, 2)

    def test_dos_solicitudes_juntas_en_conexion_real(self):
        servidor = self.servidor()
        with socket.create_connection(("127.0.0.1", servidor.puerto), timeout=1) as conexion:
            datos = b"".join(json.dumps(dict(sender="nodo-A", timestamp=i,
                                            message="hola", message_id=f"m{i}")).encode() + b"\n"
                             for i in (1, 2))
            conexion.sendall(datos)
            canal = CanalJSON(conexion)
            self.assertEqual(canal.recibir()["ack_for"], "m1")
            self.assertEqual(canal.recibir()["ack_for"], "m2")

    def test_mensaje_largo_unicode(self):
        servidor = self.servidor()
        resultado = self.cliente().enviar("nodo-B", servidor.puerto, "á🙂\n" * 2048, "largo")
        self.assertTrue(resultado["ok"], resultado)

    def test_validacion_no_acepta_booleano_como_reloj(self):
        for reloj in (True, -1, 1.5, "1", None):
            with self.subTest(reloj=reloj), self.assertRaises(ValueError):
                validar_mensaje(dict(sender="nodo-A", timestamp=reloj, message="x", message_id="m"))

    def test_servidor_rechaza_json_invalido_y_sigue_disponible(self):
        servidor = self.servidor()
        for datos in (b"{incorrecto}\n", b"[]\n", b'{"timestamp":NaN}\n'):
            with self.subTest(datos=datos), socket.create_connection(
                    ("127.0.0.1", servidor.puerto), timeout=1) as conexion:
                conexion.sendall(datos)
                respuesta = CanalJSON(conexion).recibir()
                self.assertFalse(respuesta["ok"])
                self.assertIn("error", respuesta)
        self.assertTrue(self.cliente().enviar("nodo-B", servidor.puerto, "bien", "valido")["ok"])

    def test_limite_de_tamano_y_json_truncado(self):
        conexion = Mock()
        conexion.recv.side_effect = [b'{"incompleto":', b""]
        with self.assertRaisesRegex(ValueError, "incompleto"):
            CanalJSON(conexion).recibir()
        with patch("src.sockets.servidor.MAX_MENSAJE", 16):
            conexion.recv.side_effect = [b"x" * 17]
            with self.assertRaisesRegex(ValueError, "supera"):
                CanalJSON(conexion).recibir()

    def test_destino_no_disponible(self):
        # Reservar un puerto sin listen evita depender de un puerto supuestamente libre.
        with socket.socket() as reserva:
            reserva.bind(("127.0.0.1", 0))
            respuesta = self.cliente().enviar("nodo-B", reserva.getsockname()[1], "x", "ausente")
        self.assertFalse(respuesta["ok"])
        self.assertIn("nodo-B", respuesta["error"])

    def test_timeout_y_acuse_incorrecto_descartan_conexion(self):
        for incorrecto in (False, True):
            with self.subTest(acuse_incorrecto=incorrecto), socket.socket() as escucha:
                escucha.bind(("127.0.0.1", 0))
                escucha.listen()
                espera = Barrier(2)

                def destino():
                    with escucha.accept()[0] as conexion:
                        canal = CanalJSON(conexion)
                        canal.recibir()
                        if incorrecto:
                            canal.enviar(dict(sender="nodo-B", timestamp=3, message="acuse",
                                              message_id="otro-ack", ack_for="otro", ok=True))
                        espera.wait(timeout=2)

                hilo = Thread(target=destino)
                hilo.start()
                cliente = self.cliente(timeout=0.1)
                inicio = perf_counter()
                try:
                    respuesta = cliente.enviar("nodo-B", escucha.getsockname()[1], "x", "m1")
                    self.assertFalse(respuesta["ok"])
                    self.assertFalse(cliente._canales)
                    self.assertLess(perf_counter() - inicio, 1)
                    self.assertIn("acuse" if incorrecto else "espera", respuesta["error"])
                finally:
                    espera.wait(timeout=2)
                    hilo.join(timeout=2)
                self.assertFalse(hilo.is_alive())

    def test_eventos_reales_y_empate_concurrente(self):
        carpeta = self.carpeta_temporal()
        estados = {nombre: EstadoNodo(nombre, RegistroEventos(carpeta / f"{nombre}.csv",
                                                               "empate", "sockets", nombre))
                   for nombre in ("nodo-A", "nodo-B", "nodo-C")}
        servidor = ServidorTCP(estados["nodo-C"], 0)
        servidor.iniciar()
        self.addCleanup(servidor.cerrar)
        clientes = [ClienteTCP(estados[nombre]) for nombre in ("nodo-A", "nodo-B")]
        for cliente in clientes:
            self.addCleanup(cliente.cerrar)
        barrera = Barrier(2)

        def enviar(indice):
            barrera.wait(timeout=2)
            return clientes[indice].enviar("nodo-C", servidor.puerto, "independiente", f"E-{indice}")

        with ThreadPoolExecutor(max_workers=2) as hilos:
            resultados = list(hilos.map(enviar, (0, 1)))
        self.assertTrue(all(r["ok"] for r in resultados))
        filas = []
        for ruta in carpeta.glob("*.csv"):
            with ruta.open(encoding="utf-8", newline="") as archivo:
                propias = list(csv.DictReader(archivo))
            relojes = [int(f["reloj"]) for f in propias]
            self.assertTrue(all(a < b for a, b in zip(relojes, relojes[1:])))
            filas.extend(propias)
        por_id = defaultdict(dict)
        for fila in filas:
            por_id[fila["message_id"]][fila["evento"]] = int(fila["reloj"])
        self.assertEqual(len(filas), 8)
        for par in por_id.values():
            self.assertGreater(par["recepcion"], par["envio"])
        self.assertEqual(por_id["E-0"]["envio"], por_id["E-1"]["envio"])

    def test_tres_procesos_con_ejecutor_de_diego(self):
        carpeta = self.carpeta_temporal()
        with patch.dict(os.environ, {"PYTHONDONTWRITEBYTECODE": "1"}):
            verificar_intercambios("sockets", carpeta, 20)
            with ExitStack() as pila:
                nodos = iniciar_nodos(pila, "sockets", "prueba-desarrollo")
                for modo in ("secuencial", "concurrente"):
                    for tamano in (100, 10240):
                        salida = io.StringIO()
                        resultado = medir(nodos, "sockets", f"{modo}-{tamano}", tamano,
                                          modo, False, csv.writer(salida))
                        self.assertEqual(resultado[-2], 100)
                        self.assertEqual(len(salida.getvalue().splitlines()), 100)
        filas = []
        for ruta in (carpeta / "eventos").glob("*.csv"):
            with ruta.open(encoding="utf-8", newline="") as archivo:
                filas.extend(csv.DictReader(archivo))
        self.assertEqual(len(filas), 80)  # 20 solicitudes y sus 20 acuses, envío y recepción.

    def test_cli_responde_errores_y_cierra_con_eof(self):
        # El nodo puede enviarse un mensaje a sí mismo con el servidor en segundo plano.
        with socket.socket() as reserva:
            reserva.bind(("127.0.0.1", 0))
            puerto = reserva.getsockname()[1]
        orden = json.dumps(dict(destino="nodo-A", puerto=puerto,
                                message="desde stdin", message_id="cli"))
        resultado = subprocess.run(
            [sys.executable, "-B", "-m", "src.sockets.nodo", "--nombre", "nodo-A",
             "--puerto", str(puerto), "--corrida", "prueba-cli"],
            input="no es json\n[]\n" + orden + "\n", text=True, encoding="utf-8",
            capture_output=True, timeout=10, cwd=RAIZ)
        self.assertEqual(resultado.returncode, 0, resultado.stderr)
        respuestas = [json.loads(linea) for linea in resultado.stdout.splitlines()]
        self.assertEqual([r["ok"] for r in respuestas], [True, False, False, True])
        self.assertEqual(respuestas[-1]["message_id"], "cli")
        self.assertEqual(resultado.stderr, "")

    def test_cierre_con_cliente_inactivo_y_json_incompleto(self):
        servidor = self.servidor()
        with socket.create_connection(("127.0.0.1", servidor.puerto), timeout=1) as conexion:
            conexion.sendall(b'{"sender":')
            cierre = Thread(target=servidor.cerrar)
            cierre.start()
            try:
                cierre.join(timeout=2)
                self.assertFalse(cierre.is_alive(), "El cierre quedó esperando datos del cliente")
            finally:
                conexion.close()
                cierre.join(timeout=2)

    def test_cli_humo_y_sumas_sha256(self):
        salida = self.carpeta_temporal() / "humo"
        resultado = subprocess.run(
            [sys.executable, "-B", "experiments/ejecutar.py", "--mecanismo", "sockets",
             "--modo", "humo", "--salida", str(salida)],
            capture_output=True, text=True, encoding="utf-8", timeout=15, cwd=RAIZ,
            env=os.environ | {"PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(resultado.returncode, 0, resultado.stderr)
        lineas = (salida / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lineas), 3)
        for linea in lineas:
            suma, relativa = linea.split("  ", 1)
            self.assertEqual(suma, hashlib.sha256((salida / relativa).read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
