"""Servidor TCP y mensajes JSON delimitados por un salto de línea."""

import json
import socket
import sys
from threading import Event, Lock, Thread
from time import perf_counter

from src.comun.lamport import RelojLamport

HOST = "127.0.0.1"
MAX_MENSAJE = 1024 * 1024  # Límite del JSON completo, sin el delimitador.


def rechazar_constante(valor):
    raise ValueError(f"Constante JSON no válida: {valor}")


class CanalJSON:
    """Conserva bytes sobrantes entre lecturas del mismo flujo TCP."""

    def __init__(self, conexion):
        self.conexion = conexion
        self.pendiente = bytearray()

    def enviar(self, mensaje, limite=None):
        datos = json.dumps(mensaje, ensure_ascii=False, allow_nan=False,
                           separators=(",", ":")).encode("utf-8")
        if len(datos) > MAX_MENSAJE:
            raise ValueError("El mensaje JSON supera 1 MiB")
        self._timeout(limite)
        self.conexion.sendall(datos + b"\n")

    def recibir(self, limite=None):
        while True:
            fin = self.pendiente.find(b"\n")
            if fin >= 0:
                if fin > MAX_MENSAJE:
                    raise ValueError("El mensaje JSON supera 1 MiB")
                datos = bytes(self.pendiente[:fin])
                del self.pendiente[:fin + 1]
                return json.loads(datos.decode("utf-8"),
                                  parse_constant=rechazar_constante)
            if len(self.pendiente) > MAX_MENSAJE:
                raise ValueError("El mensaje JSON supera 1 MiB")
            self._timeout(limite)
            datos = self.conexion.recv(65536)
            if not datos:
                if self.pendiente:
                    raise ValueError("La conexión terminó con un JSON incompleto")
                raise EOFError("El destino cerró la conexión")
            self.pendiente.extend(datos)

    def _timeout(self, limite):
        if limite is not None:
            restante = limite - perf_counter()
            if restante <= 0:
                raise TimeoutError("Se agotó el tiempo de espera TCP")
            self.conexion.settimeout(restante)


def validar_mensaje(mensaje):
    if not isinstance(mensaje, dict):
        raise ValueError("Se esperaba un objeto JSON")
    for campo in ("sender", "message_id"):
        if not isinstance(mensaje.get(campo), str) or not mensaje[campo].strip():
            raise ValueError(f"{campo} debe ser texto no vacío")
    if not isinstance(mensaje.get("message"), str):
        raise ValueError("message debe ser texto")
    reloj = mensaje.get("timestamp")
    if type(reloj) is not int or reloj < 0:
        raise ValueError("timestamp debe ser un entero no negativo")


class EstadoNodo:
    """Un reloj por proceso; el avance y su registro forman una operación local."""

    def __init__(self, nombre, registro=None):
        self.nombre = nombre
        self.reloj = RelojLamport()
        self.registro = registro
        self._lock = Lock()

    def evento(self, identificador, remoto=None):
        with self._lock:
            tipo = "envio" if remoto is None else "recepcion"
            valor = self.reloj.envio() if remoto is None else self.reloj.recepcion(remoto)
            if self.registro is not None:
                self.registro.escribir(identificador, tipo, valor)
            return valor


class ServidorTCP:
    """Atiende conexiones persistentes en hilos y las cierra al terminar el nodo."""

    def __init__(self, estado, puerto):
        self.estado = estado
        self._detener = Event()
        self._lock = Lock()
        self._conexiones = set()
        self._hilos = []
        self._servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # En Windows evita que dos servidores compartan accidentalmente el puerto.
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                self._servidor.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                self._servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._servidor.bind((HOST, puerto))
            self._servidor.listen()
            self._servidor.settimeout(0.2)
        except OSError:
            self._servidor.close()
            raise
        self.puerto = self._servidor.getsockname()[1]
        self._aceptador = Thread(target=self._aceptar, name="tcp-servidor")

    def iniciar(self):
        self._aceptador.start()

    def _aceptar(self):
        while not self._detener.is_set():
            try:
                conexion, _ = self._servidor.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self._detener.is_set():
                    print("Error aceptando una conexión TCP", file=sys.stderr)
                return
            hilo = Thread(target=self._atender, args=(conexion,), name="tcp-cliente")
            with self._lock:
                self._conexiones.add(conexion)
                # Descartar los hilos que ya terminaron para no acumularlos.
                self._hilos = [anterior for anterior in self._hilos if anterior.is_alive()]
                self._hilos.append(hilo)
            hilo.start()

    def _atender(self, conexion):
        canal = CanalJSON(conexion)
        try:
            while not self._detener.is_set():
                solicitud = canal.recibir()
                validar_mensaje(solicitud)
                self.estado.evento(solicitud["message_id"], solicitud["timestamp"])
                identificador = solicitud["message_id"] + "-ack"
                acuse = dict(sender=self.estado.nombre,
                             timestamp=self.estado.evento(identificador),
                             message="Mensaje recibido", message_id=identificador,
                             ack_for=solicitud["message_id"], ok=True)
                canal.enviar(acuse)
        except EOFError:
            pass
        except (ValueError, UnicodeError) as exc:
            try:
                canal.enviar(dict(ok=False, error=str(exc)))
            except OSError:
                pass
        except OSError as exc:
            if not self._detener.is_set():
                print(f"{self.estado.nombre}: conexión TCP: {exc}", file=sys.stderr)
        finally:
            with self._lock:
                self._conexiones.discard(conexion)
            conexion.close()

    def cerrar(self):
        self._detener.set()
        if self._aceptador.is_alive():
            self._aceptador.join()
        self._servidor.close()
        with self._lock:
            conexiones = list(self._conexiones)
            hilos = list(self._hilos)
        for conexion in conexiones:
            try:
                conexion.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conexion.close()
        for hilo in hilos:
            hilo.join()
