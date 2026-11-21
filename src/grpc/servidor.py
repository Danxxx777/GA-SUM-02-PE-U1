"""Servicio gRPC con un reloj Lamport y registro opcional por nodo."""

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import grpc

from src.comun.lamport import RelojLamport
from src.grpc.generado import mensajes_pb2, mensajes_pb2_grpc

HOST = "127.0.0.1"


def validar_mensaje(mensaje):
    for campo in ("sender", "message_id"):
        if not getattr(mensaje, campo).strip():
            raise ValueError(f"{campo} debe ser texto no vacío")
    if mensaje.timestamp < 0:
        raise ValueError("timestamp debe ser un entero no negativo")


class EstadoNodo:
    """Agrupa el avance del reloj con su registro para conservar el orden local."""

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


class ServicioMensajeria(mensajes_pb2_grpc.MensajeriaServicer):
    def __init__(self, estado):
        self.estado = estado

    def EnviarMensaje(self, request, context):
        try:
            validar_mensaje(request)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        self.estado.evento(request.message_id, request.timestamp)
        identificador = request.message_id + "-ack"
        return mensajes_pb2.Acuse(
            sender=self.estado.nombre,
            timestamp=self.estado.evento(identificador),
            message="Mensaje recibido", message_id=identificador,
            ack_for=request.message_id, ok=True,
        )


class ServidorGRPC:
    def __init__(self, estado, puerto):
        self._hilos = ThreadPoolExecutor(max_workers=4)
        self._servidor = grpc.server(
            self._hilos, options=(("grpc.so_reuseport", 0),)
        )
        mensajes_pb2_grpc.add_MensajeriaServicer_to_server(
            ServicioMensajeria(estado), self._servidor
        )
        try:
            self.puerto = self._servidor.add_insecure_port(f"{HOST}:{puerto}")
            if not self.puerto:
                raise RuntimeError(f"No se pudo abrir el puerto {puerto}")
        except RuntimeError:
            self.cerrar()
            raise

    def iniciar(self):
        self._servidor.start()

    def cerrar(self):
        self._servidor.stop(0).wait(timeout=2)
        self._hilos.shutdown(wait=True)
