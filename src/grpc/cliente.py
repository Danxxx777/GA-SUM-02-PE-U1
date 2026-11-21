"""RPC de ida y vuelta con plazo máximo y un canal persistente por destino."""

import math
from threading import Lock
from time import perf_counter

import grpc

from src.grpc.generado import mensajes_pb2, mensajes_pb2_grpc
from src.grpc.servidor import HOST, validar_mensaje


class ClienteGRPC:
    def __init__(self, estado, timeout=5.0):
        if not math.isfinite(timeout) or not 0 < timeout <= 10:
            raise ValueError("El timeout debe ser mayor que 0 y como máximo 10 segundos")
        self.estado = estado
        self.timeout = timeout
        self._canales = {}
        self._lock = Lock()

    def enviar(self, destino, puerto, mensaje, identificador):
        if not isinstance(destino, str) or not destino.strip():
            raise ValueError("destino debe ser texto no vacío")
        if type(puerto) is not int or not 1 <= puerto <= 65535:
            raise ValueError("puerto debe ser un entero entre 1 y 65535")
        if not isinstance(mensaje, str):
            raise ValueError("message debe ser texto")
        if not isinstance(identificador, str) or not identificador.strip():
            raise ValueError("message_id debe ser texto no vacío")
        solicitud = mensajes_pb2.Mensaje(
            sender=self.estado.nombre, message=mensaje, message_id=identificador
        )
        validar_mensaje(solicitud)
        clave = (destino, puerto)
        with self._lock:
            limite = perf_counter() + self.timeout
            try:
                if clave not in self._canales:
                    canal = grpc.insecure_channel(
                        f"{HOST}:{puerto}", options=(("grpc.enable_retries", 0),)
                    )
                    self._canales[clave] = (canal, mensajes_pb2_grpc.MensajeriaStub(canal))
                    # Igual que en TCP, establecer la conexión queda fuera de la latencia.
                    grpc.channel_ready_future(canal).result(timeout=self.timeout)
                _, stub = self._canales[clave]
                solicitud.timestamp = self.estado.evento(identificador)
                restante = limite - perf_counter()
                if restante <= 0:
                    raise grpc.FutureTimeoutError()
                inicio = perf_counter()
                acuse = stub.EnviarMensaje(solicitud, timeout=restante, wait_for_ready=False)
                validar_mensaje(acuse)
                if (not acuse.ok or acuse.sender != destino
                        or acuse.message_id != identificador + "-ack"
                        or acuse.ack_for != identificador):
                    raise ValueError("El acuse no corresponde a la solicitud o al destino")
                if acuse.timestamp <= solicitud.timestamp:
                    raise ValueError("El reloj del acuse no es posterior al envío")
                latencia = (perf_counter() - inicio) * 1000
                self.estado.evento(acuse.message_id, acuse.timestamp)
                return dict(message_id=identificador, ok=True, latencia_ms=latencia,
                            acuse=dict(sender=acuse.sender, timestamp=acuse.timestamp,
                                       message=acuse.message, message_id=acuse.message_id,
                                       ack_for=acuse.ack_for, ok=acuse.ok))
            except (grpc.RpcError, grpc.FutureTimeoutError, ValueError, OSError) as exc:
                canal = self._canales.pop(clave, None)
                if canal is not None:
                    canal[0].close()
                if isinstance(exc, grpc.FutureTimeoutError):
                    detalle = f"Tiempo de espera agotado ({self.timeout:g} s)"
                elif isinstance(exc, grpc.RpcError):
                    detalle = f"{exc.code().name}: {exc.details()}"
                else:
                    detalle = str(exc)
                return dict(message_id=identificador, ok=False,
                            error=f"gRPC hacia {destino}:{puerto}: {detalle}")

    def cerrar(self):
        with self._lock:
            for canal, _ in self._canales.values():
                canal.close()
            self._canales.clear()
