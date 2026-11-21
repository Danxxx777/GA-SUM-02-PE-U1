"""Cliente de ida y vuelta con acuses, timeout y conexiones persistentes."""

import math
import socket
from threading import Lock
from time import perf_counter

from src.sockets.servidor import HOST, CanalJSON, validar_mensaje


class ClienteTCP:
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
        solicitud = dict(sender=self.estado.nombre, timestamp=0,
                         message=mensaje, message_id=identificador)
        validar_mensaje(solicitud)
        clave = (destino, puerto)
        with self._lock:
            limite = perf_counter() + self.timeout
            try:
                if clave not in self._canales:
                    conexion = socket.create_connection((HOST, puerto), timeout=self.timeout)
                    self._canales[clave] = CanalJSON(conexion)
                canal = self._canales[clave]
                solicitud["timestamp"] = self.estado.evento(identificador)
                # La conexión inicial se establece antes del intervalo de ida y vuelta.
                inicio = perf_counter()
                canal.enviar(solicitud, limite)
                acuse = canal.recibir(limite)
                if isinstance(acuse, dict) and acuse.get("ok") is False:
                    raise ValueError(f"El destino rechazó el mensaje: {acuse.get('error', '')}")
                validar_mensaje(acuse)
                if (acuse.get("ok") is not True or acuse["sender"] != destino
                        or acuse["message_id"] != identificador + "-ack"
                        or acuse.get("ack_for") != identificador):
                    raise ValueError("El acuse no corresponde a la solicitud o al destino")
                if acuse["timestamp"] <= solicitud["timestamp"]:
                    raise ValueError("El reloj del acuse no es posterior al envío")
                latencia = (perf_counter() - inicio) * 1000
                self.estado.evento(acuse["message_id"], acuse["timestamp"])
                return dict(message_id=identificador, ok=True,
                            latencia_ms=latencia, acuse=acuse)
            except (OSError, EOFError, ValueError) as exc:
                # Tras un timeout no se reutiliza un flujo con un acuse tardío pendiente.
                canal = self._canales.pop(clave, None)
                if canal is not None:
                    canal.conexion.close()
                if isinstance(exc, TimeoutError):
                    detalle = f"Tiempo de espera agotado ({self.timeout:g} s)"
                else:
                    detalle = str(exc)
                return dict(message_id=identificador, ok=False,
                            error=f"TCP hacia {destino}:{puerto}: {detalle}")

    def cerrar(self):
        with self._lock:
            for canal in self._canales.values():
                canal.conexion.close()
            self._canales.clear()
