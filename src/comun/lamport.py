from threading import Lock


class RelojLamport:
    """Contador compartido por los hilos de un solo nodo."""

    def __init__(self) -> None:
        self._valor = 0
        self._lock = Lock()

    def envio(self) -> int:
        with self._lock:
            self._valor += 1
            return self._valor

    def recepcion(self, remoto: int) -> int:
        if type(remoto) is not int or remoto < 0:
            raise ValueError("El reloj recibido debe ser un entero no negativo")
        with self._lock:
            self._valor = max(self._valor, remoto) + 1
            return self._valor
