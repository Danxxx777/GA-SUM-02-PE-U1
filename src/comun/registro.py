import csv
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock


class RegistroEventos:
    """Escribe un CSV nuevo por nodo y corrida, incluso con varios hilos."""

    CAMPOS = ("corrida", "mecanismo", "message_id", "nodo", "evento", "reloj", "hora_utc")

    def __init__(self, ruta: Path, corrida: str, mecanismo: str, nodo: str) -> None:
        self._ruta = ruta
        self._contexto = (corrida, mecanismo, nodo)
        self._lock = Lock()
        ruta.parent.mkdir(parents=True, exist_ok=True)
        with ruta.open("x", encoding="utf-8", newline="") as archivo:
            csv.writer(archivo).writerow(self.CAMPOS)

    def escribir(self, message_id: str, evento: str, reloj: int) -> None:
        if evento not in {"envio", "recepcion"}:
            raise ValueError("El evento debe ser envio o recepcion")
        corrida, mecanismo, nodo = self._contexto
        with self._lock, self._ruta.open("a", encoding="utf-8", newline="") as archivo:
            hora = datetime.now(timezone.utc).isoformat()
            csv.writer(archivo).writerow((corrida, mecanismo, message_id, nodo, evento, reloj, hora))
