"""Nodo gRPC con servidor activo y órdenes JSON por la entrada estándar."""

import argparse
import json
import sys
from contextlib import ExitStack
from pathlib import Path

from src.comun.registro import RegistroEventos
from src.grpc.cliente import ClienteGRPC
from src.grpc.servidor import EstadoNodo, ServidorGRPC


def rechazar_constante(valor):
    raise ValueError(f"Constante JSON no válida: {valor}")


def responder(respuesta):
    print(json.dumps(respuesta, ensure_ascii=True, allow_nan=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nombre", required=True, choices=("nodo-A", "nodo-B", "nodo-C"))
    parser.add_argument("--puerto", required=True, type=int)
    parser.add_argument("--corrida", required=True)
    parser.add_argument("--eventos", type=Path)
    parser.add_argument("--timeout", type=float, default=5.0,
                        help="Tiempo máximo por operación, mayor que 0 y hasta 10 segundos")
    args = parser.parse_args()
    if not 1 <= args.puerto <= 65535:
        parser.error("--puerto debe estar entre 1 y 65535")

    try:
        with ExitStack() as pila:
            registro = None if args.eventos is None else RegistroEventos(
                args.eventos, args.corrida, "grpc", args.nombre
            )
            estado = EstadoNodo(args.nombre, registro)
            cliente = ClienteGRPC(estado, args.timeout)
            pila.callback(cliente.cerrar)
            servidor = ServidorGRPC(estado, args.puerto)
            pila.callback(servidor.cerrar)
            servidor.iniciar()
            responder(dict(ok=True, nombre=args.nombre, puerto=servidor.puerto))
            for linea in sys.stdin:
                orden = None
                try:
                    orden = json.loads(linea, parse_constant=rechazar_constante)
                    if not isinstance(orden, dict):
                        raise ValueError("La orden debe ser un objeto JSON")
                    respuesta = cliente.enviar(orden.get("destino"), orden.get("puerto"),
                                               orden.get("message"), orden.get("message_id"))
                except (ValueError, UnicodeError) as exc:
                    identificador = orden.get("message_id") if isinstance(orden, dict) else None
                    respuesta = dict(message_id=identificador, ok=False, error=str(exc))
                responder(respuesta)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"{args.nombre}: {exc}", file=sys.stderr)
        responder(dict(ok=False, error=str(exc)))
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
