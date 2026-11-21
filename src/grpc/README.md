# Integración del bloque gRPC

## Instalación y generación

Desde la raíz del repositorio, con Python 3.11 o superior. Estos comandos son para `cmd.exe`:

```bat
python -m venv .venv
.venv\Scripts\activate
python -m pip install grpcio==1.85.3 grpcio-tools==1.85.3 protobuf==7.36.2
python -m grpc_tools.protoc -Isrc/grpc/generado=proto --python_out=. --grpc_python_out=. proto/mensajes.proto
python -B -c "from src.grpc.generado import mensajes_pb2, mensajes_pb2_grpc; from src.grpc.nodo import main; print('Importaciones correctas')"
```

Los dos archivos de `generado/` salen directamente de `grpcio-tools`, sin editar sus importaciones. Las versiones indicadas son las utilizadas en las comprobaciones técnicas de Codex con Python 3.12. Diego puede incorporarlas al `requirements.txt` general junto con las dependencias del análisis.

Referencias técnicas: [generación y servicio en Python](https://grpc.io/docs/languages/python/basics/) y [módulos generados](https://grpc.io/docs/languages/python/generated-code/).

## Intercambio manual para Byron

Abrir tres terminales desde la raíz y activar el entorno en cada una. Iniciar un nodo por terminal:

```bat
python -m src.grpc.nodo --nombre nodo-A --puerto 5000 --corrida manual-byron --eventos data/grpc/manual-A.csv
python -m src.grpc.nodo --nombre nodo-B --puerto 5001 --corrida manual-byron --eventos data/grpc/manual-B.csv
python -m src.grpc.nodo --nombre nodo-C --puerto 5002 --corrida manual-byron --eventos data/grpc/manual-C.csv
```

Cada comando se ejecuta en su propia terminal y cada CSV debe ser nuevo. Tras el mensaje inicial `ok=true`, escribir esta orden en A como una sola línea:

```json
{"destino":"nodo-B","puerto":5001,"message":"Intercambio manual de Byron","message_id":"B-001"}
```

La respuesta exitosa contiene `latencia_ms` y un acuse de B con `ack_for=B-001` y `message_id=B-001-ack`. Para cerrar en `cmd.exe`, usar Ctrl+Z y Enter; también se admite Ctrl+C.

Para la captura personal, Byron debe realizar el intercambio en su equipo y mostrar su identificación y la fecha y hora reales del sistema. Una imagen tomada por Codex no acreditaría esa ejecución personal. Esta entrega no incluye una captura de Byron.

## Comprobación de desarrollo e integración

```bat
python -B -m unittest discover -s tests -p test_grpc.py -v
```

Las pruebas crean CSV temporales fuera de `data/` y los eliminan al terminar. Incluyen 20 intercambios con tres procesos y 100 solicitudes por combinación de tamaño (100 y 10240 bytes) y modo (secuencial y concurrente). Comprueban los pares de eventos de solicitudes y acuses, el crecimiento local del reloj y dos envíos independientes con el mismo valor. No ejecutan las cinco repeticiones de la campaña oficial ni sirven para concluir qué mecanismo es más rápido.

Para que Diego compruebe el modo de humo, con una ruta de salida que todavía no exista:

```bat
python experiments/ejecutar.py --mecanismo grpc --modo humo --salida data/prueba-grpc
```

El ejecutor original admite `--mecanismo ambos` sin cambiar la carga. El cliente gRPC usa timeout de 5 segundos, admite `--timeout` hasta 10 segundos y conserva el canal por destino; no reintenta solicitudes automáticamente. Los puertos 5000, 5001 y 5002 deben estar libres y TCP y gRPC se ejecutan por separado.

## Entrega a Diego y John

- La sección `docs/secciones/grpc.tex` se puede incluir desde `docs/informe.tex` con `\input{secciones/grpc}` al compilar desde `docs/`; no necesita paquetes adicionales.
- El código respeta las columnas emitidas por `RegistroEventos`: `corrida,mecanismo,message_id,nodo,evento,reloj,hora_utc`.
- En la versión revisada, `analysis/verificar_lamport.py` espera `id_mensaje,tipo_evento,hora_real` en lugar de `message_id,evento,hora_utc`. John debe integrar ese contrato antes de usarlo sobre los CSV del ejecutor. Esta entrega no cambia el módulo de análisis.
- La campaña oficial, los datos crudos y las sumas SHA-256 corresponden al flujo de Diego. El modo de humo y las comprobaciones de Codex no sustituyen esa campaña.

## Procedencia y estado de autoría

Codex escribió el contrato `.proto`, los tres módulos gRPC, `tests/test_grpc.py`, esta documentación y `docs/secciones/grpc.tex`. `grpcio-tools` generó los dos módulos de `generado/`. Se reutilizaron el reloj, el registro y el ejecutor que ya estaban en el repositorio, y se siguió la interfaz del transporte TCP existente.

Las verificaciones técnicas ejecutadas por Codex no acreditan una revisión ni una ejecución personal de Byron. La asistencia aquí descrita también forma parte de la declaración de IA del bloque en `grpc.tex`. No se atribuyen a Byron mediciones, capturas o sesiones que no haya realizado. Un commit publicado por Codex no acredita por sí mismo los tres commits significativos personales ni los mensajes escritos por el integrante que exige la guía.
