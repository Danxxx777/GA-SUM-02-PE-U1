## Antes de medir

- Confirmar la entrega de FORM-MC-01 y FORM-TL-01 y recuperar el diagrama del taller.
- Escribir y fechar la hipótesis del equipo antes de la primera campaña de medición.
- Usar tres procesos en localhost, con nodo-A en 5000, nodo-B en 5001 y nodo-C en 5002. Ejecutar los mecanismos por separado.

## Contrato propuesto para TCP y gRPC

- Un reloj por proceso y un registro por nodo y corrida para la prueba Lamport. Usar `RelojLamport.envio()` antes de escribir el mensaje y `RelojLamport.recepcion(timestamp)` al aceptarlo. Registrar el valor devuelto. Ordenar eventos locales por reloj.
- Transportar sender, timestamp, message y message_id. Las solicitudes tienen IDs únicos dentro de la corrida. El acuse confirma el ID y tiene su propio ID, derivado con el sufijo -ack, además de sender, timestamp y message. Aplicar y registrar Lamport al enviar y recibir ambos.
- Propuesta para integrar el ejecutor. Lanzar cada proceso desde la raíz con `python -m src.sockets.nodo` o `python -m src.grpc.nodo`. Ambos reciben `--nombre`, `--puerto`, `--corrida` y, para la verificación, `--eventos RUTA`. Mantener el servidor activo mientras se leen comandos del ejecutor por stdin.
- La primera línea de stdout es un JSON con ok=true cuando el servidor está listo. Cada comando es una línea JSON con destino (nombre), puerto (entero), message y message_id. Responder una línea JSON con message_id y ok. En caso de éxito, incluir latencia_ms medida en el proceso emisor con perf_counter, desde el envío de la solicitud hasta recibir el acuse válido. En caso de error, incluir error y terminar la espera dentro del timeout pactado. Usar stderr para los mensajes de diagnóstico. Cerrar limpiamente al recibir EOF en stdin.
- En las corridas de latencia omitir `--eventos` en ambos transportes para excluir la escritura de eventos del tiempo medido. Mantener el reloj activo. Reutilizar una conexión TCP por par ordenado de nodos y un canal gRPC equivalente. Establecer las conexiones durante el calentamiento y reutilizarlas en las cinco repeticiones válidas. Mantener la misma política de timeout en ambos.

## Campaña y datos

1. Ejecutar 20 solicitudes, repitiendo la ruta A → B, B → C, C → A hasta completar las 20. Registrar solicitudes y acuses. Para observar un empate concurrente, usar una corrida adicional con relojes reiniciados y los primeros envíos de A y B hacia C, liberados mediante una barrera. A y B no se envían mensajes entre sí. Revisar en ambos CSV el valor de esos primeros envíos.
2. Medir cargas útiles UTF-8 de 100 y 10240 bytes. Por mecanismo, tamaño y modo secuencial, realizar un calentamiento y cinco corridas registradas de 100 envíos cada una. Conservar el calentamiento y excluirlo del análisis principal.
3. Repetir con modo concurrente en ambos mecanismos y tamaños. Iniciar los tres emisores desde una barrera del ejecutor y distribuir las 100 solicitudes en 34, 33 y 33. Mantener las mismas rutas y distribución para TCP y gRPC.
4. Guardar los CSV crudos en data/sockets y data/grpc. Usar un archivo de eventos por nodo y corrida. Esquema: corrida,mecanismo,message_id,nodo,evento,reloj,hora_utc.
5. Para latencia: corrida,mecanismo,tamano_bytes,modo,calentamiento,message_id,emisor,destino,latencia_ms,ok,error. Para el resumen de corrida: corrida,mecanismo,tamano_bytes,modo,calentamiento,solicitudes_programadas,solicitudes_completadas,duracion_s. Usar 0 y 1 para los indicadores. Cronometrar la duración de pared desde que se libera la carga hasta que terminan todos los envíos o se agotan sus tiempos de espera. Conservar los fallos.
6. Registrar sistema operativo, procesador, memoria, versión de Python y modo de ejecución. Calcular SHA-256 de los CSV cerrados y entregar a John sus rutas junto con el esquema.

