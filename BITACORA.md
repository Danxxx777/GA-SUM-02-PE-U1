20 de noviembre de 2026 — Preparación de la campaña experimental PE-U1
Integrante participante: GIRALDO CAGUA DIEGO JOSE (dgiraldoc).

Trabajo realizado
- Documenté el protocolo común para sockets TCP y gRPC. Definí cómo se inician los tres nodos, cómo se identifican las solicitudes y los acuses, qué campos se guardan en los CSV y cómo se organizan los calentamientos y las repeticiones. Commit c847bff
- Añadí el reloj de Lamport compartido. El contador aumenta en uno al enviar y se actualiza con max(local, recibido) + 1 al recibir. Protegí las dos operaciones con un bloqueo para evitar que varios hilos actualicen el contador al mismo tiempo. Commit 06a7287
- Añadí el registro de eventos por nodo y corrida. Cada fila guarda el identificador del mensaje, el tipo de evento, el valor del reloj y la hora en UTC. Commit e49947e
- Incorporé el ejecutor que inicia y cierra los tres procesos, coordina los intercambios y las pruebas secuenciales y concurrentes, y guarda las latencias, los resúmenes de corrida y las sumas SHA-256 de los CSV. Commit 6ed3e42
Dificultad y solución. Para aplicar la misma carga a ambos transportes, definí una interfaz común entre el ejecutor y los nodos. El ejecutor envía órdenes en JSON y verifica que cada respuesta corresponda al identificador del mensaje enviado.

Estado al cierre. Quedó pendiente integrar los nodos TCP y gRPC, comprobar un intercambio con cada mecanismo y ejecutar la campaña oficial para entregar los CSV a análisis.
