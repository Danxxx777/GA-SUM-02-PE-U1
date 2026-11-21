20 de noviembre de 2026 — Preparación de la campaña experimental PE-U1
Integrante participante: GIRALDO CAGUA DIEGO JOSE (dgiraldoc).

Trabajo realizado
- Documenté el protocolo común para sockets TCP y gRPC. Definí cómo se inician los tres nodos, cómo se identifican las solicitudes y los acuses, qué campos se guardan en los CSV y cómo se organizan los calentamientos y las repeticiones. Commit c847bff
- Añadí el reloj de Lamport compartido. El contador aumenta en uno al enviar y se actualiza con max(local, recibido) + 1 al recibir. Protegí las dos operaciones con un bloqueo para evitar que varios hilos actualicen el contador al mismo tiempo. Commit 06a7287
- Añadí el registro de eventos por nodo y corrida. Cada fila guarda el identificador del mensaje, el tipo de evento, el valor del reloj y la hora en UTC. Commit e49947e
- Incorporé el ejecutor que inicia y cierra los tres procesos, coordina los intercambios y las pruebas secuenciales y concurrentes, y guarda las latencias, los resúmenes de corrida y las sumas SHA-256 de los CSV. Commit 6ed3e42
Dificultad y solución. Para aplicar la misma carga a ambos transportes, definí una interfaz común entre el ejecutor y los nodos. El ejecutor envía órdenes en JSON y verifica que cada respuesta corresponda al identificador del mensaje enviado.

Estado al cierre. Quedó pendiente integrar los nodos TCP y gRPC, comprobar un intercambio con cada mecanismo y ejecutar la campaña oficial para entregar los CSV a análisis.

21 de noviembre de 2026 — Implementación y comprobación del transporte TCP
Integrante participante: CORONEL GOMEZ KHRIZ ALEXANDER.

Trabajo realizado
- Se incorporó el servidor TCP con mensajes JSON UTF-8 delimitados por salto de línea, reconstrucción de mensajes fragmentados, validación de campos y acuses correlacionados.
- Se incorporó el cliente con conexiones persistentes, medición de ida y vuelta con perf_counter, plazo máximo por operación y descarte de conexiones tras errores.
- Se integró el nodo con la interfaz JSON por stdin/stdout del ejecutor, el reloj compartido y el registro opcional de eventos.
- Se añadió la sección sockets.tex con las decisiones de implementación y los comandos de arranque, y se preparó evidencias/khriz para la captura del intercambio.
- Pasaron 14 pruebas automatizadas: incluyen los tres procesos, 20 intercambios, 400 solicitudes en ambos tamaños y modos, errores de conexión, mensajes fragmentados y comprobación de SHA-256. Los CSV de estas pruebas fueron temporales y no forman parte de la campaña oficial.

Dificultad y solución. El entorno aislado bloqueó las conexiones locales con WinError 10013; las pruebas se ejecutaron con permiso fuera del aislamiento. Para mantener el orden local entre hilos, se agrupó el avance de Lamport con su registro mediante un bloqueo.

Asistencia de IA. Codex apoyó la implementación TCP, las pruebas y el borrador de la sección del informe. Esta asistencia deberá recogerse también en la declaración de IA del informe general.

Estado al cierre. Pendientes la captura real del intercambio en el equipo del integrante, la revisión del código para la demostración y la inclusión y compilación de sockets.tex en el informe general. La campaña oficial y la integración del transporte gRPC corresponden a las siguientes tareas del equipo.
