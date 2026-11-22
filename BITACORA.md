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

21 de noviembre de 2026 - Integración técnica del transporte gRPC
Operador de la implementación y las comprobaciones: Codex, por solicitud de Byron Daniel Loor Mendoza. Este registro no atribuye una sesión de programación ni ejecución personal a Byron.

Trabajo realizado por Codex
- Se definió proto/mensajes.proto con el servicio EnviarMensaje, solicitud y acuse correlacionado, y se generaron los módulos con grpcio-tools sin editar sus importaciones.
- Se implementaron servidor, cliente y nodo gRPC con el reloj y registro comunes, canales persistentes, timeout de 5 segundos e interfaz compatible con experiments/ejecutar.py.
- Se añadieron ocho pruebas de desarrollo gRPC. Pasaron las 22 pruebas del repositorio, incluidas las 14 existentes de TCP. Se comprobó la integración gRPC con tres procesos, 20 intercambios y 100 solicitudes por cada tamaño y modo; los CSV fueron temporales y no forman parte de la campaña oficial.
- Se documentaron el contrato, los comandos y la asistencia en docs/secciones/grpc.tex y src/grpc/README.md. Las versiones utilizadas fueron Python 3.12, grpcio 1.85.3, grpcio-tools 1.85.3 y protobuf 7.36.2.

Dificultad y solución. La generación inicial debía conservar importaciones de paquete sin modificar código generado; se utilizó el prefijo virtual -Isrc/grpc/generado=proto. Las pruebas admiten diagnósticos de cierre de gRPC en stderr y verifican que stdout conserve respuestas JSON.

Integración pendiente. La captura y ejecución personales de Byron, su revisión para la demostración y sus aportes y mensajes de commit propios no se acreditan mediante esta asistencia. Diego integra las dependencias y la sección en el informe general. John debe conciliar los nombres de columnas esperados por su verificador con los emitidos por RegistroEventos. No se modificó código de Diego, Khriz ni John ni se ejecutó la campaña oficial.

21 de noviembre de 2026 - Práctica manual de gRPC en el equipo de Byron
Operador de los nodos y los envíos: Byron Daniel Loor Mendoza. Codex proporcionó los comandos y la orientación, revisó los archivos y conservó las capturas originales suministradas por Byron.

Trabajo verificable
- Byron inició nodo-A, nodo-B y nodo-C en tres terminales de su equipo y ejecutó los intercambios B-001 (A hacia B), B-002 (B hacia C) y B-003 (C hacia A). Cerró los tres procesos mediante EOF. Las capturas muestran el usuario, el equipo, la fecha y la zona horaria del sistema.
- Los CSV de data/grpc/practica-byron-01 conservan 12 eventos: envío y recepción de tres solicitudes y tres acuses. Los eventos registrados van de las 18:49:51 a las 18:52:27 del 21 de noviembre de 2026, UTC-05:00. Codex comprobó que cada recepción tiene un reloj mayor que su envío y que el reloj local aumenta en cada nodo.
- Se conservaron seis capturas originales, sin edición, en evidencias/byron/: tres del intercambio y tres del cierre. Codex verificó que las copias coinciden mediante SHA-256 con los archivos adjuntados por Byron.
- Después del cierre se generó y verificó data/grpc/practica-byron-01/SHA256SUMS para los tres CSV, sin alterar sus bytes. Las rutas del manifiesto son relativas a esa carpeta.
- Codex añadió la regla data/**/*.csv -text en .gitattributes para conservar los bytes de los datos al usar Git, sin convertir los saltos de línea según el sistema operativo.
- Codex actualizó grpc.tex y la documentación del bloque con la práctica observada, sus archivos y el alcance de la asistencia. Se mantiene separada la ejecución manual de Byron de las comprobaciones técnicas del asistente.

Alcance. Esta sesión acredita la práctica manual descrita y sus evidencias. No es la campaña oficial: no contiene las cinco repeticiones, los dos tamaños de carga ni la prueba concurrente. Tampoco acredita modificaciones propias de código, los tres commits significativos ni dominio del código en la demostración. La implementación gRPC y esta documentación conservan la declaración de asistencia de Codex.
