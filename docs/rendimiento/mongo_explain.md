# MongoDB: consulta con y sin índice

Colecciones: 23534 trayectos y 24942 anomalías.

| Consulta | Índice | Plan | Docs examinados | Claves examinadas | Devueltos | Servidor (ms) | Cliente, mediana (ms) |
|---|---|---|---|---|---|---|---|
| trayectos {origen, destino} | sin índice | COLLSCAN | 23534 | 0 | 1 | 6 | 5.79 |
| trayectos {origen, destino} | origen_destino | IXSCAN → FETCH | 1 | 1 | 1 | 0 | 0.35 |
| anomalias de un día ordenadas | sin índice | COLLSCAN → SORT | 24942 | 0 | 1155 | 7 | 13.09 |
| anomalias de un día ordenadas | fecha_recogida | IXSCAN → FETCH | 1155 | 1155 | 1155 | 2 | 5.51 |
