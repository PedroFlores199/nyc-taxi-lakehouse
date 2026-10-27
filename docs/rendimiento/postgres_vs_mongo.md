# Demanda de una zona por hora: PostgreSQL frente a MongoDB

Mediana y percentil 95 de 200 ejecuciones por zona, medidos desde el cliente.

| Zona | PostgreSQL mediana (ms) | PostgreSQL p95 (ms) | MongoDB mediana (ms) | MongoDB p95 (ms) |
|---|---|---|---|---|
| 132 | 15.02 | 17.21 | 0.53 | 1.28 |
| 161 | 9.17 | 10.85 | 0.45 | 0.76 |
| 230 | 9.15 | 11.2 | 0.45 | 0.82 |
| 236 | 7.86 | 9.11 | 0.45 | 1.0 |
| 61 | 0.5 | 0.7 | 0.41 | 0.89 |

Tabla de hechos: 1033972 filas. Colección zonas: un documento por zona.
