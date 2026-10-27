# Delta Lake: time travel sobre silver

Se carga 2024-01 (versión 0) y después 2024-02 (versión 1) en la misma tabla Delta.

| Versión | Fecha | Operación | Filas escritas |
|---|---|---|---|
| 0 | 2026-10-27 10:41:02.694000 | WRITE | 2722242 |
| 1 | 2026-10-27 10:41:12.098000 | WRITE | 2718160 |

| Lectura | Filas por mes |
|---|---|
| Última versión | {'2024-02': 2718160, '2024-01': 2722242} |
| `option("versionAsOf", 0)` | {'2024-01': 2722242} |
| `option("timestampAsOf", "2026-10-27 10:41:02.694000")` | {'2024-01': 2722242} |
