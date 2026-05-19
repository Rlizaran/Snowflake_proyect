{% docs q_flag_codes %}

Codigo de un caracter publicado por NOAA GHCN-Daily que indica la calidad de la observacion.
Mapping completo q_flag -> q_flag_category -> descripcion:

| q_flag | q_flag_category | Descripcion                                        |
|--------|-----------------|----------------------------------------------------|
| (vacio)| OK              | Sin flag — valor limpio.                           |
| Z      | OK              | Pasa todos los QC checks.                          |
| G      | OK              | Pasa el gap check.                                 |
| S      | SUSPECT         | Sospechoso (NOAA flag de QC).                      |
| I      | INVALID         | Inconsistencia interna detectada por NOAA.         |
| X      | INVALID         | Falla el bound check de NOAA.                      |
| M      | PROCESSING      | Procesado manualmente.                             |
| R      | PROCESSING      | Reemplazado por valor en proceso QC.               |
| D      | PROCESSING      | Eliminado por NOAA (datacheck duplicado).          |
| T      | PROCESSING      | Falla el temperature check.                        |
| N      | PROCESSING      | Falla el naught check.                             |
| L      | METADATA        | Fuera del rango temporal de la estacion.           |
| O      | METADATA        | Outlier respecto a vecinas.                        |
| K      | METADATA        | Falla el streak/frequent-value check.              |
| W      | METADATA        | Falla el spatial regression check.                 |

Cualquier otro valor cae en categoria UNKNOWN (defensa por si NOAA introduce codigos nuevos).

Fuente normalizada: lookup `slv_quality_flag` (silver) y `dim_quality_flag` (gold).

{% enddocs %}
