-- slv_quality_flag: lookup normalizado del q_flag NOAA y su q_flag_category.

select
    -- PK
    q_flag,

    -- atributos
    q_flag_category,
    description
from (
    values
        ('',  'OK',         'Sin flag (valor limpio).'),
        ('Z', 'OK',         'Pasa todos los QC checks.'),
        ('G', 'OK',         'Pasa el gap check.'),
        ('S', 'SUSPECT',    'Sospechoso (NOAA flag de QC).'),
        ('I', 'INVALID',    'Inconsistencia interna detectada por NOAA.'),
        ('X', 'INVALID',    'Falla el bound check de NOAA.'),
        ('M', 'PROCESSING', 'Procesado manualmente.'),
        ('R', 'PROCESSING', 'Reemplazado por valor en proceso QC.'),
        ('D', 'PROCESSING', 'Eliminado por NOAA (datacheck duplicado).'),
        ('T', 'PROCESSING', 'Falla el temperature check.'),
        ('N', 'PROCESSING', 'Falla el naught check.'),
        ('L', 'METADATA',   'Fuera del rango temporal de la estacion.'),
        ('O', 'METADATA',   'Outlier respecto a vecinas.'),
        ('K', 'METADATA',   'Falla el streak/frequent-value check.'),
        ('W', 'METADATA',   'Falla el spatial regression check.')
) as t(q_flag, q_flag_category, description)
