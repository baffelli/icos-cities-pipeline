INSERT INTO CarboSense.reference_CO2
FROM
(
    SELECT
        'DUE' AS LocationName,
        timestamp,
        CO2 AS CO2
        CO2_F AS CO2_F
        CO2_DRY_CAL AS CO2_DRY,
        CO2_DRY_F AS CO2_DRY_F,
        H2O,
        H2O_F,
        0 AS Valvepos
    FROM NABEL_DUE
    UNION ALL
)
