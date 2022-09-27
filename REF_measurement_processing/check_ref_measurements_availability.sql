WITH HAE AS
(
	SELECT
		date,
		SUM(CO2_WET_COMP <> -999) AS n_valid,
		'HAE' AS LocationName
	FROM
	(
		SELECT
			*,
			DATE(FROM_UNIXTIME(timestamp)) AS date
		FROM NABEL_HAE
	) AS HAE
	GROUP BY date
) ,
RIG AS
(
	SELECT
		date,
		SUM(CO2_WET_COMP <> -999) AS n_valid,
		'RIG' AS LocationName
	FROM
	(
		SELECT
			*,
			DATE(FROM_UNIXTIME(timestamp)) AS date
		FROM NABEL_RIG
	) AS HAE
	GROUP BY date
),
DUE AS
(
	SELECT
		date,
		SUM(CO2_WET_COMP <> -999) AS n_valid,
		'DUE' AS LocationName
	FROM
	(
		SELECT
			*,
			DATE(FROM_UNIXTIME(timestamp)) AS date
		FROM NABEL_DUE
	) AS HAE
	GROUP BY date
),
LAEG AS
(
	SELECT
		date,
		SUM(CO2 <> -999) AS n_valid,
		'LAEG' AS LocationName
	FROM
	(
		SELECT
			*,
			DATE(FROM_UNIXTIME(timestamp)) AS date
		FROM EMPA_LAEG
	) AS LAEG
	GROUP BY date
),
GIMM AS
(
	SELECT
		date,
		SUM(CO2 <> -999) AS n_valid,
		'GIMM' AS LocationName
	FROM
	(
		SELECT
			*,
			DATE(FROM_UNIXTIME(timestamp)) AS date
		FROM UNIBE_GIMM
	) AS GIMM
	GROUP BY date
),
BRM AS
(
	SELECT
		date,
		SUM(CO2 <> -999) AS n_valid,
		'BRM' AS LocationName
	FROM
	(
		SELECT
			*,
			DATE(FROM_UNIXTIME(timestamp)) AS date
		FROM UNIBE_BRM
	) AS BRM
	GROUP BY date
)




SELECT
*
FROM
(
	SELECT
		*
	FROM HAE
	UNION ALL 
	SELECT
		*
	FROM RIG
	UNION ALL
	SELECT
		*
	FROM DUE
	UNION ALL
	SELECT
		*
	FROM LAEG
	UNION ALL
	SELECT
		*
	FROM GIMM
	UNION ALL
	SELECT
		*
	FROM BRM
) AS counts
WHERE YEAR(date) = 2020
ORDER BY date