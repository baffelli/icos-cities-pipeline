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
) 
)



SELECT
*
FROM HAE
UNION ALL 
SELECT
*
FROM RIG