
 SELECT *
 FROM
   (SELECT SensorUnit_ID,
           Date_UTC_from as ts,
           d.LocationName as LocationName,
           Remark,
           HeightAboveGround,
           h,
           LAT_WGS84,
           LON_WGS84,
           H_WGS84,
           Network,
           SiteType,
           1 as deployed
    FROM Deployment AS d
    LEFT JOIN `Location` AS l ON d.LocationName = l.LocationName
    WHERE SensorUnit_ID = {dev}

    UNION

    SELECT SensorUnit_ID,
           Date_UTC_to as ts,
           d.LocationName as LocationName,
           Remark,
           HeightAboveGround,
           h,
           LAT_WGS84,
           LON_WGS84,
           H_WGS84,
           Network,
           SiteType,
           0 as deployed
    FROM Deployment AS d
    LEFT JOIN `Location` AS l ON d.LocationName = l.LocationName
    WHERE SensorUnit_ID = {dev}

    UNION

    SELECT SensorUnit_ID,
                 Date_UTC_from,
                 NULL AS LocationName,
                 Reason AS Remark,
                 NULL,
                 NULL,
                 NULL,
                 NULL,
                 NULL,
                 NULL,
                 NULL,
                 0 as deployed
    FROM SensorExclusionPeriods
    WHERE SensorUnit_ID = {dev}

    UNION

    SELECT SensorUnit_ID,
                 Date_UTC_to,
                 NULL AS LocationName,
                 Reason AS Remark,
                 NULL,
                 NULL,
                 NULL,
                 NULL,
                 NULL,
                 NULL,
                 NULL,
                 1 as deployed
    FROM SensorExclusionPeriods
    WHERE SensorUnit_ID = {dev})
 ORDER BY ts
