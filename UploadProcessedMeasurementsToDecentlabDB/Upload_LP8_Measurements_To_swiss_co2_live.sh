#!/usr/bin/bash

for SensorUnit_ID in `seq -s ' ' 1010 1334`
  do
  
  dayOfMonth=`date +%d`
  timestamp=`date +%s`
  timestamp=$((timestamp-1814400))

  if [ $dayOfMonth == "01" ]
    then
    curl -G -H 'Authorization: Bearer eyJrIjoiSFd4bWJhczJjclpaUnpHeXluck1WYlJ0MkdINWhneFciLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0=' 'https://swiss.co2.live/api/datasources/proxy/2/query?db=processed&pretty=true' -v --data-urlencode "q=drop series from \"measurements\" where uqk = '${SensorUnit_ID}.co2.lp8_level_00'"
  else
    curl -G -H 'Authorization: Bearer eyJrIjoiSFd4bWJhczJjclpaUnpHeXluck1WYlJ0MkdINWhneFciLCJuIjoibWljaGFlbC5tdWVsbGVyQGVtcGEuY2giLCJpZCI6MX0=' 'https://swiss.co2.live/api/datasources/proxy/2/query?db=processed&pretty=true' -v --data-urlencode "q=DELETE FROM \"measurements\" WHERE uqk = '${SensorUnit_ID}.co2.lp8_level_00' AND time > ${timestamp}s AND time < now()"
  fi
  
  Rscript /project/CarboSense/Software/UploadProcessedMeasurementsToDecentlabDB/Upload_LP8_processed_one_sensor_to_Decentlab_DB.r ${SensorUnit_ID}
  
done

