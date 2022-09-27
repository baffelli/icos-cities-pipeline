library(carboutil)

#Query to estimate the number of calibration runs for each senso
cal_counts_q <- 
"SELECT CUMULATIVE_SUM(FIRST(calibration)) FROM
 (
     SELECT node, value as calibration FROM "measurements" WHERE "node" =~ /{nd}/ AND sensor =~ /calibration/ AND value = 1 
 )
 WHERE time > '{bt}'
GROUP BY node, time(1d)
"


