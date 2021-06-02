rm -r summary
rm -r results
hdfs dfs -copyToLocal /user/currieferg/FlightDelayResult/summary .
hdfs dfs -copyToLocal /user/currieferg/FlightDelayResult/results .
hdfs dfs -rm -r -skipTrash /user/currieferg/FlightDelayResult/summary
hdfs dfs -rm -r -skipTrash /user/currieferg/FlightDelayResult/results
