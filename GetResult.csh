rm -rf summary
rm -rf results
hdfs dfs -copyToLocal /user/currieferg/OnlineNewsPopRes/summary .
hdfs dfs -copyToLocal /user/currieferg/OnlineNewsPopRes/results .
hdfs dfs -rm -r -skipTrash /user/currieferg/OnlineNewsPopRes/summary
hdfs dfs -rm -r -skipTrash /user/currieferg/OnlineNewsPopRes/results
