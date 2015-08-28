#hdfs dfs -ls /user/hduser/Retail2*
#hdfs dfs -mkdir /user/hduser/Retail2-in
#hdfs dfs -copyFromLocal DailySales*.txt /user/hduser/Retail2-in
hdfs dfs -ls /user/hduser/Retail2-in
hdfs dfs -rm -r /user/hduser/Retail2-out0
hadoop jar /usr/local/hadoop220/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar -D mapred.job.name='RetailR2' -mapper /home/hduser/RetailSales/Retail-V2/LinReg2-map.R -reducer /home/hduser/RetailSales/Retail-V2/LinReg2-red.R -input /user/hduser/Retail2-in/* -output /user/hduser/Retail2-out0 
hdfs dfs -ls /user/hduser/Retail2-out0
rm results
hdfs dfs -copyToLocal /user/hduser/Retail2-out0/part-00000 results
cat results

