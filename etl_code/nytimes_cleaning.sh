cd NYTimes
module purge
module load spark/2.4.0
module load sbt/1.2.8
module load scala/2.11.8
sbt package
hdfs dfs -rm -r NYTimes_Cleaning
spark2-submit --name "NYTimes_Cleaning" --class NYTimes_Cleaning --master yarn --deploy-mode cluster --driver-memory 5G --executor-memory 2G --num-executors 10 target/scala-2.11/nytimes_2.11-0.1.0-SNAPSHOT.jar
