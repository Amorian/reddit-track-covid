cd Reddit
module purge
module load spark/2.4.0
module load sbt/1.2.8
module load scala/2.11.8
sbt package
hdfs dfs -rm -r Reddit_Cleaning
spark2-submit --name "Reddit_Posts_Cleaning" --class Reddit_Posts_Cleaning --master yarn --deploy-mode cluster --driver-memory 5G --executor-memory 2G --num-executors 10 target/scala-2.11/reddit_2.11-0.1.0-SNAPSHOT.jar
spark2-submit --name "Reddit_Comments_Cleaning" --class Reddit_Comments_Cleaning --master yarn --deploy-mode cluster --driver-memory 5G --executor-memory 2G --num-executors 10 target/scala-2.11/reddit_2.11-0.1.0-SNAPSHOT.jar
