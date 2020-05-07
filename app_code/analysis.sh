module purge
module load spark/2.4.0
module load sbt/1.2.8
module load scala/2.11.8
sbt package
hdfs dfs -rm -r -skipTrash Reddit_Mentions States States_Normalized Correlations Summarization
spark2-submit --name "Analysis" --class Analysis --master yarn --deploy-mode cluster --driver-memory 10G --executor-memory 10G --num-executors 20 target/scala-2.11/analysis_2.11-0.1.0-SNAPSHOT.jar
