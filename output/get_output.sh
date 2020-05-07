rm -rf Reddit_Mentions
rm -rf States
rm -rf States_Normalized
rm -rf Correlations
rm -rf NYTimes_Cleaning
hdfs dfs -get Reddit_Mentions
hdfs dfs -get States
hdfs dfs -get States_Normalized
hdfs dfs -get Correlations
hdfs dfs -get NYTimes_Cleaning
