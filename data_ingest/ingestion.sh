module load anaconda3/2019.10
conda activate reddit-scraper
python scraper.py
hdfs dfs -rm -r ProjectData
wget -o ../data/us-states.csv https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv
hdfs dfs -put ../data ProjectData
