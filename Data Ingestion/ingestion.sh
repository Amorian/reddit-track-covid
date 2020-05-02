module load anaconda3/2019.10
conda activate reddit-scraper
python scraper.py
hdfs dfs -rm -r ProjectData
hdfs dfs -put ../data ProjectData
