#!/bin/bash
now=$(date +"%Y%m%d")
p_3d_date=$(date "--date=${now} -2 day" +%Y%m%d)
p_2d_date=$(date "--date=${now} -1 day" +%Y%m%d)

spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web01/access_log.1.gz s3://ecomm-web-logs/web01/alsoviewed1
spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web02/access_log.1.gz s3://ecomm-web-logs/web02/alsoviewed1
spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web03/access_log.1.gz s3://ecomm-web-logs/web03/alsoviewed1
spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web04/access_log-$now.gz s3://ecomm-web-logs/web04/alsoviewed1
spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web05/access_log-$now.gz s3://ecomm-web-logs/web05/alsoviewed1
spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web06/access_log-$now.gz s3://ecomm-web-logs/web06/alsoviewed1
spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web07/access_log-$now.gz s3://ecomm-web-logs/web07/alsoviewed1
spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web08/access_log-$now.gz s3://ecomm-web-logs/web08/alsoviewed1

if [ $1 = "2d" ]; then
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web01/access_log.2.gz s3://ecomm-web-logs/web01/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web02/access_log.2.gz s3://ecomm-web-logs/web02/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web03/access_log.2.gz s3://ecomm-web-logs/web03/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web04/access_log-$p_2d_date.gz s3://ecomm-web-logs/web04/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web05/access_log-$p_2d_date.gz s3://ecomm-web-logs/web05/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web06/access_log-$p_2d_date.gz s3://ecomm-web-logs/web06/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web07/access_log-$p_2d_date.gz s3://ecomm-web-logs/web07/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web08/access_log-$p_2d_date.gz s3://ecomm-web-logs/web08/alsoviewed2
fi

if [ $1 = "3d" ]; then
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web01/access_log.2.gz s3://ecomm-web-logs/web01/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web01/access_log.3.gz s3://ecomm-web-logs/web01/alsoviewed3
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web02/access_log.2.gz s3://ecomm-web-logs/web02/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web02/access_log.3.gz s3://ecomm-web-logs/web02/alsoviewed3
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web03/access_log.2.gz s3://ecomm-web-logs/web03/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web03/access_log.3.gz s3://ecomm-web-logs/web03/alsoviewed3
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web04/access_log-$p_2d_date.gz s3://ecomm-web-logs/web04/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web04/access_log-$p_3d_date.gz s3://ecomm-web-logs/web04/alsoviewed3
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web05/access_log-$p_2d_date.gz s3://ecomm-web-logs/web05/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web05/access_log-$p_3d_date.gz s3://ecomm-web-logs/web05/alsoviewed3
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web06/access_log-$p_2d_date.gz s3://ecomm-web-logs/web06/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web06/access_log-$p_3d_date.gz s3://ecomm-web-logs/web06/alsoviewed3
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web07/access_log-$p_2d_date.gz s3://ecomm-web-logs/web07/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web07/access_log-$p_3d_date.gz s3://ecomm-web-logs/web07/alsoviewed3
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web08/access_log-$p_2d_date.gz s3://ecomm-web-logs/web08/alsoviewed2
  spark-submit --class "LogAnalyzer" --master yarn ./urbn-rec-bd-1.2.jar s3://ecomm-web-logs/web08/access_log-$p_3d_date.gz s3://ecomm-web-logs/web08/alsoviewed3
fi




