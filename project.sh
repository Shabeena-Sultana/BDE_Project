#!/bin/bash

#Reading commandline arguments
MYSQL_USER=$1
MYSQL_PASSWORD=$2
MASTER_NODE_PRIVATE_HOST=$3

# MYSQL commands
mysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}"
CREATE USER 'sqoop'@'%' IDENTIFIED BY 'changeme1';
GRANT ALL PRIVILEGES ON *.* TO 'sqoop';
source /home/hadoop/Yellow_Taxi.sql
select * from taxi_data.yellow_taxi limit 2;
exit;

#Sqoop Commands
sqoop list-databases --connect jdbc:mysql://"${MASTER_NODE_PRIVATE_HOST}" \
    --username sqoop --password-alias mysql.password # Sqoop command to list databases using keystore


sqoop --options-file /home/hadoop/sqoop-import-options.txt \
    --connect jdbc:mysql://"${MASTER_NODE_PRIVATE_HOST}"/taxi_data \
    --table yellow_taxi # Sqoop command to specify the sqoop import options file and execute import



#Hive commands
hive -f /home/hadoop/create_table.hql

# Spark Submit
spark-submit --master yarn Project.py
spark-submit --master yarn Project_analytics.py