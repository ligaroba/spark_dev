#!/bin/bash
    echo "RUNNING JOB Qnique Script"


python {BASE_PATH_TO_THE_SCRIPT}/scripts/python/qnique/qnique_add_column.py

/opt/mapr/spark/spark-2.4.0/bin/spark-submit --jars {BASE_PATH_TO_THE_SCRIPT}/libs/spark-cassandra-connector_2.11-2.4.1.jar,{BASE_PATH_TO_THE_SCRIPT}/libs/ojdbc6.jar,{BASE_PATH_TO_THE_SCRIPT}/libs/jsr166e-1.1.0.jar {BASE_PATH_TO_THE_SCRIPT}/libs/mssql-jdbc-6.1.0.jre8.jar  --master yarn  --deploy-mode cluster  --conf spark.driver.memory=15g --conf spark.executor.memory=10g --conf spark.memory.fraction=0.8 --conf spark.memory.storageFraction=0.35 --conf spark.driver.maxResultSize=5G --conf spark.network.timeout=800 --queue heavy {BASE_PATH_TO_THE_SCRIPT}/scripts/python/qnique/qnique_pivot_appraisal.py




