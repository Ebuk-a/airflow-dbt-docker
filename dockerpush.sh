#!/bin/bash

#Filter out the names of the different servers from the response of docker ps
WEBSERVER_NAME=`docker ps | grep webserver | awk '{print $1}'`
SCHEDULER_NAME=`docker ps | grep scheduler | awk '{print $1}'`
TRIGGERER_NAME=`docker ps | grep triggerer | awk '{print $1}'`
WORKER_1_NAME=`docker ps | grep worker-1 | awk '{print $1}'`
WORKER_2_NAME=`docker ps | grep worker-2 | awk '{print $1}'`

#Tag each local image according, ready for push to remote repo.
docker tag $WEBSERVER_NAME:latest dataebuka/airflow-webserver:v$BUILD_NUMBER_$BUILD_TIMESTAMP
docker tag $SCHEDULER_NAME:latest dataebuka/airflow-scheduler:v$BUILD_NUMBER_$BUILD_TIMESTAMP
docker tag $TRIGGERER_NAME:latest dataebuka/airflow-triggerer:v$BUILD_NUMBER_$BUILD_TIMESTAMP
docker tag $WORKER_1_NAME:latest dataebuka/airflow-worker-1:v$BUILD_NUMBER_$BUILD_TIMESTAMP
docker tag $WORKER_2_NAME:latest dataebuka/airflow-worker-2:v$BUILD_NUMBER_$BUILD_TIMESTAMP

#Push tagged images to remote repository
docker push dataebuka/airflow-webserver:v$BUILD_NUMBER_$BUILD_TIMESTAMP
docker push dataebuka/airflow-scheduler:v$BUILD_NUMBER_$BUILD_TIMESTAMP
docker push dataebuka/airflow-triggerer:v$BUILD_NUMBER_$BUILD_TIMESTAMP
docker push dataebuka/airflow-worker-1:v$BUILD_NUMBER_$BUILD_TIMESTAMP
docker push dataebuka/airflow-worker-2:v$BUILD_NUMBER_$BUILD_TIMESTAMP


