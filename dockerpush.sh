#!/bin/bash

#Filter out the names of the different servers from the response of docker ps
WEBSERVER_NAME=`docker ps | grep webserver | awk '{print $2}'`
SCHEDULER_NAME=`docker ps | grep scheduler | awk '{print $2}'`
TRIGGERER_NAME=`docker ps | grep triggerer | awk '{print $2}'`
WORKER_1_NAME=`docker ps | grep worker-1 | awk '{print $2}'`
WORKER_2_NAME=`docker ps | grep worker-2 | awk '{print $2}'`

#Tag each local image according, ready for push to remote repo.
docker tag $WEBSERVER_NAME:latest dataebuka/airflow-webserver:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully tagged $WEBSERVER_NAME:latest as dataebuka/airflow-webserver:v$BUILD_NUMBER_$BUILD_TIMESTAMP"
docker tag $SCHEDULER_NAME:latest dataebuka/airflow-scheduler:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully tagged $SCHEDULER_NAME:latest as dataebuka/airflow-scheduler:v$BUILD_NUMBER_$BUILD_TIMESTAMP"
docker tag $TRIGGERER_NAME:latest dataebuka/airflow-triggerer:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully tagged $TRIGGERER_NAME:latest as dataebuka/airflow-triggerer:v$BUILD_NUMBER_$BUILD_TIMESTAMP"
docker tag $WORKER_1_NAME:latest dataebuka/airflow-worker-1:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully tagged $WORKER_1_NAME:latest as dataebuka/worker-1:v$BUILD_NUMBER_$BUILD_TIMESTAMP"
docker tag $WORKER_2_NAME:latest dataebuka/airflow-worker-2:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully tagged $WORKER_2_NAME:latest as dataebuka/airflow-worker-2:v$BUILD_NUMBER_$BUILD_TIMESTAMP"

#Login to Dockerhub
docker login -u $DOCKER_CREDENTIAL_USR -p $DOCKER_CREDENTIAL_PSW

#Push tagged images to remote repository
docker push dataebuka/airflow-webserver:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully pushed dataebuka/airflow-webserver:v$BUILD_NUMBER_$BUILD_TIMESTAMP"
docker push dataebuka/airflow-scheduler:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully pusheddataebuka/airflow-scheduler:v$BUILD_NUMBER_$BUILD_TIMESTAMP"
docker push dataebuka/airflow-triggerer:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully pushed dataebuka/airflow-triggerer:v$BUILD_NUMBER_$BUILD_TIMESTAMP"
docker push dataebuka/airflow-worker-1:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully pushed dataebuka/airflow-worker-1:v$BUILD_NUMBER_$BUILD_TIMESTAMP"
docker push dataebuka/airflow-worker-2:v$BUILD_NUMBER_$BUILD_TIMESTAMP
echo "successfully pushed dataebuka/airflow-worker-2:v$BUILD_NUMBER_$BUILD_TIMESTAMP"

#Remove all unused(old) local images, not just dangling ones
#NOTE: here we assume this server is only running our airflow containers, we are cleaning all pushed(old) images i.e deleting "not latest".
docker image prune -af