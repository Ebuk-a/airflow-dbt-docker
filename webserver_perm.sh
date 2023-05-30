#!/bin/bash

sleep 60
WEBSERVER_ID=`docker ps | grep webserver | awk '{print $1}'`
docker exec -u root $WEBSERVER_ID chmod 777 /opt/airflow
sleep 80