#!/bin/bash

sleep 60
WEBSERVER=`docker ps | grep webserver | awk '{print $1}'`
docker exec -u root -i -t $WEBSERVER chmod 777 /opt/airflow