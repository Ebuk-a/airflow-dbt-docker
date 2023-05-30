#!/bin/bash

echo "################################################################"

ls /var/lib/jenkins/workspace/airflow-ci-jenkins &> /dev/null

if [ $? -ne 0 ]
then
        echo "No existing build file...continuing"
else
        echo "Backing up the old build file as airflow-ci-jenkins_$BUILD_NUMBER_$BUILD_TIMESTAMP"
        echo $BUILD_NUMBER_$BUILD_TIMESTAMP
        rm -rf airflow-ci-jenkins@tmp
        mv /var/lib/jenkins/workspace/airflow-ci-jenkins /var/lib/jenkins/workspace/airflow-ci-jenkins_$BUILD_NUMBER_$BUILD_TIMESTAMP
fi