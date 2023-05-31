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



#!/bin/bash
echo "###############################################################"
ls /var/lib/jenkins/workspace/airflow-ci-jenkins &> /dev/null
while [ $? -eq 0 ]
do
    echo "Backing up the old build file as airflow-ci-jenkins_v$BUILD_NUMBER_$BUILD_TIMESTAMP"
    rm -rf airflow-ci-jenkins@tmp
    mv /var/lib/jenkins/workspace/airflow-ci-jenkins /var/lib/jenkins/workspace/airflow-ci-jenkins_v$BUILD_NUMBER_$BUILD_TIMESTAMP
    ls /var/lib/jenkins/workspace/airflow-ci-jenkins &> /dev/null
    sleep 2
done
echo "Backed up to airflow-ci-jenkins_v$BUILD_NUMBER_$BUILD_TIMESTAMP"

echo "###############################################################"