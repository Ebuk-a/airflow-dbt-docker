#!/bin/bash

echo "################################################################"

ls /var/lib/jenkins/workspace/airflow-ci-jenkins &> /dev/null

if [ $? -eq 0 ]
then
	echo "Backing up the old build file"
	mv airflow-ci-jenkins airflow-ci-jenkins_${env.BUILD_NUMBER}_${env.BUILD_TIMESTAMP}
	rm -rf airflow-ci-jenkins@tmp
else
	echo "No existing build file...continuing"
fi
echo "################################################################"