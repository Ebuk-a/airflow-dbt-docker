def COLOR_MAP = [
    'SUCCESS': 'good', 
    'FAILURE': 'danger',
]

pipeline {
    agent any
    tools {
        maven 'MAVEN3'
        jdk 'OracleJDK8'
    }

    environment {
        registryCredential = 'ecr:eu-west-2:awscreds'
        pipelineRegistry = "117542381924.dkr.ecr.eu-west-2.amazonaws.com/airflowimg"
        airflowRegistry = "https://117542381924.dkr.ecr.eu-west-2.amazonaws.com"
        cluster = "airflow"
        service = "airflowsvc"
    }

    stages {
        stage('Fetch code') {
            steps{
                git branch: 'main', url: 'git@github.com:Ebuk-a/airflow-dbt-docker.git'
            }
        }
        stage('Start container') {
          steps {
            sh 'docker compose up -d --force-recreate --build --no-color'
          }
        }
        stage('Update Webserver permission') {
          steps {
            sh 'chmod 777 webserver_perm.sh'
            sh 'whoami'
            sh './webserver_perm.sh'
          }
          post {
            always{
              sh 'docker compose ps'  
            }  
          }
        }
        stage('Run health tests against the container') {
          steps {
            sh 'curl http://localhost:8081/health'
          }
        }
    }
    post {
        always {
            echo 'Slack Notifications.'
            slackSend channel: '#jenkinscicd',
                color: COLOR_MAP[currentBuild.currentResult],
                message: "*${currentBuild.currentResult}:* Job ${env.JOB_NAME} build ${env.BUILD_NUMBER} \n More info at: ${env.BUILD_URL}"
        }
    }
}