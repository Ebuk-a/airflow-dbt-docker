def COLOR_MAP = [
    'SUCCESS': 'good', 
    'FAILURE': 'danger',
]

pipeline {
    agent any
    environment {
        DOCKER_CREDENTIAL = credentials('dockercred')
        REGISTRY_CREDENTIAL = 'awscreds'
        AWS_REGISTRY = "117542381924.dkr.ecr.eu-west-2.amazonaws.com/airflowimg"
        AWSAIRFLOW_REGISTRY = "https://117542381924.dkr.ecr.eu-west-2.amazonaws.com"
        CLUSTER = "airflow"
        SERVICE = "airflowsvc"
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
        stage('Push Containers to DockerRegistry & Delete Old Images') {
          steps {
            sh 'chmod 777 dockerpush.sh'
            sh './dockerpush.sh'
          }
        }
    }
    post {
        always {
            echo 'Slack Notifications.'
            slackSend channel: '#jenkinscicd',
                color: COLOR_MAP[currentBuild.currentResult],
                message: "*${currentBuild.currentResult}:* Job ${env.JOB_NAME} build ${env.BUILD_NUMBER} \n More info at: ${env.BUILD_URL}"
            cleanWs(cleanWhenNotBuilt: false,
                    deleteDirs: true,
                    disableDeferredWipeout: true,
                    notFailBuild: true,
                    patterns: [[pattern: '.gitignore', type: 'INCLUDE'],
                               [pattern: '.propsfile', type: 'EXCLUDE']])
        }
    }
}