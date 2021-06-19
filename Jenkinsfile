def DBTOKEN = "databricks_ws_access_token"
def DBURL  = "workspace_url"
def CURRENTRELEASE = "main"
def GITREPOREMOTE   = "https://github.com/santhoshraj2960/AzureDatabricksLearn"

pipeline {
      agent any

      stages {
          stage('Setup') {
              withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN'), 
              string(credentialsId: DBURL, variable: 'WORKSPACEURL')]) {
                sh """#!/bin/bash
                    python3 -m venv tutorial-env
                    source tutorial-env/bin/activate
                    pip install databricks-cli
                    # Configure Databricks CLI for deployment
                    echo "$DBURL
                    $TOKEN" | databricks configure --token
                    databricks jobs list
                   """
              }
          }
          stage('Checkout') { // for display purposes
              echo "Pulling ${CURRENTRELEASE} Branch from Github"
              git branch: CURRENTRELEASE, url: GITREPOREMOTE
              sh """#!/bin/bash
                  ls -l
                  source tutorial-env/bin/activate
                  ls -l
                 """
          }
          stage('Deploy') { // for display purposes
              sh """#!/bin/bash
                  source tutorial-env/bin/activate
                  databricks workspace import_dir --overwrite notebooks /Shared/jenkins_deploy
                 """
          }
      }
}
