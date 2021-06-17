def DBTOKEN = "<databricks-token>"
def DBURL  = "https://<databricks-instance>"

stage('Setup') {
      withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
        sh """#!/bin/bash

            # Configure Databricks CLI for deployment
            echo "${DBURL}
            $TOKEN" | databricks configure --token
           """
      }
  }
