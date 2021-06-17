def DBTOKEN = "<databricks-token>"
def DBURL  = "https://adb-5240650903622184.4.azuredatabricks.net/"

stage('Setup') {
      withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
        sh """#!/bin/bash

            # Configure Databricks CLI for deployment
            echo "${DBURL}
            $TOKEN" | databricks configure --token
           """
      }
  }
