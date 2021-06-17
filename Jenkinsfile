def DBTOKEN = credentials('dbkey')
def DBURL  = "https://adb-5240650903622184.4.azuredatabricks.net/"

stage('Setup') {
      withCredentials([string(credentialsId: DBTOKEN, variable: 'TOKEN')]) {
        sh """#!/bin/bash
            echo "**************HELLO***************"

            # Configure Databricks CLI for deployment
            echo "${DBURL}
            $TOKEN" | databricks configure --token
           """
      }
  }
