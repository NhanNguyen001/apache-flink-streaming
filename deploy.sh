#!/bin/bash
ACCOUNT_ID=889924997113
ASSUME_ROLE=arn:aws:iam::${ACCOUNT_ID}:role/non-prod-data-serverless-assume-role
S3_BUCKET=data-lambda-non-prod-dev-serverlessdeploymentbuck-vwh46n0a0i0v
PARALLELISM=1
PARALLELISM_PER_KPU=1
SUBNET_IDS="\"subnet-05eb12efd63cc427d\",\"subnet-0776327ccad401f08\",\"subnet-0c883c4babd9799b1\""  # Comma-separated list of subnet IDs
SECURITY_GROUP_IDS="sg-01823a58d138aeffc"
SERVICE_ROLE=arn:aws:iam::${ACCOUNT_ID}:role/non-prod-test-kda-role



export JSON=$(aws sts assume-role --role-arn ${ASSUME_ROLE} --role-session-name "Operations")
export AWS_ACCESS_KEY_ID=$(echo ${JSON} | jq -r ".Credentials[\"AccessKeyId\"]")
export AWS_SECRET_ACCESS_KEY=$(echo ${JSON} | jq -r ".Credentials[\"SecretAccessKey\"]")
export AWS_SESSION_TOKEN=$(echo ${JSON} | jq -r ".Credentials[\"SessionToken\"]")
aws s3 cp jobs-0.1.0.jar s3://${S3_BUCKET}/datalake-flink-service-0.1.0.jar

# Define the configuration file
config_file="list_app.conf"

if [ ! -f "$config_file" ]; then 
  echo "Config file '$config_file' does not exist."
  exit 1
fi

source $config_file




echo "DEV_ENV: $DEV_ENV"
IFS=',' read -ra values <<< "$APPLICATION_NAME_LIST"
for value in "${values[@]}"; do
  APPLICATION_NAME="${value}_${DEV_ENV}"
  echo "APPLICATION_NAME: $APPLICATION_NAME"
  BUILD_FILE=datalake-flink-service-0.1.0.jar
  S3_KEY=$BUILD_FILE
  app_info=$(aws kinesisanalyticsv2 describe-application --application-name "$APPLICATION_NAME" 2>/dev/null)
  # check application exist
  if [ $? -eq 0 ]; then
    current_version=$(echo "$app_info" | jq -r '.ApplicationDetail.ApplicationVersionId')
    CURRENT_VERSION="$current_version"
    # aws kinesisanalyticsv2 update-application --application-name $APPLICATION_NAME --current-application-version-id $CURRENT_VERSION 

    STATUS=$(aws kinesisanalyticsv2 describe-application --application-name $APPLICATION_NAME --query 'ApplicationDetail.ApplicationStatus' --output text)
    aws kinesisanalyticsv2 stop-application --application-name $APPLICATION_NAME --force
    while true; do
      STATUS=$(aws kinesisanalyticsv2 describe-application --application-name $APPLICATION_NAME --query 'ApplicationDetail.ApplicationStatus' --output text)

      if [ "$STATUS" != "READY" ]; then
          echo "App is not READY: $STATUS"
          # aws kinesisanalyticsv2 stop-application --application-name $APPLICATION_NAME 
          sleep 10  # Adjust the sleep duration as needed
      else
          echo "App is READY. Updating loop."
          aws kinesisanalyticsv2 update-application --application-name $APPLICATION_NAME --current-application-version-id $CURRENT_VERSION --application-configuration-update "{\"ApplicationCodeConfigurationUpdate\": {\"CodeContentUpdate\": {\"S3ContentLocationUpdate\": {\"BucketARNUpdate\": \"arn:aws:s3:::${S3_BUCKET}\",\"FileKeyUpdate\": \"${S3_KEY}\"}}}}"  || error_exit "Reload Application failed. Please check"    
          break
      fi
    done
    echo "Status: $STATUS"
    aws kinesisanalyticsv2 start-application --application-name "$APPLICATION_NAME" --run-configuration "{\"ApplicationRestoreConfiguration\":{\"ApplicationRestoreType\":\"SKIP_RESTORE_FROM_SNAPSHOT\"}}"

  else
    # create application
    # aws kinesisanalyticsv2 stop-application --force true
    echo "The application '$APPLICATION_NAME' does not exist."
    APPLICATION_CONFIGURATION="{\"FlinkApplicationConfiguration\":{\"MonitoringConfiguration\": {\"ConfigurationType\": \"CUSTOM\",\"MetricsLevel\": \"APPLICATION\",\"LogLevel\": \"INFO\"},\"ParallelismConfiguration\":{\"ConfigurationType\":\"CUSTOM\",\"Parallelism\":${PARALLELISM},\"ParallelismPerKPU\":${PARALLELISM_PER_KPU},\"AutoScalingEnabled\":true}},\"EnvironmentProperties\":{\"PropertyGroups\":[{\"PropertyGroupId\":\"Config\",\"PropertyMap\":{\"table\":\"${APPLICATION_NAME}\",\"developingEnv\":\"${DEV_ENV}\"}}]},\"VpcConfigurations\":[{\"SubnetIds\":[${SUBNET_IDS}],\"SecurityGroupIds\":[\"${SECURITY_GROUP_IDS}\"]}],\"ApplicationCodeConfiguration\": {\"CodeContent\": {\"S3ContentLocation\": {\"BucketARN\": \"arn:aws:s3:::${S3_BUCKET}\",\"FileKey\": \"${S3_KEY}\"}},\"CodeContentType\": \"ZIPFILE\"}}"
    aws logs create-log-group --log-group-name /finx/kinesis-analytics/${APPLICATION_NAME}
    aws logs create-log-stream --log-group-name /finx/kinesis-analytics/${APPLICATION_NAME} --log-stream-name finx-kinesis-analytics-log-stream
    aws kinesisanalyticsv2 create-application \
    --region  ap-southeast-1 \
    --application-name "$APPLICATION_NAME" \
    --runtime-environment FLINK-1_15 \
    --service-execution-role "$SERVICE_ROLE" \
    --application-configuration "$APPLICATION_CONFIGURATION" \
    --application-mode STREAMING \
    --cloud-watch-logging-options "[{\"LogStreamARN\":\"arn:aws:logs:ap-southeast-1:${ACCOUNT_ID}:log-group:/finx/kinesis-analytics/${APPLICATION_NAME}:log-stream:finx-kinesis-analytics-log-stream\"}]"          
    aws kinesisanalyticsv2 start-application --application-name "$APPLICATION_NAME" 
  fi
done
  






