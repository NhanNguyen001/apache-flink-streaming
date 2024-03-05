ASSUME_ROLE=arn:aws:iam::889924997113:role/non-prod-data-serverless-assume-role

prepare:
	cd proto/ &&\
	protoc --descriptor_set_out messages.desc ./*.proto &&\
	cp messages.desc ../src/main/resources/ &&\
	mkdir -p ../src/test/resources/ &&\
	cp messages.desc ../src/test/resources/ &&\
	rm messages.desc

build:prepare
	mvn clean package

deploy: build
	export JSON=$(aws sts assume-role --role-arn ${ASSUME_ROLE} --role-session-name "Operations") &&\
	export AWS_ACCESS_KEY_ID=$(echo ${JSON} | jq -r ".Credentials[\"AccessKeyId\"]") &&\
	export AWS_SECRET_ACCESS_KEY=$(echo ${JSON} | jq -r ".Credentials[\"SecretAccessKey\"]") &&\
	export AWS_SESSION_TOKEN=$(echo ${JSON} | jq -r ".Credentials[\"SessionToken\"]") &&\
	aws s3 cp target/jobs-0.1.0.jar s3://data-lambda-non-prod-dev-serverlessdeploymentbuck-vwh46n0a0i0v/datalake-flink-service-0.1.0.jar