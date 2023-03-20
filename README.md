## This script loads real estate data from S3 to Snowflake.

#### To run locally

Set all these environment variables in a .env file

```
SNOWFLAKE_USERNAME
SNOWFLAKE_PASSWORD
SNOWFLAKE_ACCOUNT
WAREHOUSE
DATABASE
SCHEMA
BUCKET_NAME
ACCESS_KEY
SECRET_ACCESS_KEY
```

Build and run the docker container

`docker compose up --build`

*If you encounter a 403 during this step, try restarting docker hub and runnning this:

`aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws`

Post an event to trigger the script

`curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"extractDate": "2023-03-17"}'`
