import os
import boto3
import json
import snowflake.connector
import pandas as pd
from io import StringIO
from datetime import date
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
load_dotenv()


def get_df_from_s3(client, bucket_name, category, extract_date):
	objects_metadata = client.list_objects(
		Bucket=bucket_name, Prefix=f'real_estate/cost_of_living/{extract_date}'
	)
	keys = [obj['Key'] for obj in objects_metadata['Contents'] if category in obj['Key']]
	objects = [client.get_object(Bucket=bucket_name, Key=key) for key in keys]
	df = pd.concat([pd.read_csv(StringIO(obj['Body'].read().decode('utf-8'))) for obj in objects])
	return df

def get_household_df(conn):
	household_df = pd.read_sql('SELECT * FROM HOUSEHOLD', conn)
	return household_df

def transform_living_wage_df(living_wage_df):
	living_wage_df = living_wage_df.rename(columns={
		'wage_level': 'WAGE_LEVEL', 'county': 'COUNTY', 'num_children': 'CHILDREN',
		'num_adults': 'ADULTS', 'num_working': 'WORKING_ADULTS', 'usd_amount': 'HOURLY_WAGE'
	})
	living_wage_df.CHILDREN = living_wage_df.CHILDREN.astype(int)
	living_wage_df['AS_OF_DATE'] = date.today()
	return living_wage_df

def transform_expense_df(expense_df):
	expense_df.usd_amount = expense_df.usd_amount.apply(lambda x: x.replace(',', '')).astype(float)
	expense_df.num_children = expense_df.num_children.astype(int)
	expense_df['as_of_date'] = date.today()
	expense_df = expense_df.rename(columns={
		'num_children': 'CHILDREN', 'num_adults': 'ADULTS', 'num_working': 'WORKING_ADULTS',
		'expense_category': 'CATEGORY', 'usd_amount': 'AMOUNT', 'as_of_date': 'AS_OF_DATE', 
		'county': 'COUNTY'
	})
	return expense_df

def transform_annual_salary_df(annual_salary_df):
    annual_salary_df = annual_salary_df.rename(columns={
        'occupational_area': 'OCCUPATION', 'typical_annual_salary': 'SALARY', 'county': 'COUNTY'
    })
    annual_salary_df['AS_OF_DATE'] = date.today()
    return annual_salary_df


def main(event, context):	
	bucket_name = os.getenv('BUCKET_NAME')
	client = boto3.client(
		's3', 
		endpoint_url='https://s3.amazonaws.com',
		aws_access_key_id=os.getenv('ACCESS_KEY'),
		aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY')
	)
	conn = snowflake.connector.connect(
		user=os.getenv('SNOWFLAKE_USERNAME'),
		password=os.getenv('SNOWFLAKE_PASSWORD'),
		account=os.getenv('SNOWFLAKE_ACCOUNT'),
		warehouse=os.getenv('WAREHOUSE'),
		database=os.getenv('DATABASE'),
		schema=os.getenv('SCHEMA')
	)
	extract_date = event['extractDate']
	# Get data from S3 and Snowflake
	household_df = get_household_df(conn)
	expense_df = get_df_from_s3(client, bucket_name, 'expenses', extract_date)
	living_wage_df = get_df_from_s3(client, bucket_name, 'living_wage', extract_date)
	annual_salary_df = get_df_from_s3(client, bucket_name, 'typical_salaries', extract_date)

	# Transform
	expense_df = transform_expense_df(expense_df)
	living_wage_df = transform_living_wage_df(living_wage_df)
	annual_salary_df = transform_annual_salary_df(annual_salary_df)

	# Join
	expense_df = expense_df.merge(household_df, on=['CHILDREN', 'ADULTS', 'WORKING_ADULTS'])
	living_wage_df = living_wage_df.merge(household_df, on=['CHILDREN', 'ADULTS', 'WORKING_ADULTS'])

	# Filter
	expense_df = expense_df[['CATEGORY', 'AMOUNT', 'COUNTY', 'AS_OF_DATE', 'HOUSEHOLD_ID']]
	living_wage_df = living_wage_df[['WAGE_LEVEL', 'HOURLY_WAGE', 'HOUSEHOLD_ID', 'COUNTY', 'AS_OF_DATE']]

	# Load to Snowflake
	write_pandas(conn, expense_df, 'ANNUAL_EXPENSE')
	write_pandas(conn, living_wage_df, 'WAGE')
	write_pandas(conn, annual_salary_df, 'ANNUAL_SALARY')
	return {'statusCode': 200}
