import os
import boto3
import snowflake.connector
import pandas as pd
from io import StringIO
from datetime import date
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
load_dotenv()


def get_df_from_s3(client, bucket_name, extract_date):
	objects_metadata = client.list_objects(
		Bucket=bucket_name, Prefix=f'real_estate/listings/{extract_date}'
	)
	keys = [obj['Key'] for obj in objects_metadata['Contents']]
	objects = [client.get_object(Bucket=bucket_name, Key=key) for key in keys]
	df = pd.concat(
		[pd.read_json(StringIO(obj['Body'].read().decode('utf-8')), dtype={'zipCode': 'object'}) 
   		for obj in objects]
	)
	return df

def transform_listing_df(listing_df):
	listing_df = listing_df.rename(columns={
		'zipCode': 'ZIP_CODE', 'id': 'LISTING_ID', 'price': 'PRICE', 'bedrooms': 'BEDROOMS',
		'bathrooms': 'BATHROOMS', 'squareFootage': 'SQUARE_FOOTAGE', 'propertyType': 'PROPERTY_TYPE',
		'listedDate': 'LISTED_DATE', 'removedDate': 'REMOVED_DATE', 'yearBuilt': 'YEAR_BUILT',
		'lotSize': 'LOT_SIZE'
	})
	keep_columns = [
		'ZIP_CODE', 'PRICE', 'BATHROOMS', 'BEDROOMS', 'SQUARE_FOOTAGE', 'PROPERTY_TYPE', 
		'LISTED_DATE', 'REMOVED_DATE', 'LOT_SIZE', 'YEAR_BUILT'
	]
	listing_df = listing_df[keep_columns]
	listing_df = listing_df.dropna(subset=filter(lambda x: x != 'REMOVED_DATE', listing_df.columns))
	listing_df.YEAR_BUILT = listing_df.YEAR_BUILT.astype(int)
	listing_df.PRICE = listing_df.PRICE.astype(float)
	listing_df['SNAPSHOT_DATE'] = date.today()
	return listing_df


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

	# Extract from S3
	listing_df = get_df_from_s3(client, bucket_name, extract_date)

	# Get DE location data from Snowflake
	location_df = pd.read_sql("SELECT location_id, zip_code FROM dim_location WHERE state = 'DE'", conn)

	# Transform
	listing_df = transform_listing_df(listing_df)

	# Add location_id to crime_rate data
	listing_merged_df = listing_df.merge(location_df, on='ZIP_CODE', how='inner')

	# Filter
	listing_keep_cols = [
		'LOCATION_ID', 'PRICE', 'BEDROOMS', 'BATHROOMS', 'SQUARE_FOOTAGE',
		'LISTED_DATE', 'REMOVED_DATE', 'YEAR_BUILT', 'LOT_SIZE', 'SNAPSHOT_DATE'
	]
	listing_merged_df = listing_merged_df[listing_keep_cols]
	
	# Load to Snowflake
	write_pandas(conn, listing_merged_df, 'FACT_LISTING')
	
	return {'statusCode': 200}
