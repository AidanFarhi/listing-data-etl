import os
import boto3
import snowflake.connector
import pandas as pd
from io import StringIO
from typing import Tuple
from datetime import date
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()


def get_df_from_s3(client: boto3.client, bucket_name: str, extract_date: str) -> pd.DataFrame:
    """
    Retrieves data from S3 bucket and returns a DataFrame.

    Args:
        client (boto3.client): S3 client.
        bucket_name (str): Name of the S3 bucket.
        extract_date (str): Date for extraction.

    Returns:
        pandas.DataFrame: DataFrame containing the retrieved data.
    """
    objects_metadata = client.list_objects(Bucket=bucket_name, Prefix=f"real_estate/listings/{extract_date}")
    keys = [obj["Key"] for obj in objects_metadata["Contents"]]
    objects = [client.get_object(Bucket=bucket_name, Key=key) for key in keys]
    dfs = []
    for obj in objects:
        try:
            dfs.append(
                pd.read_json(
                    StringIO(obj["Body"].read().decode("utf-8")),
                    dtype={"zipCode": "object"},
                )
            )
        except Exception as e:
            print(e)
    df = pd.concat(dfs)
    return df


def get_min_and_max_date_from_df(df: pd.DataFrame) -> Tuple[date, date]:
    """
    Retrieves the minimum and maximum dates from the specified DataFrame columns.

    Args:
        df (pd.DataFrame): Input DataFrame containing date columns.

    Returns:
        Tuple[date, date]: Minimum and maximum dates as datetime.date objects.
    """
    df = df[["LISTED_DATE", "REMOVED_DATE", "SNAPSHOT_DATE"]]
    min_date = min(filter(lambda x: not pd.isna(x), df.min().values))
    max_date = max(filter(lambda x: not pd.isna(x), df.max().values))
    return min_date, max_date


def merge_and_rename_date_columns(listing_df: pd.DataFrame, dim_date_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges a listing DataFrame with a dimension date DataFrame and renames the merged date columns.

    Args:
        listing_df (pd.DataFrame): The listing DataFrame.
        dim_date_df (pd.DataFrame): The dimension date DataFrame.

    Returns:
        pd.DataFrame: The merged DataFrame with renamed date columns.
    """
    df_with_listed_date_id = listing_df.merge(dim_date_df, left_on="LISTED_DATE", right_on="DATE")
    df_with_listed_date_id = df_with_listed_date_id.rename(columns={"DATE_ID": "LISTED_DATE_ID"})
    df_with_removed_date_id = df_with_listed_date_id.merge(
        dim_date_df, how="left", left_on="REMOVED_DATE", right_on="DATE"
    )
    df_with_removed_date_id = df_with_removed_date_id.rename(columns={"DATE_ID": "REMOVED_DATE_ID"})
    df_with_snapshot_id = df_with_removed_date_id.merge(dim_date_df, left_on="SNAPSHOT_DATE", right_on="DATE")
    df_with_all_ids = df_with_snapshot_id.rename(columns={"DATE_ID": "SNAPSHOT_DATE_ID"})
    return df_with_all_ids


def transform_listing_df(listing_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the listing DataFrame and returns the transformed DataFrame.

    Args:
        listing_df (pandas.DataFrame): Input DataFrame.

    Returns:
        pandas.DataFrame: Transformed DataFrame.
    """
    listing_df = listing_df.rename(
        columns={
            "zipCode": "ZIP_CODE",
            "id": "LISTING_ID",
            "price": "PRICE",
            "bedrooms": "BEDROOMS",
            "bathrooms": "BATHROOMS",
            "squareFootage": "SQUARE_FOOTAGE",
            "propertyType": "PROPERTY_TYPE",
            "listedDate": "LISTED_DATE",
            "removedDate": "REMOVED_DATE",
            "yearBuilt": "YEAR_BUILT",
            "lotSize": "LOT_SIZE",
        }
    )
    keep_columns = [
        "ZIP_CODE",
        "PRICE",
        "BATHROOMS",
        "BEDROOMS",
        "SQUARE_FOOTAGE",
        "PROPERTY_TYPE",
        "LISTED_DATE",
        "REMOVED_DATE",
        "LOT_SIZE",
        "YEAR_BUILT",
    ]
    listing_df = listing_df[keep_columns]
    listing_df = listing_df.dropna(subset=filter(lambda x: x != "REMOVED_DATE", listing_df.columns))
    listing_df.YEAR_BUILT = listing_df.YEAR_BUILT.astype(int)
    listing_df.PRICE = listing_df.PRICE.astype(float)
    listing_df.LISTED_DATE = pd.to_datetime(listing_df.LISTED_DATE).dt.date
    listing_df["SNAPSHOT_DATE"] = date.today()
    return listing_df


def main(event: dict, context: dict) -> dict:
    """
    Main function to execute the ETL process.

    Args:
        event: Event data (e.g., AWS Lambda event).
        context: Context object (e.g., AWS Lambda context).

    Returns:
        dict: Response data containing the status code.
    """
    bucket_name = os.getenv("BUCKET_NAME")
    client = boto3.client(
        "s3",
        endpoint_url="https://s3.amazonaws.com",
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
    )
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USERNAME"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("WAREHOUSE"),
        database=os.getenv("DATABASE"),
        schema=os.getenv("SCHEMA"),
    )
    extract_date = event["extractDate"]

    # Extract listing data from S3
    listing_df = get_df_from_s3(client, bucket_name, extract_date)

    # Get reference data from Snowflake
    location_df = pd.read_sql("SELECT location_id, zip_code FROM dim_location WHERE state = 'DE'", conn)

    # Transform
    listing_df = transform_listing_df(listing_df)

    # Get min and max dates from transformed data
    min_date, max_date = get_min_and_max_date_from_df(listing_df)

    dim_date_df = pd.read_sql(
        f"SELECT date_id, date FROM dim_date WHERE date >= '{min_date}' AND date <= '{max_date}'",
        conn,
    )

    # Add location_id
    listing_merged_df = listing_df.merge(location_df, on="ZIP_CODE", how="inner")

    # Add date ids
    final_df = merge_and_rename_date_columns(listing_merged_df, dim_date_df)

    # Filter
    keep_cols = [
        "LOCATION_ID",
        "PRICE",
        "BEDROOMS",
        "BATHROOMS",
        "SQUARE_FOOTAGE",
        "LISTED_DATE_ID",
        "REMOVED_DATE_ID",
        "YEAR_BUILT",
        "LOT_SIZE",
        "SNAPSHOT_DATE_ID",
    ]
    final_df = final_df[keep_cols]

    # Load to Snowflake
    write_pandas(conn, final_df, "FACT_LISTING")

    return {"statusCode": 200}


if __name__ == "__main__":
    from sys import argv

    main({"extractDate": argv[1]}, None)
