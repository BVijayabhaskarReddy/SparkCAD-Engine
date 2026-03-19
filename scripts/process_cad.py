"""
Silver Layer: CAD Geometry Extraction (Spark Job)
Reads raw .step files from MinIO Bronze bucket, extracts volume
and surface area using pythonocc, and writes results to Silver bucket.

Usage:
    spark-submit process_cad.py
"""
import boto3
import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO configuration
MINIO_URL = os.environ.get('MINIO_URL', 'http://minio:9000')
MINIO_ACCESS = os.environ.get('MINIO_ACCESS', 'admin')
MINIO_SECRET = os.environ.get('MINIO_SECRET', 'password')
MINIO_REGION = os.environ.get('MINIO_REGION', 'us-east-1')


def process_single_file(file_key):
    """
    Download a .step file from Bronze, extract volume and surface area,
    and return a tuple of (filename, volume, surface_area, status).
    
    This function runs on Spark Workers, so all imports and configs
    must be defined inside it.
    """
    import os
    import boto3
    from OCC.Core.STEPControl import STEPControl_Reader
    from OCC.Core.GProp import GProp_GProps
    from OCC.Core.BRepGProp import (
        brepgprop_VolumeProperties,
        brepgprop_SurfaceProperties,
    )
    from OCC.Core.IFSelect import IFSelect_RetDone

    # Configs must be inside the function for Spark Workers
    MINIO_URL = os.environ.get('MINIO_URL', 'http://minio:9000')
    MINIO_ACCESS = os.environ.get('MINIO_ACCESS', 'admin')
    MINIO_SECRET = os.environ.get('MINIO_SECRET', 'password')

    local_path = os.path.join("/tmp", os.path.basename(file_key))

    try:
        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_URL,
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )

        # 1. Download from Bronze
        s3.download_file('bronze', file_key, local_path)

        # 2. Validate file
        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            return (file_key, 0.0, 0.0, "Error: File empty or download failed")

        # 3. Read STEP file
        reader = STEPControl_Reader()
        status = reader.ReadFile(local_path)

        if status == IFSelect_RetDone:
            reader.TransferRoots()
            shape = reader.OneShape()

            # Extract volume
            vol_props = GProp_GProps()
            brepgprop_VolumeProperties(shape, vol_props)
            volume = vol_props.Mass()

            # Extract surface area
            surf_props = GProp_GProps()
            brepgprop_SurfaceProperties(shape, surf_props)
            surface_area = surf_props.Mass()

            return (file_key, float(volume), float(surface_area), "Success")
        else:
            return (file_key, 0.0, 0.0, f"CAD Read Error: status={status}")

    except Exception as e:
        return (file_key, 0.0, 0.0, f"Python Error: {str(e)}")
    finally:
        # Cleanup to prevent Worker disk fill-up
        if os.path.exists(local_path):
            os.remove(local_path)


def main():
    """Main entry point: list Bronze files, distribute to Spark, write to Silver."""
    spark = SparkSession.builder \
        .appName("CAD-Silver-Processing") \
        .getOrCreate()

    # Master needs its own S3 client to list files
    s3_master = boto3.client(
        's3',
        endpoint_url=MINIO_URL,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name=MINIO_REGION,
    )

    try:
        objects = s3_master.list_objects_v2(Bucket='bronze')
        file_keys = [
            obj['Key']
            for obj in objects.get('Contents', [])
            if obj['Key'].endswith('.step')
        ]

        if not file_keys:
            logger.warning("No .step files found in Bronze bucket!")
            return

        logger.info(f"Found {len(file_keys)} STEP file(s) in Bronze. Processing...")

        # Distribute the list of keys to Workers
        rdd = spark.sparkContext.parallelize(file_keys, numSlices=len(file_keys))
        results = rdd.map(process_single_file).collect()

        schema = StructType([
            StructField("filename", StringType(), True),
            StructField("volume", FloatType(), True),
            StructField("surface_area", FloatType(), True),
            StructField("status", StringType(), True),
        ])

        df = spark.createDataFrame(results, schema)
        df.show(truncate=False)

        # Upload CSV to Silver bucket
        logger.info("Uploading CSV to Silver bucket...")
        csv_data = df.toPandas().to_csv(index=False)
        s3_master.put_object(
            Bucket='silver',
            Key='cad_metadata.csv',
            Body=csv_data,
        )
        
        # Save as Parquet locally for dbt to seamlessly read
        df.write.mode("overwrite").parquet("/opt/spark/data/silver_parquet")
        
        logger.info("Silver layer processing complete!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()