import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="teste",
    table_name="list_books_cleaned",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1695676920752 = DynamicFrame.fromDF(
    S3bucket_node1.toDF().dropDuplicates(["isbn10"]),
    glueContext,
    "DropDuplicates_node1695676920752",
)

# Script generated for node Change Schema
ChangeSchema_node1695677878421 = ApplyMapping.apply(
    frame=DropDuplicates_node1695676920752,
    mappings=[
        ("final_price", "float", "final_price", "float"),
        ("reviews_count", "long", "reviews_count", "long"),
        ("title", "string", "title", "string"),
        ("author", "string", "author", "string"),
        ("description", "string", "description", "string"),
        ("url", "string", "url", "string"),
        ("isbn10", "string", "isbn10", "string"),
        ("rating2", "string", "rating2", "float"),
        ("published_date", "date", "published_date", "date"),
    ],
    transformation_ctx="ChangeSchema_node1695677878421",
)

# Script generated for node Filter
Filter_node1695678068829 = Filter.apply(
    frame=ChangeSchema_node1695677878421,
    f=lambda row: (row["reviews_count"] > 15000 and row["final_price"] < 10),
    transformation_ctx="Filter_node1695678068829",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.getSink(
    path="s3://how-finalproject-curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["published_date"],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node2",
)
S3bucket_node2.setCatalogInfo(
    catalogDatabase="teste", catalogTableName="list_books_curated"
)
S3bucket_node2.setFormat("glueparquet")
S3bucket_node2.writeFrame(Filter_node1695678068829)
job.commit()
