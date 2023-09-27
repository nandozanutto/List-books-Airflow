import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_regex_extract

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon_books
Amazon_books_node1695656180306 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://how-finalproject-raw/amazon-dataset/"],
        "recurse": True,
    },
    transformation_ctx="Amazon_books_node1695656180306",
)

# Script generated for node list_books
list_books_node1695656208578 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://how-finalproject-raw/list-books/"],
        "recurse": True,
    },
    transformation_ctx="list_books_node1695656208578",
)

# Script generated for node dropColumnsAmazon
dropColumnsAmazon_node1695656291583 = ApplyMapping.apply(
    frame=Amazon_books_node1695656180306,
    mappings=[
        ("ISBN10", "string", "ISBN10_left", "string"),
        ("final_price", "string", "final_price", "string"),
        ("rating", "string", "rating", "string"),
        ("reviews_count", "string", "reviews_count", "string"),
    ],
    transformation_ctx="dropColumnsAmazon_node1695656291583",
)

# Script generated for node Join
Join_node1695656708271 = Join.apply(
    frame1=dropColumnsAmazon_node1695656291583,
    frame2=list_books_node1695656208578,
    keys1=["ISBN10_left"],
    keys2=["ISBN10"],
    transformation_ctx="Join_node1695656708271",
)

# Script generated for node Change Schema
ChangeSchema_node1695657013968 = ApplyMapping.apply(
    frame=Join_node1695656708271,
    mappings=[
        ("ISBN10_left", "string", "ISBN10_left", "string"),
        ("final_price", "string", "final_price", "float"),
        ("rating", "string", "rating", "string"),
        ("reviews_count", "string", "reviews_count", "bigint"),
        ("list_id", "string", "list_id", "string"),
        ("list_name", "string", "list_name", "string"),
        ("list_updated", "string", "list_updated", "string"),
        ("Title", "string", "Title", "string"),
        ("Author", "string", "Author", "string"),
        ("Description", "string", "Description", "string"),
        ("URL", "string", "URL", "string"),
        ("ISBN10", "string", "ISBN10", "string"),
        ("published_date", "string", "published_date", "date"),
    ],
    transformation_ctx="ChangeSchema_node1695657013968",
)

# Script generated for node Regex Extractor
RegexExtractor_node1695657626210 = ChangeSchema_node1695657013968.gs_regex_extract(
    colName="rating", regex="^\S*", newCols="rating2"
)

# Script generated for node Amazon S3
AmazonS3_node1695658263921 = glueContext.getSink(
    path="s3://how-finalproject-cleaned",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["published_date"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1695658263921",
)
AmazonS3_node1695658263921.setCatalogInfo(
    catalogDatabase="teste", catalogTableName="list_books_cleaned"
)
AmazonS3_node1695658263921.setFormat("glueparquet")
AmazonS3_node1695658263921.writeFrame(RegexExtractor_node1695657626210)
job.commit()
