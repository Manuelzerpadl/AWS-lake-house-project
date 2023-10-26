import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer landing
accelerometerlanding_node1698352379346 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-lake-house",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1698352379346",
)

# Script generated for node customer trusted
customertrusted_node1698352435750 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lake-house-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1698352435750",
)

# Script generated for node Join
Join_node1698352312629 = Join.apply(
    frame1=accelerometerlanding_node1698352379346,
    frame2=customertrusted_node1698352435750,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1698352312629",
)

# Script generated for node Drop Fields
DropFields_node1698353358416 = DropFields.apply(
    frame=Join_node1698352312629,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1698353358416",
)

# Script generated for node Amazon S3
AmazonS3_node1698352317426 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1698353358416,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lake-house-project/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1698352317426",
)

job.commit()
