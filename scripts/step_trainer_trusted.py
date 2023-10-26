import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1698357662717 = glueContext.create_dynamic_frame.from_catalog(
    database="udacity-lake-house",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1698357662717",
)

# Script generated for node step trainer landing
steptrainerlanding_node1698357669384 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lake-house-project/step-trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainerlanding_node1698357669384",
)

# Script generated for node SQL Query
SqlQuery378 = """
SELECT *
FROM customer_curated
JOIN step_trainer ON customer_curated.serialnumber = step_trainer.serialnumber;
"""
SQLQuery_node1698357806848 = sparkSqlQuery(
    glueContext,
    query=SqlQuery378,
    mapping={
        "customer_curated": AWSGlueDataCatalog_node1698357662717,
        "step_trainer": steptrainerlanding_node1698357669384,
    },
    transformation_ctx="SQLQuery_node1698357806848",
)

# Script generated for node drop duplicates
SqlQuery379 = """
select distinct serialnumber, sensorreadingtime, distancefromobject from myDatasource

"""
dropduplicates_node1698359949703 = sparkSqlQuery(
    glueContext,
    query=SqlQuery379,
    mapping={"myDataSource": SQLQuery_node1698357806848},
    transformation_ctx="dropduplicates_node1698359949703",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1698357995328 = glueContext.write_dynamic_frame.from_options(
    frame=dropduplicates_node1698359949703,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lake-house-project/step-trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="steptrainertrusted_node1698357995328",
)

job.commit()
