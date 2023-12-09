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

# Script generated for node Customer Landing
CustomerLanding_node1701440463944 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-aniq/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1701440463944",
)

# Script generated for node Share With Research
SqlQuery147 = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate != 0;
"""
ShareWithResearch_node1702150938454 = sparkSqlQuery(
    glueContext,
    query=SqlQuery147,
    mapping={"customer_landing": CustomerLanding_node1701440463944},
    transformation_ctx="ShareWithResearch_node1702150938454",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702137521518 = glueContext.getSink(
    path="s3://udacity-aniq/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1702137521518",
)
CustomerTrusted_node1702137521518.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1702137521518.setFormat("json")
CustomerTrusted_node1702137521518.writeFrame(ShareWithResearch_node1702150938454)
job.commit()
