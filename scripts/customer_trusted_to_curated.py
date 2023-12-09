import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1701772602373 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedZone_node1701772602373",
    )
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1701772064606 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1701772064606",
)

# Script generated for node Join
SqlQuery127 = """
select *
from customer_trusted
join accelerometer_trusted
on email = user;
"""
Join_node1702144895063 = sparkSqlQuery(
    glueContext,
    query=SqlQuery127,
    mapping={
        "customer_trusted": CustomerTrustedZone_node1701772064606,
        "accelerometer_trusted": AccelerometerTrustedZone_node1701772602373,
    },
    transformation_ctx="Join_node1702144895063",
)

# Script generated for node Drop Fields
DropFields_node1701773488634 = DropFields.apply(
    frame=Join_node1702144895063,
    paths=["user", "z", "timestamp", "x", "y"],
    transformation_ctx="DropFields_node1701773488634",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1702145249569 = DynamicFrame.fromDF(
    DropFields_node1701773488634.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1702145249569",
)

# Script generated for node Customer Curated
CustomerCurated_node1701771766151 = glueContext.getSink(
    path="s3://udacity-aniq/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1701771766151",
)
CustomerCurated_node1701771766151.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1701771766151.setFormat("json")
CustomerCurated_node1701771766151.writeFrame(DropDuplicates_node1702145249569)
job.commit()
