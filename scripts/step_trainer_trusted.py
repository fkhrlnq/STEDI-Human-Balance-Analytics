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

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1702138081168 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_landing",
        transformation_ctx="StepTrainerLandingZone_node1702138081168",
    )
)

# Script generated for node Customer Curated
CustomerCurated_node1702138081812 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1702138081812",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1702138191250 = ApplyMapping.apply(
    frame=CustomerCurated_node1702138081812,
    mappings=[
        ("registrationdate", "long", "cc_registrationdate", "long"),
        ("customername", "string", "cc_customername", "string"),
        ("birthday", "string", "cc_birthday", "string"),
        ("sharewithfriendsasofdate", "long", "cc_sharewithfriendsasofdate", "long"),
        ("sharewithpublicasofdate", "long", "cc_sharewithpublicasofdate", "long"),
        ("lastupdatedate", "long", "cc_lastupdatedate", "long"),
        ("email", "string", "cc_email", "string"),
        ("serialnumber", "string", "cc_serialnumber", "string"),
        ("sharewithresearchasofdate", "long", "cc_sharewithresearchasofdate", "long"),
        ("phone", "string", "cc_phone", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1702138191250",
)

# Script generated for node Join
SqlQuery147 = """
SELECT *
FROM
  step_trainer_landing
JOIN
  customer_curated
ON
  step_trainer_landing.serialnumber = customer_curated.cc_serialnumber
"""
Join_node1702140493166 = sparkSqlQuery(
    glueContext,
    query=SqlQuery147,
    mapping={
        "step_trainer_landing": StepTrainerLandingZone_node1702138081168,
        "customer_curated": RenamedkeysforJoin_node1702138191250,
    },
    transformation_ctx="Join_node1702140493166",
)

# Script generated for node Drop Fields
DropFields_node1702138204275 = DropFields.apply(
    frame=Join_node1702140493166,
    paths=[
        "cc_registrationdate",
        "cc_phone",
        "cc_customername",
        "cc_birthday",
        "cc_sharewithfriendsasofdate",
        "cc_sharewithpublicasofdate",
        "cc_lastupdatedate",
        "cc_email",
        "cc_serialnumber",
        "cc_sharewithresearchasofdate",
    ],
    transformation_ctx="DropFields_node1702138204275",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1702141315107 = DynamicFrame.fromDF(
    DropFields_node1702138204275.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1702141315107",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1702138222122 = glueContext.getSink(
    path="s3://udacity-aniq/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrustedZone_node1702138222122",
)
StepTrainerTrustedZone_node1702138222122.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrustedZone_node1702138222122.setFormat("json")
StepTrainerTrustedZone_node1702138222122.writeFrame(DropDuplicates_node1702141315107)
job.commit()
