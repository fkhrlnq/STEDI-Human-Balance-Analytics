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

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1701772602373 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_landing",
        transformation_ctx="AccelerometerLandingZone_node1701772602373",
    )
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1701772064606 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted_2",
    transformation_ctx="CustomerTrustedZone_node1701772064606",
)

# Script generated for node Join Customer
JoinCustomer_node1701771743818 = Join.apply(
    frame1=AccelerometerLandingZone_node1701772602373,
    frame2=CustomerTrustedZone_node1701772064606,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1701771743818",
)

# Script generated for node Drop Fields
DropFields_node1701773488634 = DropFields.apply(
    frame=JoinCustomer_node1701771743818,
    paths=[
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1701773488634",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1701771766151 = glueContext.getSink(
    path="s3://udacity-aniq/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrustedZone_node1701771766151",
)
AccelerometerTrustedZone_node1701771766151.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrustedZone_node1701771766151.setFormat("json")
AccelerometerTrustedZone_node1701771766151.writeFrame(DropFields_node1701773488634)
job.commit()
