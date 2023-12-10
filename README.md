# STEDI-Human-Balance-Analytics

## Project Overview

### Project Introduction

In this project, we aim to build a data lakehouse solution for the STEDI team, specifically focusing on sensor data to train a machine learning model. The STEDI Step Trainer, equipped with sensors, aims to train users in balance exercises while collecting data to enhance a machine-learning algorithm for step detection. The project also involves a companion mobile app that collects customer data and interacts with the device sensors.

### Project Details

The hardware STEDI Step Trainer collects data through motion sensors, and the companion mobile app collects data using a mobile phone accelerometer. The objective is to use this motion sensor data to train a machine learning model capable of real-time step detection. Privacy is a primary concern, and only data from customers who have agreed to share their information for research purposes should be used in the training data.

### Project Summary

As a data engineer on the STEDI Step Trainer team, the goal is to extract data from the Step Trainer sensors and the mobile app, curate this data into a data lakehouse solution on AWS, allowing Data Scientists to train the machine learning model.

## Project Data

### Customer Records

**Data Source:** [Customer Record](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/tree/main/data/customer/landing)

**Fields:**

-   serialnumber
-   sharewithpublicasofdate
-   birthday
-   registrationdate
-   sharewithresearchasofdate
-   customername
-   email
-   lastupdatedate
-   phone
-   sharewithfriendsasofdate

### Step Trainer Records

**Data Source:** [Step Trainer Records](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/tree/main/data/step_trainer/landing)

**Fields:**

-   sensorReadingTime
-   serialNumber
-   distanceFromObject

### Accelerometer Records

**Data Source:** [Accelerometer Records](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/tree/main/data/accelerometer/landing)

**Fields:**

-   timeStamp
-   user
-   x
-   y
-   z

## Implementation

### Landing Zone

**Glue Tables SQL DDL**:

 -   [customer_landing.sql](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/customer_landing.sql)
 -   [accelerometer_landing.sql](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/accelerometer_landing.sql)
 -  [step_trainer_landing.sql](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/step_trainer_landing.sql)

**Athena Landing Zone Data Query**

 - `customer_landing` table:
![customer_landing](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/customer_landing.jpg)

 -   The `customer_landing` data contains multiple rows with a blank *shareWithResearchAsOfDate*.
   ![The customer_landing data contains multiple rows with a blank shareWithResearchAsOfDate](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/customer_landing_sharewithresearchasofdate.jpg)

 - `accelerometer_landing` table:
![accelerometer_landing](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/accelerometer_landing.jpg)

 - `step_trainer_landing` table:
![step_trainer_landing](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/step_trainer_landing.jpg)

### Trusted Zone

**Glue job scripts**:

-   [customer_landing_to_trusted.py](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/customer_landing_to_trusted.py)
-   [accelerometer_landing_to_trusted_zone.py](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/accelerometer_landing_to_trusted.py)
-  [step_trainer_trusted.py](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/step_trainer_trusted.py)

**Athena Trusted Zone Data Query**
 - `customer_trusted` table:
![customer_trusted](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/customer_trusted.jpg)

 -  The resulting `customer_trusted`  data has no rows where *shareWithResearchAsOfDate* is blank.
![*shareWithResearchAsOfDate* is blank](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/customer_trusted_sharewithresearchasofdate_is_null.jpg)

 - `accelerometer_trusted` table:
![accelerometer_trusted](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/accelerometer_trusted.jpg)

- `step_trainer_trusted` table:
![step_trainer_trusted](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/step_trainer_trusted.jpg)

## Curated Zone

**Glue job scripts**:
- [customer_trusted_to_curated.py](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/customer_trusted_to_curated.py)
-  [step_trainer_trusted.py](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/step_trainer_trusted.py)
- [machine_learning_curated.py](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/scripts/machine_learning_curated.py)

**Athena Curated Zone Data Query**
- `customer_curated` table:
![customer_curated](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/customer_curated.jpg)

- `machine_learning_curated` table:
![machine_learning_curated](https://github.com/fkhrlnq/STEDI-Human-Balance-Analytics/blob/main/images/machine_learning_curated.jpg)
