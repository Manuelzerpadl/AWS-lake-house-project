# Project: STEDI Human Balance Analytics
Spark and AWS Glue allow you to process data from multiple sources, categorize the data, and curate it to be queried in the future for multiple purposes. As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

# Project Details
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

trains the user to do a STEDI balance exercise;
and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.


## Usage
### Prerequisites
- An S3 bucket to store data categorized into either the landing, trusted, or curated zone
- Landing zone S3 buckets to ingest raw customer, step trainer, and accelerometer JSON files
- IAM permissions for S3, Glue, and Athena
- Database specific for project's Glue tables`

### Outline
The solution is built on AWS and uses the following services:
- S3 for data storage
- Glue for data processing
- Athena for querying data

My data lakehouse solution is comprised of five Python scripts which are run in AWS Glue. The scripts are run in the following order:
1. `Customer_Landing_to_Trusted.py`: This script transfers customer data from the 'landing' to 'trusted' zones. It filters for customers who have agreed to share data with researchers.
2. `Accelerometer_Landing_to_Trusted.py`: This script transfers accelerometer data from the 'landing' to 'trusted' zones. It filters for Accelerometer readings from customers who have agreed to share data with researchers.
3. `Customer_Trusted_to_Curated.py`: This script transfers customer data from the 'trusted' to 'curated' zones. It filters for customers with Accelerometer readings and have agreed to share data with researchers.
4. `Step_Trainer_Landing_to_Curated.py`: This script transfers step trainer data from the 'landing' to 'curated' zones. It filters for curated customers with Step Trainer readings.
5. `Machine_Learning_Curated.py`: This script combines Step Trainer and Accelerometer data from the 'curated' zone into a single table to train a machine learning model.

