
---
# * IN PROGRESS *
---
<img src="assets/apache_airflow_logo.png" width="33%" align="right" alt="" title="logo" />

### Udacity Data Engineering Nanodegree
# Project 5: Data Pipelines with Airflow


##### &nbsp;


For instructions on how to setup and run this project, jump to the ['Running the Project'](https://github.com/tommytracey/udacity_data_engineering/tree/master/p5_data_pipeline_airflow#running-the-project) section.
##### &nbsp;


## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


##### &nbsp;

## Goals
This project introduces the core concepts of Apache Airflow. To complete the project, we need to create a pipeline that:

1. Stages data
1. Populates a data warehouse
1. Runs checks on the data to make sure the process completed successfully


##### &nbsp;

## Project Scope
Udacity provides a project template that takes care of all the imports. However, there are four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that need to be linked to achieve a coherent and sensible data flow within the pipeline.

Udacity also provides a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

#### Example Pipeline DAG
<img src="assets/example-dag.png" width="100%" align="upper-left" alt="" title="Example DAG" />

##### &nbsp;

### Project Setup & Instructions
Below are links to more detailed steps.

1. [Add Airflow connections to AWS](instructions/01-airflow-aws-instructions.pdf)
1. [Configure DAG and build the operators](instructions/02-project-instructions.pdf)
1. [Start the Airflow web server](instructions/03-workspace-instructions.pdf)


##### &nbsp;

---

## My Implementation

*** IN PROGRESS ***

##### &nbsp;

### Running the Project
<!--Here are the steps to run my implementation of this project:

1. Create your own S3 bucket and Amazon EMR cluster with the dependencies listed in `requirements.txt`.
    - Or, if you're a student in Udacity's Data Engineering Nanodegree (DEND) program, you can use the project workspace.

1. Add your AWS keys to `dl.cfg` in the project root folder. Do not enclose your key values in quotes.

1. Update the data paths within the `main()` function in `etl.py`. These paths should point to the input and output directories you are using.
   - A subset of the input data can be found in the `/data` directory.

1. Run the ETL process:
`$ python etl.py`

1. Examine your output data directory to verify the data is properly partitioned and stored in parquet format.
    - A sample of my output can be found in the `/analytics` directory.

1. Run a few Spark queries on the parquet output files to verify the tables are structured correctly and populated with data. This can be done via the notebook [`test-outputs.ipynb`](test-outputs.ipynb).

1. Delete your AWS cluster and S3 bucket (if needed). This must be done manually.


##### &nbsp;

### Schema for Song Play Analysis
Using the song and event datasets, here is a star schema designed to support song play analysis.

#### Fact Table
- **songplays** &mdash; records in event data associated with song plays, i.e. records with `page = NextSong`

#### Dimension Tables
- **users** &mdash; users in the app
- **songs** &mdash; songs in music database
- **artists** &mdash; artists in music database
- **time** &mdash; timestamps of records in songplays broken down into specific units

#### Schema Diagram

<img src="assets/sparkify-schema.png" width="100%" align="top-left" alt="" title="Sparkify Schema" />


##### &nbsp;

### Spark Query Example

<img src="assets/songs-table-verify.png" width="70%" align="top-left" alt="" title="Sample query" />

_view all table queries in [test-outputs.ipynb](test-outputs.ipynb)_

-->
