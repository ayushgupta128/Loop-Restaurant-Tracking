**Video Demonstration of this project: https://drive.google.com/file/d/1OUh89EXESOswbHMYBkhAm9RFGVtH1ysR/view?usp=drive_web**

The Database used in this proect is MySQL DB - version 8.0.

All 3 data sources are loaded in mysql "inputdb" as database and  tables as (stores_status, menu_Hours, timezone_info)

The data computation is done using spark using Mysql connector jars and driver config.

pandas is used to generate final dataframe and output it as html template in /get_report endpoint

There are 4 main modules in this Project

**1) API_Endpoint.py:**
Backend API is created using Flask application in this module.
a. /trigger_report endpoint that will trigger report generation from the data provided (stored in mysql DB)
    1. No input 
    2. Output - report_id (random string is generated using python random library) 
    - Connection to mysql Database is created using mysql.connector module
    - report_id is inserted into reportdb.report_status table with status "Running"
    - Implemented using trigger() function. A separate thread is created using builtin Thread class which runs loop.py module to perform data computation and generate final data.
    
b. /get_report/<string: report_id> endpoint that will return the status of the report or the csv
    1. Input - report_id
    2. Output
        - if report generation is not complete, return “Running” as the output
        - if report generation is complete, return “Complete” along with the CSV file with the schema ['store_id', 'uptime_last_hour',   'uptime_last_day','uptime_last_week','downtime_last_hour','downtime_last_day','downtime_last_week'].

    - Connection to mysql Database is created using mysql.connector module
    - Validation check is applied to make sure report_id entered in URL is valid or not. If invalid, warning message is returned to the user.
    - If report_id is valid,
              Check if The status of report_id is "Running" then return status as "Running"    
              Check if the status of report_id is "{db}.{table}" format then return Report status as "Complete" plus final csv data in html format to the response of "/get_report/<string: report_id>" endpoint 


**2) loop.py:**
Spark is used to do computation on data.

The data computation using spark is performed in this module to generate output data.
- The Extrapolation Logic
       - The record of status of a restraunt is available in 1st data source "df_store_status" oly if it is active when polled.
       - So, The extrapolation logic is created like whenever we find a restraunt is active we extrapolate the status as active for last 1  hour (range - [extrapolate_start_time, extrapolate_end_time] where extrapolate_start_time + 1 Hour = extrapolate_end_time) for that restaurent and for other timeframe status is assumed to be inactive. Because we do not have enough data points to contradict that logic.

- All 3 data sources are joined using spark dataframe oin operation with appropiate fields.
- The Data is filtered with records with extrapolate_start_time and extrapolate_end_time out of Business Hour Range       
- The timestamp is converted into a single timezone of UTC to avoid confusion.
- The data is then aggregated by store_id to compute uptime and downtime for Last Hour, Last Day and Last Week in minutes, hour and hour respectively.
- The status of the generated random report_id is changed from "Running" to "reportdb.{report_id}_info_data" in mysql db database "reportdb" and table "report_status"

**3) app-config.properties:**
This file contain MySQL DB connection properties to avoid hard-coding of sensitive information in main code.

**4) templates/table.html:**
This html file contain the output html template of data which has to be shown to user when they hit /get_report/<string: report_id> endpoint.







