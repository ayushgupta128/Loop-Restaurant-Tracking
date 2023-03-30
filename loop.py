from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mysql.connector

class Report_Trigger_Spark:
	def __init__(self, report_db, report_id, report_status_table):

		# Initializing Spark Session with required dependencies
		spark = SparkSession.builder.appName('loop').config("spark.jars", "mysql-connector-java-8.0.13.jar").getOrCreate()

		self.report_id = report_id
		self.report_db = report_db
		self.report_status_table=report_status_table

		# Loading Store Status DataFrame from mysql
		print("Loading Store Status DataFrame from mysql")
		df_store_status=spark.read \
		    .format("jdbc") \
		    .option("driver","com.mysql.cj.jdbc.Driver") \
		    .option("url", "jdbc:mysql://localhost:3306/inputdb") \
		    .option("dbtable", "stores_status") \
		    .option("user", "root") \
		    .option("password", "mysql123") \
		    .load()

        
        # Adding weekday column (1 - Sunday 7 - Saturday) for timestamp_utc field and casting it to Timestamp Type
		print("Adding weekday column (1 - Sunday 7 - Saturday) for timestamp_utc field and casting it to Timestamp Type")
		self.df_store_status=df_store_status.withColumn("weekday",dayofweek("timestamp_utc")).select("store_id","status",col("timestamp_utc").cast(TimestampType()),"weekday")


		self.df_store_status.printSchema()
		self.df_store_status.show(10,False)

		# Loading Active Business Hours DataFrame from mysql
		print("Loading Active Business Hours DataFrame from mysql")
		df_active_bus_hrs=spark.read \
		    .format("jdbc") \
		    .option("driver","com.mysql.cj.jdbc.Driver") \
		    .option("url", "jdbc:mysql://localhost:3306/inputdb") \
		    .option("dbtable", "menu_Hours") \
		    .option("user", "root") \
		    .option("password", "mysql123") \
		    .option("inferSchema",True) \
		    .load()

		# Changing week day id of Monday from 0 to 2 of df_active_bus_hrs
		print("Changing week day id of Monday from 0 to 2 of df_active_bus_hrs")
		self.df_active_bus_hrs=df_active_bus_hrs.withColumn("dayofweek",(col("day").cast("int")+2)%7) \
		                                   .withColumn("weekday",when(col("dayofweek")==0,7).otherwise(col("dayofweek"))).drop("dayofweek","day")

		self.df_active_bus_hrs.printSchema()
		self.df_active_bus_hrs.show()

		# Loading Timezone dataframe from mysql  
		print("Loading Timezone dataframe from mysql")                              
		self.df_tz_info=spark.read \
		    .format("jdbc") \
		    .option("driver","com.mysql.cj.jdbc.Driver") \
		    .option("url", "jdbc:mysql://localhost:3306/inputdb") \
		    .option("dbtable", "timezone_info") \
		    .option("user", "root") \
		    .option("password", "mysql123") \
		    .option("inferSchema",True) \
		    .load()    

		self.df_tz_info.printSchema()
		self.df_tz_info.show()    

	# Function Which triggers the computation for target report data
	def start_report_generation(self):
		df_active_bus_hrs=self.df_active_bus_hrs
		df_tz_info=self.df_tz_info
		df_store_status=self.df_store_status

		print("Triggering the computation for target report data")

		# Left Joining active business hrs with timezone dataframe and setting timezone as America/Chicago for NULL fields
		df_active_bus_hrs_timezone=df_active_bus_hrs.join(df_tz_info,df_active_bus_hrs.store_id==df_tz_info.store_id,"left").select(df_active_bus_hrs.store_id, \
		                                                                                                     "start_time_local","end_time_local","weekday", \
		                                                                                                     when(col("timezone_str").isNull(),"America/Chicago").otherwise(col("timezone_str")).alias("timezone_str"))


		# Converting all local timrstamps to standard UTC timestamp
		df_bus_hrs=df_active_bus_hrs_timezone.withColumn("start_time_utc",to_utc_timestamp("start_time_local",col("timezone_str"))).withColumn("end_time_utc",to_utc_timestamp("end_time_local",col("timezone_str"))) \
		                                                                                             .withColumn("utc_start",date_format("start_time_utc","HH:mm:ss")) \
		                                                                                             .withColumn("utc_end",date_format("end_time_utc","HH:mm:ss")) \
		                                                                                             .drop("end_time_utc","start_time_utc","start_time_local","end_time_local")

		 # Left Joining Store Status Dataframe with df_bus_hrs on "store_id","weekday" and setting start_utc="00:00:00" and end_utc="23:59:59" in place of NULL for 24*7 open stores                                                                                             
		df_status_busHrs=df_store_status.join(df_bus_hrs,["store_id","weekday"],"left").drop("timezone_str").select("store_id","status","weekday","timestamp_utc",when(col("utc_start").isNull(),"00:00:00").otherwise(col("utc_start")).alias("start_time_utc"), \
		                                        when(col("utc_end").isNull(),"23:59:59").otherwise(col("utc_end")).alias("end_time_utc"))


		# Building a new column extrapolate_start_time = col("timestamp_utc")-expr("INTERVAL 1 HOUR") and extrapolate_end_time = col("timestamp_utc")
		df_status_busHrs=df_status_busHrs.withColumn("extrapolate_start_time",col("timestamp_utc")-expr("INTERVAL 1 HOUR")) \
		                .withColumn("extrapolate_end_time",col("timestamp_utc"))

		# Filtering out records with extrapolate_start_time and extrapolate_end_time out of Business Hour Range
		df_final=df_status_busHrs.filter(~((date_format(col("extrapolate_end_time"),"HH:mm:ss.SSS")<col("start_time_utc")) | (date_format(col("extrapolate_start_time"),"HH:mm:ss.SSS")>col("end_time_utc")))) \
		                       .withColumn("last_hour_time", to_timestamp(lit("2023-01-25 18:13:18.64706"),"yyyy-MM-dd HH:mm:ss.SSSSS")-expr("INTERVAL 1 HOUR")) \
		                       .withColumn("last_day_time", to_timestamp(lit("2023-01-25 18:13:18.64706"),"yyyy-MM-dd HH:mm:ss.SSSSS")-expr("INTERVAL 1 DAY")) \
		                       .withColumn("last_week_time", to_timestamp(lit("2023-01-25 18:13:18.64706"),"yyyy-MM-dd HH:mm:ss.SSSSS")-expr("INTERVAL 1 WEEK")) \
		                       .withColumn("current time",to_timestamp(lit("2023-01-25 18:13:18.64706"),"yyyy-MM-dd HH:mm:ss.SSSSS")).withColumn("DiffInSeconds_lastHour",unix_timestamp("extrapolate_end_time")-unix_timestamp("last_hour_time")) \
		                       .withColumn("DiffInSeconds_lastDay",unix_timestamp("extrapolate_end_time")-unix_timestamp("last_day_time")) \
		                       .withColumn("DiffInSeconds_lastWeek",unix_timestamp("extrapolate_end_time")-unix_timestamp("last_week_time")) 

		# Performaing Group By operation of store_id and calculating uptime and downtime for Last Hour, Last Day and Last Week        
		df_Final_RestrauntData=df_final.groupBy("store_id").agg(sum(expr("case when DiffInSeconds_lastHour<0 then 0 else DiffInSeconds_lastHour/60 end as uptime_last_hour")).alias("uptime_last_hour"),
		                               sum(expr("case when DiffInSeconds_lastDay<0 then 0 else DiffInSeconds_lastDay/3600 end as uptime_last_day")).alias("uptime_last_day"),
		                               sum(expr("case when DiffInSeconds_lastWeek<0 then 0 else DiffInSeconds_lastWeek/3600 end as uptime_last_week")).alias("uptime_last_week"),
		                               sum(expr("case when DiffInSeconds_lastHour<0 then 60 else 60-DiffInSeconds_lastHour/60 end as uptime_last_hour")).alias("downtime_last_hour"),
		                               sum(expr("case when DiffInSeconds_lastDay<0 then 24 else 24-DiffInSeconds_lastDay/3600 end as uptime_last_hour")).alias("downtime_last_day"),
		                               sum(expr("case when DiffInSeconds_lastWeek<0 then 168 else 168-DiffInSeconds_lastWeek/3600 end as uptime_last_hour")).alias("downtime_last_week"))		                          
		df_Final_RestrauntData.show(10,False)
		df_Final_RestrauntData.printSchema()
        
		# Writing final DataFrame back to Mysql table {report_id}_info_data
		df_Final_RestrauntData.write \
							  .format("jdbc") \
							  .option("driver","com.mysql.cj.jdbc.Driver") \
							  .option("url", "jdbc:mysql://localhost:3306/{0}".format(self.report_db)) \
							  .option("dbtable", "{0}_info_data".format(self.report_id)) \
							  .option("user", "root") \
							  .option("password", "mysql123") \
						      .save()

		print("Updating the status of final report_status table for the report_id using mysql connector")
		return self.Update_Report_DB_status("{0}_info_data".format(self.report_id))
    
	# Updating the status of final report_status table for the report_id using mysql connector
	def Update_Report_DB_status(self, final_data_table):
		try:
		    connection = mysql.connector.connect(host='localhost',
		                                         database='reportdb',
		                                         user='root',
		                                         password='mysql123')

		    if connection.is_connected():
		          cursor = connection.cursor()
		          print("update {0} set status='{1}.{2}' where report_id='{3}'".format(self.report_status_table, self.report_db, final_data_table, self.report_id))
		          cursor.execute("update {0} set status='{1}.{2}' where report_id='{3}'".format(self.report_status_table, self.report_db, final_data_table, self.report_id)) 
		          connection.commit()

		except Exception as err:
		   print("Status updation failed due to error "+str(err))        
		   return False       

		return True                                  	


