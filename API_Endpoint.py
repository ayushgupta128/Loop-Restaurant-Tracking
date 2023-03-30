from flask import Flask, render_template, jsonify
from jproperties import Properties
import loop
import string
import random
import mysql.connector
import pandas as pd
import json
from threading import Thread

app=Flask(__name__)

configs = Properties()
with open('app-config.properties', 'rb') as config_file:
		 configs.load(config_file)

def build_Mysql_Connection():
	global configs
	try:
		connection = mysql.connector.connect(host=configs.get("mysql_host").data,
		                                     database=configs.get("mysql_report_database").data,
		                                     user=configs.get("mysql_user").data,
		                                     password=configs.get("mysql_password").data)
		if connection.is_connected():
			print(connection)
			db_Info = connection.get_server_info()
			print("Connected to MySQL Server version ", db_Info)
			return connection, connection.cursor()
	except Exception as err:
	   print("Connection to MySQL DB failed. ERROR - "+ str(err))		
	finally:
	   if connection.is_connected():
	   	  pass

def call_loop(reportdb, report_id, report_status_table):
	Loop_Report_Obj=loop.Report_Trigger_Spark(reportdb, report_id, report_status_table)
	response = Loop_Report_Obj.start_report_generation()

@app.route('/trigger')
def trigger():
	global configs
	N = 10
	report_id=''.join(random.choices(string.ascii_uppercase + string.digits, k=N))
	try:
		print(report_id)
		connection, cursor = build_Mysql_Connection()
		print(report_id)
		print(cursor)
		cursor.execute("Create Table if not exists {report_status_table}(report_id varchar(20), status varchar(50))".format(report_status_table=configs.get("mysql_report_status_table").data))

		cursor.execute("Insert into {report_status_table} values('{rep_id}','{status}')".format(report_status_table=configs.get("mysql_report_status_table").data,rep_id=report_id,status="Running"))
		connection.commit()

		thread = Thread(target=call_loop, kwargs={"reportdb":configs.get("mysql_report_database").data, "report_id": report_id, "report_status_table": configs.get("mysql_report_status_table").data})
		thread.start()

		return jsonify({ "Report ID":report_id })

	except Exception as err:
	    print("Error Executing Queries on MySQL DB. ERROR - " + str(err))


@app.route('/get_report/<string:rep_id>')
def get_report(rep_id):
	global configs
	connection, cursor = build_Mysql_Connection()
	#cursor = connection.cursor()
	print("Fetching Data for Report ID - "+ str(rep_id))
	cursor.execute("SELECT * FROM reportdb.report_status where report_id='{0}'".format(rep_id))
	data_table = cursor.fetchall()

	# Checking Whether the Report ID entered is VALID or not.
	if len(data_table) == 0:
		return "The Report id entered is incorrect. Try Again"
	table_name = data_table[0][1]

	if table_name == "Running":
		return jsonify({"Report Status": "Running"})
    
	print("Report ID is Valid. Proceeding to fetch Data")
	cursor.execute("SELECT * FROM {final_table_name}".format(final_table_name=table_name))
	data = cursor.fetchall()
	dataframe = pd.DataFrame(data, columns = ['store_id', 'uptime_last_hour', 'uptime_last_day','uptime_last_week','downtime_last_hour','downtime_last_day','downtime_last_week'])

	return json.dumps({"Report Status": "Complete"}) + "<br>" + render_template('table.html', tables=[dataframe.to_html()], titles=[''])

if __name__ == "__main__":
     app.run(debug=True)







