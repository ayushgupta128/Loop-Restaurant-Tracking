with DAG():
    ds1 = Task(
        task_id = "ds1", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "ds1", "sourceName" : "ayush_demos_demos", "sourceType" : "Table"}
    )
