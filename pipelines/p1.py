with DAG():
    numeric_metrics_1 = Task(
        task_id = "numeric_metrics_1", 
        component = "Dataset", 
        table = {"name" : "p1", "sourceType" : "Seed"}, 
        writeOptions = {"writeMode" : "overwrite"}
    )
    numeric_metrics = Task(
        task_id = "numeric_metrics", 
        component = "Dataset", 
        table = {"name" : "p1", "sourceType" : "Seed"}, 
        writeOptions = {"writeMode" : "overwrite"}
    )
    p1__limit_gem = Task(task_id = "p1__limit_gem", component = "Model", modelName = "p1__limit_gem")
    numeric_metrics.out >> p1__limit_gem.in_0
    numeric_metrics_1.out >> p1__limit_gem.in_1
