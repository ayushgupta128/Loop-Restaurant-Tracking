with DAG():
    numeric_metrics_1 = Task(
        task_id = "numeric_metrics_1", 
        component = "Dataset", 
        table = {"name" : "p1", "sourceType" : "Seed"}, 
        writeOptions = {"writeMode" : "overwrite"}
    )
    numeric_metrics_1_1 = Task(
        task_id = "numeric_metrics_1_1", 
        component = "Dataset", 
        table = {"name" : "p1", "sourceType" : "Seed"}, 
        writeOptions = {"writeMode" : "overwrite"}
    )
    p1__join_on_a = Task(task_id = "p1__join_on_a", component = "Model", modelName = "p1__join_on_a")
    numeric_metrics = Task(
        task_id = "numeric_metrics", 
        component = "Dataset", 
        table = {"name" : "p1", "sourceType" : "Seed"}, 
        writeOptions = {"writeMode" : "overwrite"}
    )
    numeric_metrics.out >> p1__join_on_a.in_0
    numeric_metrics_1.out >> p1__join_on_a.in_1
    numeric_metrics_1_1.out >> p1__join_on_a.in_3
