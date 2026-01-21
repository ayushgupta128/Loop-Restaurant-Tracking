with DAG():
    trigger_p1_pipeline = Task(
        task_id = "trigger_p1_pipeline", 
        component = "Pipeline", 
        maxTriggers = 10000, 
        triggerCondition = "Always", 
        enableMaxTriggers = False, 
        pipelineName = "p1", 
        parameterSet = "default", 
        parameters = {}
    )
