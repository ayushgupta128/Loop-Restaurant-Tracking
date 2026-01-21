with DAG():
    people_0 = SourceTask(
        task_id = "people_0", 
        component = "OrchestrationSource", 
        kind = "DatabricksSource", 
        connector = Connection(
          kind = "databricks", 
          provider = "", 
          clientId = "", 
          oAuthType = "u2m", 
          kgConfig = {
            "authProperties": {"authType" : "oauth", "clientId" : "", "oAuthAppRegistrationID" : "", "oAuthType" : "u2m"}, 
            "kgSchedule": {}, 
            "useConnectorAuth": True
          }, 
          authType = "pat", 
          id = "databricks_2", 
          oAuthAppRegistrationID = ""
        ), 
        format = DATABRICKSFormat(
          schema = {
            "fields": [{"name" : "name", "dataType" : {"type" : "utf8"}},                         {"name" : "row_origin", "dataType" : {"type" : "utf8"}}], 
            "providerType": "arrow"
          }
        ), 
        tableFullName = {"database" : "ayush_demos", "name" : "people", "schema" : "demos"}
    )
    p1__entity_name_filter = Task(
        task_id = "p1__entity_name_filter", 
        component = "Model", 
        modelName = "p1__entity_name_filter"
    )
