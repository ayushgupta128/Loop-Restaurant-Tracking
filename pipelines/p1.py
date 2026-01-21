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
    entity_name_origin = Task(
        task_id = "entity_name_origin", 
        component = "Dataset", 
        table = {"name" : "klp", "sourceType" : "Table", "sourceName" : "ayush_demos_demos", "alias" : ""}, 
        writeOptions = {"writeMode" : "overwrite"}
    )
    people_0.out0 >> entity_name_origin.in0
