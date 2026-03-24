from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, prophecy_project_config=None, **kwargs):
        self.spark = None
        self.update(prophecy_project_config, **kwargs)

    def update(self, prophecy_project_config=None, **kwargs):
        prophecy_spark = self.spark
        prophecy_project_config = self.update_project_conf_values(prophecy_project_config, kwargs)
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config)
        pass
