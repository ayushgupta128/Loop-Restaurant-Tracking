from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, sub1: str=None, sub2: str=None, prophecy_project_config=None, **kwargs):
        self.spark = None
        self.update(sub1, sub2, prophecy_project_config, **kwargs)

    def update(self, sub1: str="sub1", sub2: str="sub2", prophecy_project_config=None, **kwargs):
        prophecy_spark = self.spark
        prophecy_project_config = self.update_project_conf_values(prophecy_project_config, kwargs)
        self.update_and_add_project_config(prophecy_spark, prophecy_project_config)
        self.sub1 = sub1
        self.sub2 = sub2
        pass
