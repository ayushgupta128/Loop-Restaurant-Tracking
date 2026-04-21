from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, vrfe: str=None, **kwargs):
        self.spark = None
        self.update(vrfe)

    def update(self, vrfe: str="vera", **kwargs):
        prophecy_spark = self.spark
        self.vrfe = vrfe
        pass
