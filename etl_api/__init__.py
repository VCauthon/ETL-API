from etl_api.extractor import Extractor
from etl_api.transformer import Transformer
from etl_api.loader import Loader


__version__ = '0.1.0'


class ETLPipeline:
    def __init__(
            self,
            extractor: Extractor,
            transformer: Transformer,
            loader: Loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        data = self.extractor.extract()
        transformed_data = self.transformer.transform(data)
        self.loader.load(transformed_data)