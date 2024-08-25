from flask import Flask, request, jsonify


from etl_api.base import ModuleDetail
from etl_api.extractor import Extractor, ExtractionTypes
from etl_api.transformer import Transformer, TransformationTypes
from etl_api.loader import Loader, LoaderType


app = Flask(__name__)


class ETLPipeline:
    def __init__(
        self,
        extractor: Extractor.Arguments,
        transformer: Transformer.Arguments,
        loader: Loader.Arguments,
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        data, schema = Extractor.extract(
            api=self.extractor.api, **self.extractor.arguments
        )
        transformed_data = Transformer.transform(
            type=self.transformer.type, data_raw=data, data_schema=schema
        )
        return Loader.load(type=self.loader.type, data=transformed_data)


def parse_arguments(expected_arguments: ModuleDetail):
    def wrapper(func):
        def args_wrapper(*args, **kwargs):
            received_args = request.args.to_dict()
            sent_args = {
                val.name: received_args.get(val.name)
                for val in expected_arguments.config
                if val.name in received_args
            }
            if len(sent_args) != len(expected_arguments.config):
                missing_args = [
                    val.name
                    for val in expected_arguments.config
                    if val.name not in received_args
                ]
                raise ValueError(f"Missing arguments: {missing_args}")
            return func(**sent_args)

        return args_wrapper

    return wrapper


@app.route("/api/help", methods=["GET"])
def list_extraction_options():
    context = Extractor.get_options(ExtractionTypes.YahooFinance)
    return jsonify(
        [
            {
                "url": "/api/yahoofinance",
                "description": context.description,
            },
        ]
    )


@app.route("/api/yahoofinance", methods=["GET"])
@parse_arguments(Extractor.get_options(ExtractionTypes.YahooFinance))
def yahoofinance_get_data(ticker: str, period: str):
    pipeline = ETLPipeline(
        extractor=Extractor.Arguments(
            api=ExtractionTypes.YahooFinance,
            arguments={"ticker": ticker, "period": period},
        ),
        transformer=Transformer.Arguments(
            type=TransformationTypes.JSON  # TODO: Needs a transform to DICT
        ),
        loader=Loader.Arguments(type=LoaderType.RETURN_REQUEST),
    )

    return pipeline.run()
