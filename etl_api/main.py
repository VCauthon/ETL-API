from flask import Flask, request, jsonify

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


@app.route("/api/help", methods=["GET"])  # TODO: There is a better way to handle this ?
def list_extraction_options():
    return jsonify(
        [
            {
                "url": "/api/yahoofinance",
                "description": "Gives the prices history from a concrete ticker/isin",  # TODO: Add a description from the own class
            },
        ]
    )


# TODO: Look how to handle the return of parameters for each selected option


@app.route("/api/yahoofinance", methods=["GET"])  # TODO: This must be a POST instead
def yahoofinance_get_data():
    args = (
        request.args.to_dict()
    )  # TODO: Find a better way to handle this only sending the needed arguments

    pipeline = ETLPipeline(
        extractor=Extractor.Arguments(api=ExtractionTypes.YahooFinance, arguments=args),
        transformer=Transformer.Arguments(
            type=TransformationTypes.JSON  # TODO: Needs a transform to DICT
        ),
        loader=Loader.Arguments(
            type=LoaderType.RETURN_REQUEST  # TODO: Based on loader the transformer must be define
        ),
    )

    return pipeline.run()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
