"""
python consumer.py --streaming
또는
python consumer.py --streaming \
--runner DataflowRunner \
--project bqml-tutorial-314812 \
--region us-central1 \
--temp_location gs://tutorial-bucket-2021/tmp \
--staging_location gs://tutorial-bucket-2021/staging
"""

import os
import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
)

from config import Config
from inference import predict_sentiment


logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Config.credentials_path


class PredictTwitterSentiment(beam.DoFn):
    """모델 예측 처리 클래스"""

    def process(
        self,
        element: bytes,
        timestamp=beam.DoFn.TimestampParam,
        window=beam.DoFn.WindowParam,
    ):

        parsed = json.loads(element.decode("utf-8"))
        print(parsed)
        parsed["timestamp"] = timestamp.to_rfc3339()
        pred_cls, positive, neutral, negative = predict_sentiment(parsed["message"])
        parsed["predicted_value"] = float(pred_cls)
        parsed["positive_score"] = positive
        parsed["neutral_score"] = neutral
        parsed["negative_score"] = negative
        print()
        yield parsed


def run(save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='Google Cloud Pub/Sub의 Subscription ID: "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
        default=Config.pubsub_subscription_id,
    )

    parser.add_argument(
        "--output_table", help="결과를 저장할 빅쿼리 테이블 이름", default=Config.bq_table_name
    )
    parser.add_argument(
        "--output_schema",
        help="결과를 저장할 빅쿼리 테이블 스키마 : 필드명:자료형,필드명:자료형의 구조",
        default=Config.bq_table_scheme,
    )
    known_args, pipeline_args = parser.parse_known_args()

    print(known_args)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub"
            >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=known_args.input_subscription, timestamp_attribute=None
            )
            | "PredictTwitterSentiment" >> beam.ParDo(PredictTwitterSentiment())
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()
