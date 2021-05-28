"""
python provider.py --keywords python
또는
python provider.py --keywords python --publish
"""

import os
import json
import argparse
import tweepy

from datetime import datetime
from pprint import pprint
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.futures import Future

from config import Config

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Config.credentials_path


class PubSubPublisher:
    """Pub/Sub에 publish하는 클래스"""

    def __init__(self, project_id: str, topic_id: str) -> None:
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    def publish(self, data: str) -> Future:

        result = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        return result


class StreamListener(tweepy.StreamListener):
    """트위터 스트리밍 API"""

    def on_status(self, status):
        print(datetime.now())
        data = json.dumps({"message": status.text})

        if PUBLISH:
            publisher.publish(data)
            pprint(f"* Published data:")
        else:
            print(f"* Streaming data:")
        pprint(data)
        print("====================================\n")

    def on_error(self, status_code):
        if status_code == 420:
            return False


publisher = PubSubPublisher(Config.project_id, Config.pubsub_topic_id)


def publish_twitter_stream(keywords):

    auth = tweepy.OAuthHandler(Config.twitter_api_key, Config.twitter_api_secret_key)
    auth.set_access_token(
        Config.twitter_access_token, Config.twitter_access_token_secret
    )
    api = tweepy.API(auth)
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=keywords.split(","), languages=["en"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--keywords", help="트위터 검색을 위한 검색어. 쉼표(,)로 구분하여 입력하세요.", required=True
    )
    parser.add_argument(
        "--publish", help="pub/sub topic에 메시지를 발행할지 여부를 선택하세요.", action="store_true"
    )
    args = parser.parse_args()

    PUBLISH = args.publish
    publish_twitter_stream(args.keywords)
