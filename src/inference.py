import nltk
import numpy as np
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download("vader_lexicon")


def predict_sentiment(text):
    sid = SentimentIntensityAnalyzer()
    ss = sid.polarity_scores(text)
    positive, neutral, negative = ss["pos"], ss["neu"], ss["neg"]
    pred_cls = np.argmax([positive, neutral, negative])
    return pred_cls, positive, neutral, negative
