from collections import Counter

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.data.find("sentiment/vader_lexicon.zip")

class Sentiment:
    def __init__(self,text):
        self.text = text
    def sentiment(self):
        score = SentimentIntensityAnalyzer().polarity_scores(self.text)
        if score["compound"] >= 0.5:
            return "positive"
        elif score["compound"] <= -0.5:
            return "negative"
        else:
            return "neutral"



