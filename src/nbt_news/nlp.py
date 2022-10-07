import re
import os
import hashlib
import nltk
import s3fs
import pandas as pd
import numpy as np

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.text import Text

from textblob import TextBlob
from sklearn.feature_extraction.text import CountVectorizer


# Transforms contractions into corresponding long-form phrases
def handle_contractions(sent):
    contractions_dict = { "ain't": "are not","'s":" is","aren't": "are not",
                     "can't": "cannot","can't've": "cannot have",
                     "'cause": "because","could've": "could have","couldn't": "could not",
                     "couldn't've": "could not have", "didn't": "did not","doesn't": "does not",
                     "don't": "do not","hadn't": "had not","hadn't've": "had not have",
                     "hasn't": "has not","haven't": "have not","he'd": "he would",
                     "he'd've": "he would have","he'll": "he will", "he'll've": "he will have",
                     "how'd": "how did","how'd'y": "how do you","how'll": "how will",
                     "I'd": "I would", "I'd've": "I would have","I'll": "I will",
                     "I'll've": "I will have","I'm": "I am","I've": "I have", "isn't": "is not",
                     "it'd": "it would","it'd've": "it would have","it'll": "it will",
                     "it'll've": "it will have", "let's": "let us","ma'am": "madam",
                     "mayn't": "may not","might've": "might have","mightn't": "might not", 
                     "mightn't've": "might not have","must've": "must have","mustn't": "must not",
                     "mustn't've": "must not have", "needn't": "need not",
                     "needn't've": "need not have","o'clock": "of the clock","oughtn't": "ought not",
                     "oughtn't've": "ought not have","shan't": "shall not","sha'n't": "shall not",
                     "shan't've": "shall not have","she'd": "she would","she'd've": "she would have",
                     "she'll": "she will", "she'll've": "she will have","should've": "should have",
                     "shouldn't": "should not", "shouldn't've": "should not have","so've": "so have",
                     "that'd": "that would","that'd've": "that would have", "there'd": "there would",
                     "there'd've": "there would have", "they'd": "they would",
                     "they'd've": "they would have","they'll": "they will",
                     "they'll've": "they will have", "they're": "they are","they've": "they have",
                     "to've": "to have","wasn't": "was not","we'd": "we would",
                     "we'd've": "we would have","we'll": "we will","we'll've": "we will have",
                     "we're": "we are","we've": "we have", "weren't": "were not","what'll": "what will",
                     "what'll've": "what will have","what're": "what are", "what've": "what have",
                     "when've": "when have","where'd": "where did", "where've": "where have",
                     "who'll": "who will","who'll've": "who will have","who've": "who have",
                     "why've": "why have","will've": "will have","won't": "will not",
                     "won't've": "will not have", "would've": "would have","wouldn't": "would not",
                     "wouldn't've": "would not have","y'all": "you all", "y'all'd": "you all would",
                     "y'all'd've": "you all would have","y'all're": "you all are",
                     "y'all've": "you all have", "you'd": "you would","you'd've": "you would have",
                     "you'll": "you will","you'll've": "you will have", "you're": "you are",
                     "you've": "you have"}

    contractions_re = re.compile('(%s)' % '|'.join(contractions_dict.keys()))

    def replace(match):
        return contractions_dict[match.group(0)]

    return contractions_re.sub(replace, sent)


# Creates lemmatization based on the part of speech of each word
def lemma_per_pos(sent):
    t = TextBlob(sent)
    t_dict = {
        "J": 'a',
        "N": 'n',
        "V": 'v',
        "R": 'r'
    }
    w_n_t = [(w, t_dict.get(p[0], 'n')) for w, p in t.tags]
    lemmatized_list = [w.lemmatize(t) for w, t in w_n_t]
    return " ".join(lemmatized_list)


html_cleaner = re.compile('<.*?>')


def clean_html(text):
    cleantext = re.sub(html_cleaner, '', text)
    return cleantext


# Clean text, removing punctuation, stop words, numbers, etc.
def preprocess_text(df, extra_stopwords=[]):
    stop = list(set(stopwords.words('english')))
    stop.extend(extra_stopwords)

    def cleaner(content):
        content = content.lower()
        content = handle_contractions(content)
        content = clean_html(content)
        content = re.sub(r'\d+', '', content)
        word_tokens = word_tokenize(content)
        filtered_sentence = [w for w in word_tokens if not w.lower() in stop]
        filtered_sentence = [
            word for word in filtered_sentence if word.isalnum()
        ]
        sent = ' '.join(filtered_sentence)
        sent = lemma_per_pos(sent)
        return sent

    df['clean_1'] = df['text'].apply(cleaner)
    return df


# Add some data to each row: word counts, length, average word length
def add_word_counts(df):
    df['length'] = df['text'].str.len()
    df['word_count'] = df['text'].str.split().map(lambda x: len(x))
    df['avg_word_length'] = df['text'].str.split() \
        .apply(lambda x: [len(i) for i in x]) \
        .map(lambda x: np.mean(x))
    return df


# Get words that appear in greater than 5% of statements but not more than 95%
def process_word_counts(df):
    corpus = df['clean_1']
    count_vect = CountVectorizer(
        max_df=.95,
        min_df=.05
    )
    count_vect.fit_transform(corpus)
    return count_vect.get_feature_names_out()


# Remove words that aren't in the list created by the last function
def clean_by_word_count(df):
    keepers = process_word_counts(df)
    df['clean_2'] = df['clean_1'].str.split() \
        .apply(lambda x: [
            word.lower() for word in x if word.lower() in keepers
        ]) \
        .apply(lambda x: ' '.join(x))
    return df


# Add sentiment score
def add_polarity(df):
    df['polarity'] = df.clean_2.apply(lambda x: TextBlob(x).sentiment.polarity)
    return df


# run all of the above functions and return a DataFrame
def pipeline(df, extra_stopwords=[]):
    df = df.rename(columns={"snippet":"text"})
    df = preprocess_text(df, extra_stopwords)
    df = add_word_counts(df)
    df = clean_by_word_count(df)
    df = add_polarity(df)
    return df

