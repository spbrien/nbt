import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from textblob import TextBlob

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import text 

import plotly.express as px



def all_line_plot(df, title):
    # Base Plot
    fig, ax = plt.subplots(figsize = (20,10))
    l = sns.lineplot(ax=ax, data=df, x='date', y='value', hue='station')
    
    # Plot Mean
    # l.axhline(df['value'].mean(), color='orange', label='mean')
    
    # Label
    # l.text(
    #     x = l.get_xbound()[0]+.1, # y-coordinate 
    #     y = df['value'].mean(),
    #     s = 'Mean: %s' % round(df['value'].mean(), 2), # data label
    #     color = 'orange'
    # )
    
     # Quantiles
    a = df['value'].quantile(.25)
    b = df['value'].quantile(.50)
    c = df['value'].quantile(.75)
    
    bounds = [l.get_xbound()[0], l.get_xbound()[1]]
    y_upper_bound = l.get_ybound()[1]
    
    l.fill_between(
        bounds, 
        y1=0,
        y2=a,
        alpha=0.1, 
        facecolor='gray'
    )
    l.fill_between(
        bounds, 
        y1=a,
        y2=b,
        alpha=0.02, 
        facecolor='gray'
    )
    l.fill_between(
        bounds, 
        y1=b,
        y2=c,
        alpha=0.1, 
        facecolor='gray'
    )
    l.fill_between(
        bounds, 
        y1=c,
        y2=y_upper_bound,
        alpha=0.02, 
        facecolor='gray'
    )
    
    # Labels
    ax.set_title("Coverage of %s Over Time" % title, size = 17, pad = 10)
    ax.set_xlabel("Date of Coverage")
    ax.set_ylabel("Percentage of Airtime")
    
    l.margins(0)
    l.legend(title="Networks", labels=["CNN", "FOX News", "MSNBC"])
    
    return l


def line_plot(df, station):
    # Base Plot
    fig, ax = plt.subplots(figsize = (20,10))
    l = sns.lineplot(ax=ax, data=df, x='date', y='value')
    
    # Plot Mean
    l.axhline(df['value'].mean(), color='orange', label='mean')
    
    # Label
    l.text(
        x = l.get_xbound()[0]+.1, # y-coordinate 
        y = df['value'].mean()+.1,
        s = 'Mean: %s' % round(df['value'].mean(), 2), # data label
        color = 'orange'
    )
    
    # Quantiles
    a = df['value'].quantile(.25)
    b = df['value'].quantile(.50)
    c = df['value'].quantile(.75)
    
    bounds = [l.get_xbound()[0], l.get_xbound()[1]]
    y_upper_bound = l.get_ybound()[1]
    
    l.fill_between(
        bounds, 
        y1=0,
        y2=a,
        alpha=0.1, 
        facecolor='gray'
    )
    l.fill_between(
        bounds, 
        y1=a,
        y2=b,
        alpha=0.02, 
        facecolor='gray'
    )
    l.fill_between(
        bounds, 
        y1=b,
        y2=c,
        alpha=0.1, 
        facecolor='gray'
    )
    l.fill_between(
        bounds, 
        y1=c,
        y2=y_upper_bound,
        alpha=0.02, 
        facecolor='gray'
    )
    
    # Labels
    ax.set_title("Coverage of Over Time", size = 17, pad = 10)
    ax.set_xlabel("Date of Coverage")
    ax.set_ylabel("Percentage of Airtime")
    
    l.margins(0)
    l.legend(title="Networks", labels=[station])
    
    return l

def hist_plot(df, station, scale=1):
    fig, ax = plt.subplots(figsize = (10,7))
    value = df['value']
    value.plot(kind = "hist", density = True, alpha = 0.55, bins = 15)
    value.plot(kind = "kde")

    ax.set_xlim(0, scale)
    ax.set_xlabel("Volume of Airtime as a % of Total Airtime")
    ax.set_title("Airtime on %s" % station, size = 17, pad = 10)

    ax.set_yticks([])
    ax.set_ylabel("Frequency of a Specific Volume % Occurrence")

    (
        quant_50, 
        quant_75, 
        quant_95
    ) = (
        value.quantile(0.5), 
        value.quantile(0.75), 
        value.quantile(0.95)
    )

    quants = [
        [quant_50, 1, .80],  
        [quant_75, 1, .80], 
        [quant_95, 1, .80]
    ]

    for i in quants:
        ax.axvline(i[0], alpha = i[1], ymax = i[2], linestyle = ":")

    ax.text(quant_50, ax.get_ybound()[1]*.8, "50th", size = 12)
    ax.text(quant_75, ax.get_ybound()[1]*.8, "75th", size = 12)
    ax.text(quant_95, ax.get_ybound()[1]*.8, "95th Percentile", size = 12)

    return ax

def sentiment(df):
    s = df.copy()
    s['full_text'] = s[['show', 'date','show_date','snippet']].groupby(['show','show_date'])['snippet'].transform(lambda x: ','.join(x))
    s = s.drop_duplicates(subset='full_text')
    s['polarity'] = s['full_text'].map(lambda text: TextBlob(text).sentiment.polarity)
    return s[['show', 'date', 'show_date', 'full_text', 'polarity']]

def get_top_n_words(
    corpus, 
    n=20, 
    remove_stopwords=False,
    additional_stopwords=[],
):
    vec = None
    if remove_stopwords:
        stop_words = text.ENGLISH_STOP_WORDS.union(additional_stopwords)
        vec = CountVectorizer(stop_words=stop_words).fit(corpus)
    else:
        vec = CountVectorizer().fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0) 
    words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True)
    return words_freq[:n]

def get_top_n_bigram(
    corpus, 
    n=20, 
    remove_stopwords=False,
    additional_stopwords=[],
):
    if remove_stopwords:
        stop_words = text.ENGLISH_STOP_WORDS.union(additional_stopwords)
        vec = CountVectorizer(stop_words=stop_words, ngram_range=(2, 2)).fit(corpus)
    else:
        vec = CountVectorizer(ngram_range=(2, 2)).fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0) 
    words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True)
    return words_freq[:n]

def get_top_n_trigram(
    corpus, 
    n=20, 
    remove_stopwords=False,
    additional_stopwords=[],
):
    vec = None
    if remove_stopwords:
        stop_words = text.ENGLISH_STOP_WORDS.union(additional_stopwords)
        vec = CountVectorizer(stop_words=stop_words, ngram_range=(3, 3)).fit(corpus)
    else:
        vec = CountVectorizer(ngram_range=(3, 3)).fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0) 
    words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True)
    return words_freq[:n]

def get_top_n_ngram(
    corpus, 
    n=20, 
    ngram=4, 
    remove_stopwords=False,
    additional_stopwords=[],
):
    vec = None
    if remove_stopwords:
        stop_words = text.ENGLISH_STOP_WORDS.union(additional_stopwords)
        vec = CountVectorizer(stop_words=stop_words, ngram_range=(ngram, ngram)).fit(corpus)
    else:
        vec = CountVectorizer(ngram_range=(ngram, ngram)).fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0) 
    words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True)
    return words_freq[:n]

def graph_top_words(common_words, title=None):
    df = pd.DataFrame(common_words, columns = ['full_text' , 'frequency'])
    df = df.groupby('full_text').sum()['frequency'].sort_values(ascending=False).reset_index()
    fig = px.bar(
        df, 
        title=title,
        x="full_text", 
        y="frequency", 
        height=800, 
        width=1000,
        labels={
            "full_text": "Phrase",
            "frequency": "Frequency"
        }
    )
    fig.show()