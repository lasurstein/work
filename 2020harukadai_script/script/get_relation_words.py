"""
教師データのツイート内に含まれる各単語と
'桜'の関連度を計算する
"""

from pymongo import MongoClient, DESCENDING
from .s_lib import setup_mongo
import sys, math, collection



def setup_mongo(db_name):
    connection = MongoClient()
    db = connection[db_name]
    print('mongoDB ready')
    return db


def calc_pmi(sw, w, s, n):
  pmi = math.log2(((sw + 1) * N) / (w * s))
  return(pmi)


def calc_soa(sw, ns_w, w, s, ns, n):
    #  soa = math.log2(((sw + 1) * ns)/((ns_w + 1) * s))
    soa = calc_pmi(sw, w, s, N) - calc_pmi(ns_w, w, ns, N)
    return(soa)


def main():
    args = sys.argv

    db = setup_mongo(db_name)
    p_name = '東京都'
    p = 'tk'

    collection = db['season_' + p_name]
    sakura_tweets = list(collection.find({'sakura_twi': 1}))
    non_sakura_tweets = list(collection.find({'sakura_twi': 0}))

    words = []
    # '桜'が含まれているツイート内の単語をwordsに格納
    for sakura_tweet in sakura_tweets:
        words.extend(sakura_tweet['morpho_text'].split(' '))
    unique_words = list(set(words))
    non_sakura_words = {}
    # '桜'が含まれていないツイートにwords内の単語が含まれていた場合、non_sakura_wordsに記録
    for non_sakura_tweet in non_sakura_tweets:
        morpho = non_sakura_tweet['morpho_text'].split(' ')
        for word in unique_words:
            if word in morpho:
                non_sakura_words[w] = non_sakura_words[w] + 1 if w in ns_words else 1

    sakura_word_count = collectionlections.Counter(words)
    sakura_tweet_count = len(sakura_tweets)
    non_sakura_tweet_count = len(non_sakura_tweets)
    n = sakura + non_sakura

    soa_of_word = {}
    for w, count in sakura_word_count.items():
        if w in ['桜', 'さくら', 'サクラ']:
            continue
        sakura_w_count = c
        non_sakura_w_count = ns_words[w] if w in ns_words else 0
        w_count = sakura_w_count + non_sakura_w_count
        if not sakura_count < 1:
            soa_of_word[w] = calc_soa(
                sakura_w_count, non_sakura_w_count, w_count, sakura_count, non_sakura_count, n)
    sorted_soa = sorted(soa_of_word.items(), key=lambda x: x[1], reverse=True)
    with open(f'/now24/a.saito/work/result/{p}.txt', 'w') as f:
        for w, soa in sorted_soa:
            f.write(f'{w}, {soa}\n')

    # pmi_of_word = {}
    # for w, c in sw_count.items():
    #   if w in ['桜', 'さくら', 'サクラ']:
    #     continue
    #   sw = c
    #   ns_w = ns_words[w] if w in ns_words else 0
    #   wc = sw + ns_w
    #   if not any([wc < 1, s < 1]):
    #     pmi_of_word[w] = calc_pmi(sw, wc, s, N)
    # sorted_pmi = sorted(pmi_of_word.items(), key=lambda x:x[1], reverse=True)

    # with open('/now24/a.saito/work/result/' + p_name + '_pmi.txt', 'w') as f:
    #   for w, pmi in sorted_pmi:
    #     f.write(f'{w}, {pmi}\n')

main()
