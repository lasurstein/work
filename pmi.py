from pymongo import MongoClient, DESCENDING
import os, sys, json, re, glob
import collections, math

def setup_mongo():
  connection = MongoClient()
  db = connection['1120_2014_sakura_twi']
  print('mongoDB ready')
  return db

def main():
  db = setup_mongo()
  pnames = ['hk', 'is', 'tk']
  for pname in pnames:
    col = db['twi_syun_2014_' + pname]
    sakura_tweets = list(col.find({'sakura_twi': 1}))
    words = []
    non_sakura_words = {}
    non_sakura_tweets = list(col.find({'sakura_twi': 0}))
    for stwi in sakura_tweets:
      words.extend(stwi['morpho_text'].split(' '))
    uwords = list(set(words))
    for w in uwords:
      count = 0
      for ntwi in non_sakura_tweets:
        morpho = ntwi['morpho_text'].split(' ')
        count = count + 1 if (w in morpho) else count
      non_sakura_words[w] = count
    sakura_word_count = collections.Counter(uwords)
    
    s_count = len(sakura_tweets)
    ns_count = len(non_sakura_tweets)
    a_count = s_count + ns_count
    pmi_of_word = {}
    print(type(sakura_word_count))
    for w, c in sakura_word_count.items():
      s_w_count = c
      ns_w_count = non_sakura_words[w]
      w_count = s_w_count + ns_w_count
      if not any([w_count < 1, s_count < 1]):
        pmi = math.log2(((s_w_count + 1) * a_count) / (w_count * s_count))
        pmi_of_word[w] = pmi
        print(pmi)
    sorted_pmi = sorted(pmi_of_word.items(), key=lambda x:x[1], reverse=True)
    print('###        ' + pname)
    with open('/now18/a.saito/work/result/1121_last_' + pname + '_pmi.txt', 'w') as f:
      for w, pmi in sorted_pmi:
        f.write(f'{w}, {pmi}\n')
main()
