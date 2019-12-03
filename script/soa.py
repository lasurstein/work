from pymongo import MongoClient, DESCENDING
import os, sys, json, re, glob
import collections, math

def setup_mongo():
  connection = MongoClient()
  db = connection['1120_2014_sakura_twi']
  print('mongoDB ready')
  return db

def pmi(sw, w, s, N):
  pmi = math.log2(((sw + 1) * N) / (w * s))
  return(pmi)

def soa(ns_w, sw, w, s, N):
  pmi = pmi(sw, w, s, N)
  nw_pmi = pmi(sw, N - w, s, N)
  soa = pmi - nw_pmi
  return(soa)

def main():
  db = setup_mongo()
  pnames = ['hk', 'is', 'tk']
  for pname in pnames:
    col = db['twi_syun_2014_' + pname]
    s_twis = list(col.find({'sakura_twi': 1}))
    ns_twis = list(col.find({'sakura_twi': 0}))

    words = []
    non_sakura_words = {}
    for s_twi in s_twis:
      words.extend(stwi['morpho_text'].split(' '))
    uwords = list(set(words))
    for ns_twi in ns_twis:
      morpho = nstwi['morpho_text'].split(' ')
      for w in uwords:
        if w in morpho:
          ns_words[w] = ns_words[w] + 1 if w in ns_word else 1

    sw_cnt = collections.Counter(words)
    s_cnt = len(s_twis)
    ns_cnt = len(ns_twis)
    a_cnt = s_cnt + ns_cnt

    pmi_of_word = {}
    soa_of_word = {}
    for w, c in sw_cnt.items():
      sw_cnt = c
      ns_w_cnt = ns_words[w]
      w_cnt = s_w_cnt + ns_w_cnt
      if not any([w_cnt < 1, s_cnt < 1]):
        soa_of_word[w] = soa(sw_cnt , w_cnt, s_cnt, a_cnt)
    sorted_soa = sorted(soa_of_word.items(), key=lambda x:x[1], reverse=True)
    
    with open('/now18/a.saito/work/result/' + pname + '_soa.txt', 'w') as f:
      for w, soa in sorted_soa:
        f.write(f'{w}, {soa}\n')

main()
