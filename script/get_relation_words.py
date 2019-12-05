from pymongo import MongoClient, DESCENDING
import os, sys, json, re, glob
import collections, math

def setup_mongo():
  connection = MongoClient()
  db = connection['2014_sakura_twi']
  print('mongoDB ready')
  return db

def calc_pmi(sw, w, s, N):
  pmi = math.log2(((sw + 1) * N) / (w * s))
  return(pmi)

def calc_soa(sw, ns_w, s, ns):
  soa = math.log2(((sw + 1) * ns)/((ns_w + 1) * s))
  return(soa)

def main():
  args = sys.argv

  db = setup_mongo()
  pnames = ['hk', 'is', 'tk']
  for pname in pnames:
    col = db['season_' + pname]
    s_twis = list(col.find({'sakura_twi': 1}))
    ns_twis = list(col.find({'sakura_twi': 0}))

    words = []
    ns_words = {}
    for s_twi in s_twis:
      words.extend(s_twi['morpho_text'].split(' '))
    uwords = list(set(words))
    for ns_twi in ns_twis:
      morpho = ns_twi['morpho_text'].split(' ')
      for w in uwords:
        if w in morpho:
          ns_words[w] = ns_words[w] + 1 if w in ns_words else 1

    sw_count = collections.Counter(words)
    s = len(s_twis)
    ns = len(ns_twis)
    N = s + ns

    if args[1] == 'pmi':
      pmi_of_word = {}
      for w, c in sw_count.items():
        sw = c
        ns_w = ns_words[w] if w in ns_words else 0
        wc = sw + ns_w
        if not any([wc < 1, s < 1]):
          pmi_of_word[w] = calc_pmi(sw, wc, s, N)
      sorted_pmi = sorted(pmi_of_word.items(), key=lambda x:x[1], reverse=True)
      with open('/now24/a.saito/work/result/1204/' + pname + '_pmi.txt', 'w') as f:
        for w, pmi in sorted_pmi:
          f.write(f'{w}, {pmi}\n')

    elif args[1] == 'soa':
      soa_of_word = {}
      for w, c in sw_count.items():
        sw = c
        ns_w = ns_words[w] if w in ns_words else 0
        if not s < 1:
          soa_of_word[w] = calc_soa(sw, ns_w, s, ns)
      sorted_soa = sorted(soa_of_word.items(), key=lambda x:x[1], reverse=True)
      with open('/now24/a.saito/work/result/1204/' + pname + '_soa.txt', 'w') as f:
        for w, soa in sorted_soa:
          f.write(f'{w}, {soa}\n')
    else:
      print('Prease specify pmi or soa.')
main()
