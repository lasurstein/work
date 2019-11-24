from pymongo import MongoClient, DESCENDING
from dateutil import parser
from datetime import datetime
from pytz import timezone
import os, sys, MeCab, json, re, glob

def setup_mecab():
  mecab = MeCab.Tagger('-d /now18/a.saito/local/mecab/lib/mecab/dic/mecab-ipadic-neologd')
  mecab.parse('')
  print('mecab ready')
  return mecab

def setup_mongo():
  connection = MongoClient()
  db = connection['1120_2014_sakura_twi']
  print('mongoDB ready')
  return db

def morpho_text(text, mecab):
  morpho_text = []
  node = mecab.parseToNode(text).next
  while node.next:
    feature = node.feature.split(',')[0]
    if feature in ['名詞', '動詞']:
      if node.feature.split(",")[6] == '*':
        word = node.surface
      else:
        word = node.feature.split(",")[6]
      morpho_text.append(word)
    node = node.next
  morpho_text = list(set(morpho_text))
  return morpho_text

def text_cleaning(s):
  s = re.sub('@[A-Za-z0-9_]* ', '', s)
  s = re.sub('[\“`\"\'()\n]*', '', s)
  s = re.sub('https?://[A-Za-z0-9./]*', '', s)
  return s

def main():
  sakura = re.compile("桜|さくら|サクラ")
  mecab = setup_mecab()
  db = setup_mongo()
  month = [str(s).zfill(2) for s in range(2,6)]
  for m in month:
    col = db['twi_2014' + str(m)]
    for filename in glob.glob('/now18/a.saito/data_2014/2014-' + str(m) + '/*.txt'):
      with open(filename, 'r') as f:
        print('##### insert ' + filename + ' ...')
        line = f.readline()
        while line:
          jsonline = re.sub('^\d*\t', '', line)
          try:
            textline = json.loads(jsonline)
            text = text_cleaning(textline['text'])
            if sakura.search(text):
              created_at = textline['created_at']
              insert_line = {
                'pname' : textline['reverse_geo']['pname'],
                'text' : text,
                'morpho_text' : ' '.join(morpho_text(text, mecab)),
                'created_at': created_at,
                'created_at_iso' : parser.parse(created_at).astimezone(timezone('Asia/Tokyo')).isoformat()
              }
              col.insert_one(insert_line)
          except Exception:
            pass
          line = f.readline()

main()
