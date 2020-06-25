from pymongo import MongoClient, DESCENDING
from dateutil import parser
from datetime import datetime
from pytz import timezone
import os, sys, MeCab, json, re, glob

r_men = re.compile('@(\w)+\s')
r_kigou = re.compile('[!"#$%&\'\\\\()*+,-./:;<=>?@[\\]^_`{|}~「」〔〕“”〈〉『』【】＆＊・（）＄＃＠。、？！｀＋￥％]')
r_url = re.compile('http(s)?://([-\w]+\.)+[-\w]+(/[- ./?%\w&=]*)?')

sakura = set(['桜', 'さくら', 'サクラ'])

def setup_mecab():
  mecab = MeCab.Tagger('-d /now24/a.saito/local/mecab/lib/mecab/dic/mecab-ipadic-neologd')
  mecab.parse('')
  print('mecab ready')
  return mecab

def setup_mongo():
  connection = MongoClient()
  db = connection['2015_tk_twi']
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
  s = r_men.sub('', s)
  s = r_url.sub('', s)
  s = r_kigou.sub('', s)
  return s

def insert(line, p, c, mecab):
  jsonline = re.sub('^\d*\t', '', line)
  try:
    textline = json.loads(jsonline)
    pname = textline['reverse_geo']['pname']
    if pname == p:
      text = text_cleaning(textline['text'])
      morpho = morpho_text(text, mecab)
      created_at = textline['created_at']
      sakura_twi = 1 if (len(sakura & set(morpho)) > 0) else 0
      insert_line = {
        'pname' : pname,
        'text' : text,
        'morpho_text' : ' '.join(morpho),
        'sakura_twi' : sakura_twi,
        'created_at': created_at,
        'created_at_iso' : parser.parse(created_at).astimezone(timezone('Asia/Tokyo')).isoformat()
      }
      c.insert_one(insert_line)
  except Exception:
    pass
  return

def main():
  mecab = setup_mecab()
  db = setup_mongo()

  month = [str(s).zfill(2) for s in range(2,13)]
  for m in month:
    col = db['2015-' + m]
    for filename in glob.glob('/now24/a.saito/data/2015-' + str(m) + '/*.txt'):
      with open(filename, 'r') as f:
        print('##### insert ' + filename + ' ...')
        line = f.readline()
        while line:
          insert(line, '東京都', col, mecab)
          line = f.readline()

main()
