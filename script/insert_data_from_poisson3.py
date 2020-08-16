from pymongo import MongoClient, DESCENDING
from dateutil import parser
from datetime import datetime
from pytz import timezone
import os, sys, MeCab, json, re, glob
from normalizer import twitter_normalizer, whitespace_normalizer
import logging


sakura = set(['桜', 'さくら', 'サクラ'])

logging.basicConfig(filename='/now24/a.saito/work/script/log/0708_2015_insert.log', level=logging.DEBUG)

def setup_mecab():
  mecab = MeCab.Tagger('-d /now24/a.saito/local/mecab/lib/mecab/dic/mecab-ipadic-neologd')
  mecab.parse('')
  logging.info('mecab ready')
  return mecab

def setup_mongo():
  connection = MongoClient()
  db = connection['tweet2015']
  logging.info('mongoDB ready: {}'.format('tweet2014'))
  return db

def text_morphological(text, mecab):
  morpho_text = []
  node = mecab.parseToNode(text).next
  while node.next:
    feature = node.feature.split(',')[0]
    if node.feature.split(",")[6] == '*':
      word = node.surface
    else:
      word = node.feature.split(",")[6]
    morpho_text.append(word + '/' + feature)
    node = node.next
  morpho_text = list(set(morpho_text))
  return morpho_text

def text_cleaning(s):
  # r_men = re.compile("\@[a-z0-9\_]+:?")
  r_kigou = re.compile('["#$%&\'\\\\()*+,-.\/:;<=>@[\\]^_`{|}~「」〔〕“”〈〉『』【】＆＊・（）＄＃＠。、｀＋￥％]+')
  # r_url = re.compile("https?://[-_.!~*\'()a-z0-9;/?:@&=+$,%#]+")
  # r_partialurl = re.compile("(((https|http)(.{1,3})?)|(htt|ht))$")
  r_space = re.compile("[\s]+")
  r_emoji = re.compile('(\u3000)')
  # s = r_men.sub('', s)
  # s = r_url.sub('', s)
  s = twitter_normalizer(s)
  s = r_space.sub(" ", s)
  s = r_emoji.sub(" ", s)
  s = r_kigou.sub(" ", s)
  s = whitespace_normalizer(s)
  return s

def insert(line, p, c, mecab):
  jsonline = re.sub('^\d*\t', '', line)
  try:
    textline = json.loads(jsonline)
    pname = textline['reverse_geo']['pname']
    if pname == p:
      text = textline['text']
      cleaning_text = text_cleaning(text)
      morpho_text = text_morphological(cleaning_text, mecab)
      created_at = textline['created_at']
      sakura_twi = 1 if (len(sakura & set(morpho_text)) > 0) else 0
      insert_line = {
        'pname' : pname,
        'text' : text,
        'cleaning_text' : cleaning_text,
        'morpho_text' : ' '.join(morpho_text),
        'sakura_twi' : sakura_twi,
        'created_at': created_at,
        'created_at_iso' : parser.parse(created_at).astimezone(timezone('Asia/Tokyo')).isoformat()
      }
      c.insert_one(insert_line)
      # logging.debug(insert_line)
  except Exception as e:
    logging.warning(e)
    logging.warning(jsonline)
  return

def main():
  mecab = setup_mecab()


  d = {
    'hk': {
      'pname': '北海道',
      # 'file_lst': ['0429', '0430', '0501']
    },
    'is': {
      'pname': '石川県',
      # 'file_lst': ['0401', '0402', '0403', '0404', '0405', '0406', '0407']
    },
    # 'tk': {
    #   'pname': '東京都',
    #   # 'file_lst': ['0325', '0326', '0327', '0328', '0329', '0330']
    # }
  }

  # db = setup_mongo()
  # for key in d:
  #   month = [str(s).zfill(2) for s in range(1,13)]
  #   col = db[key + '2014']
  #   for m in month:
  #     logging.info('##### insert 2014-{0}/{1}'.format(m, key))
  #     for filename in glob.glob('/poisson3/backup/now09/shimozono/2018/experiment/data_2014/2014-' + str(m) + '/*.txt'):
  #       insert_lines = 0
  #       with open(filename, 'r') as f:
  #         line = f.readline()
  #         while line:
  #           insert(line, d[key]['pname'], col, mecab)
  #           insert_lines = insert_lines + 1
  #           line = f.readline()
  #         logging.info('\tinserted {} lines.'.format(insert_lines + 1))

  db = setup_mongo()
  for key in d:
    month = [str(s).zfill(2) for s in range(1,13)]
    col = db[key + '2015']
    for m in month:
      logging.info('##### insert 2015-{0}/{1}'.format(m, key))
      for filename in glob.glob('/now24/a.saito/data/2015-' + str(m) + '/*.txt'):
        with open(filename, 'r') as f:
          line = f.readline()
          while line:
            insert(line, d[key]['pname'], col, mecab)
            line = f.readline()
          logging.info(f'\tinserted {len(f.readlines())} lines.: {filename}')

main()
