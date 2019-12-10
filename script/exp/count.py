from pymongo import MongoClient, DESCENDING
import pandas as pd
from datetime import datetime, timedelta

result_dir = '/now24/a.saito/work/result/1208/'
pname = '北海道'

def setup_mongo(dbname):
  connection = MongoClient()
  db = connection[dbname]
  print('mongoDB ready')
  return db

def daterange(start, end):
  for n in range((end - start).days):
    yield start + timedelta(n)

def get_relation_words(filename, rate):
  with open(filename, 'r') as f:
    df = pd.read_csv(f, header=None, names=['word', 'aso'])
    aso_bool = df['aso'] > 0
    count = aso_bool.sum()
    num = int(round(count * (rate / 100)))
    limit = df.iat[num - 1, 1]
    word_lst = []
    word_lst = df[df.aso >= limit].word.values.tolist()
    print('file: ' + filename + ', rate: ' + str(rate) + '\n limit: ' + str(limit) + ', words_count: ' + str(len(word_lst)))
  return word_lst

def count_all(db, start, end):
  atwi = []
  print('hk_all_count')
  for date in daterange(start, end):  
    today = date.isoformat()
    nday = (date + timedelta(days=1)).isoformat()
    atwi_pipe = {
      'created_at_iso': {
          '$gte': today,
          '$lt': nday
      }
    }
    m = str(date.month).zfill(2)
    d = str(date.day).zfill(2)
    col = db['2015-' + m]
    atwi_count = col.find(atwi_pipe).count()
    atwi.append('\t'.join([m, d, str(atwi_count)]))

  atwi_path = result_dir + '/count/hk_all.txt'
  with open(atwi_path, 'w') as af:
    af.write('\n'.join(atwi))


def main():
  db = setup_mongo('2015_hk_twi_1208')
  start = datetime.strptime('20150217', '%Y%m%d')
  end = datetime.strptime('20151231', '%Y%m%d')

  rates = range(10, 101, 10)
  for ext in ['pmi', 'soa']:
    print('### ' + ext)
    filename = result_dir + 'hk_' + ext + '.txt'
    for rate in rates:
      print('###\t' + str(rate))
      words = set(get_relation_words(filename, rate))
      stwi = []
      for date in daterange(start, end):  
        today = date.isoformat()
        nday = (date + timedelta(days=1)).isoformat()
        stwi_pipe = {
            'sakura_twi': 1,
            'created_at_iso': {
              '$gte': today,
              '$lt': nday
            }
        }
        m = str(date.month).zfill(2)
        d = str(date.day).zfill(2)
        col = db['2015-' + m]

        stwis = col.find(stwi_pipe)
        stwi_count = 0
        for twi in stwis:
          if len(set(twi['morpho_text'].split()) & words) > 0:
            stwi_count = stwi_count + 1
        stwi.append('\t'.join([m, d, str(stwi_count)]))

      stwi_path = result_dir + '/count/hk_s_' + ext + '_' + str(rate) + '.txt'
      with open(stwi_path, 'w') as sf:
        sf.write('\n'.join(stwi))


main()
