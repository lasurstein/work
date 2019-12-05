from pymongo import MongoClient, DESCENDING
import pandas as pd
from datetime import datetime, timedelta

resurt_dir = '/now24/a.saito/work/resurt/1205/'

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
    df = pd.read_csv(filename, header=None, names=['word', 'aso'])
    count = df[df.aso > 0].sum()
    num = round(count * (rate / 100))
    limit = df[num,1]
    words = []
    words = df[df.aso > limit].values.tolist()
    print('file: ' + filename + ', rate: ' + str(rate) + '\t limit: ' + str(limit) + 'words_count: ' + len(words))
  return words

def main():
  db = setup_mongo('2015_hk_twi')
  start = datetime.strptime('20150217', '%Y%m%d')
  end = datetime.strptime('20151231', '%Y%m%d')

  for date in daterange(start, end):  
    today = 'ISODate(' + isoformat(date) + ')'
    nday = 'ISODate(' + isoformat(date + timedelta(1)) + ')'
    atwi_pipe = {
      'created_at_iso': {
          '$gte': today,
          '$lt': nday
      }
    }
    m = date.Month
    d = date.Day
    col = db['2015-' + m]
    atwi_count = col.find(atwi_pipe).count()
    atwi.append('\t'.join([m, d, atwi_count]))

  atwi_path = result_dict + 'hk_all.txt'
  with open(atwi_path, 'w') as af:
    af.write('\n'.join(atwi))

  stwis = col.aggregate(stwi_pipe, allowDiskUse=True)
  rates = range(10, 101, 10)    
  for ext in ['pmi', 'soa']:
    print('###\t' + ext)
    filename = resurt_dir + '1204/hk_' + ext + '.txt'
    for rate in rates:
      print('###\t\t' + str(rate))   
      for date in daterange(start, end):  
        today = 'ISODate(' + isoformat(date) + ')'
        nday = 'ISODate(' + isoformat(date + timedelta(1)) + ')'
        stwi_pipe = [
          {
            '$match': {
              'reverse_geo': {'pname': pname},
              'sakura_twi': 1,
              'created_at_iso': {
                '$gte': today,
                '$lt': nday
              }
            }
          }
        ]
        m = date.Month
        d = date.Day
        col = db['2015-' + m]
        
        stwi = []
        stwi_count = 0
        words = set(get_relation_words(filename, rate))
        for twi in stwis:
          if len(set(twi['morpho_text']) & words) > 0:
            stwi_count = stwi_count + 1
          stwi.append('\t'.join([m, d, stwi_count]))

        stwi_path = resurt_dir + '1205/hk_s_' + ext + '_' + str(rate) + '.txt'
        with open(stwi_path, 'w') as sf:
          sf.write('\n'.join(stwi))


main()
