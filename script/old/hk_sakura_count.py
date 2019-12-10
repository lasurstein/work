from pymongo import MongoClient, DESCENDING
import os, sys, json, re, glob

def setup_mongo():
  connection = MongoClient()
  db = connection['2015_twi_asaito']
  print('mongoDB ready')
  return db

def setup_pipe():
  pipe = [
    {
      '$match': {
        'text': {'$regex' : '桜|さくら|サクラ'}
      }
    },
    {
      '$project': {
        'text' : '$text',
        'created_at': {'$add': [{'$toDate': '$created_at'}, 9 * 60 * 60 * 1000]}
      }
    },
    {
      '$group': {
        '_id': {
          'day': {'$dayOfMonth': '$created_at'},
        },
        'count': {'$sum': 1}
      }
    },
    {
      '$sort': {
        '_id': 1
      } 
    }
  ]
  return pipe

def main():
  db = setup_mongo()
  pipe = setup_pipe()
  month = [str(s).zfill(2) for s in range(2,13)]
  for m in month:
    col = db['hk_twi_2015' + m]
    tweets_count = col.aggregate(pipe, allowDiskUse=True)
    w = []
    for t in tweets_count:
      w.append('\t'.join([str(i) for i in [m, t['_id']['day'], t['count']]]))
    print(w)
    with open('/now25/a.saito/work/result/2015_hk_sakura_count.txt', 'a') as f:
      f.write('\n'.join(w))
      f.write('\n')

main()
