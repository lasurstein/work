def setup_mongo(db_name):
  connection = MongoClient()
  db = connection[db_name]
  print('mongoDB ready')
  return db

def setup_mecab(mecab_path):
  mecab = MeCab.Tagger('-d ' + mecab_path)
  mecab.parse('')
  print('mecab ready')
  return mecab