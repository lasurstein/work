"""
textファイルを出力
### input
p_name: ex.)北海道
p_code: ex.)hk

### output
以下のカラムからなるtsvファイルpname_2015.tsv
text

"""

from pymongo import MongoClient, DESCENDING

result_dir = '/now24/a.saito/work/hottoSNS-bert'


def setup_mongo(dbname):
    connection = MongoClient()
    db = connection[dbname]
    print('mongoDB ready')
    return db


# BERT用TSVデータ作成
def make_data(db, p):
    month = [str(i).zfill(2) for i in range(2, 6)]
    lines = []
    for m in month:
        print('\n\n\n  ###################' + str(m))
        collection = db['2015-' + m]
        twis = collection.find()

        for twi in twis:
            text = twi['text']
            text = text.replace('\n', ' ')
            if not text.isspace():
                lines.append("{0}\t{1}".format(text, twi['created_at']))

        print("{0}:db count:  {1}".format(m, len(lines)))

    test_path = '/now24/a.saito/work/0625test_{}_2015.tsv'.format(p)
    with open(test_path, 'w') as f:
        for l in lines:
            f.write(l + '\n')


def main():
    d = {
        'hk': {
            'pname': '北海道',
            'dbname': '2015_hk_twi_1208',
        },
        'is': {
            'pname': '石川県',
            'dbname': '2015_is_twi',
        },
        # 'tk': {
        #     'pname': '東京都',
        #     'file_lst': ['0325', '0326', '0327', '0328', '0329', '0330'],
        #     'm_lst': ['03']
        # }
    }

    for p in d.keys():
        print(p)
        print(d[p])
        print(d[p]['dbname'])
        db = setup_mongo(d[p]['dbname'])
        make_data(db, p)



main()
