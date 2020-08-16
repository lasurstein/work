"""
count.pyを元にtextとsakura_labelからなるtsvファイルを出力
### input
rate: ex.)80
p_name: ex.)北海道
p_code: ex.)hk

### output
以下のカラムからなるtsvファイルpname_rate_(sakura|other).tsv
text [tab] sakura_label

"""

from pymongo import MongoClient, DESCENDING
# from datetime import datetime, timedelta
import datetime
import pandas as pd
import random, pytz

# from s_lib import setup_mongo

result_dir = '/now24/a.saito/work/hottoSNS-bert'


def setup_mongo(dbname):
    connection = MongoClient()
    db = connection[dbname]
    print('mongoDB ready')
    return db


# def daterange(start, end):
#     for n in range((end - start).days):
#         yield start + timedelta(n)


# 事前に求めた関連キーワードをリストとして読み込む
def get_relation_words(relation_words_file, rate):
    with open(relation_words_file, 'r') as f:
        df = pd.read_csv(f, header=None, names=['word', 'aso'])
        is_asosiation_over_zero = df['aso'] > 0
        count = is_asosiation_over_zero.sum()
        num = int(round(count * (rate / 100)))
        limit = df.iat[num - 1, 1]
        word_lst = []
        word_lst = df[df.aso >= limit].word.values.tolist()
        print('file: ' + relation_words_file + ', rate: ' + str(rate) + '\n limit: ' +
              str(limit) + ', words_count: ' + str(len(word_lst)))
    return word_lst


# # 日毎の全ツイート数を求める
# def count_all(p_name, db, start, end):
#     all_twi = []
#     print(p_name + '_all_count')
#     for date in daterange(start, end):
#         today = date.isoformat()
#         nday = (date + timedelta(days=1)).isoformat()
#         all_twi_pipe = {
#             'created_at_iso': {
#                 '$gte': today,
#                 '$lt': nday
#             }
#         }
#         m = str(date.month).zfill(2)
#         d = str(date.day).zfill(2)
#         collection = db['2015-' + m]
#         all_twi_count = collection.find(all_twi_pipe).count()
#         all_twi.append('\t'.join([m, d, str(all_twi_count)]))
#
#     all_twi_path = result_dir + '/count/' + p_name + '_all.txt'
#     with open(all_twi_path, 'w') as af:
#         af.write('\n'.join(all_twi))


# def iso_to_jstdt(iso_str):
#     dt = None
#     try:
#         dt = datetime.datetime.strptime(iso_str, '%Y-%m-%dT%H:%M:%S')
#         dt = pytz.utc.localize(dt).astimezone(pytz.timezone("Asia/Tokyo"))
#     except ValueError:
#         try:
#             dt = datetime.datetime.strptime(iso_str, '%Y-%m-%dT%H:%M:%S')
#             dt = dt.astimezone(pytz.timezone("Asia/Tokyo"))
#         except ValueError:
#             pass
#     return dt
#
#
# def get_datatime(year, date):
#     month, day = int(date[:2]), int(date[2:])
#     date = datetime.date(year, month, day)
#     print(date)
#     return date

sakura = ['桜', 'さくら', 'サクラ']
# BERT用TSVデータ作成
def make_data(db, p, rate, relation_words_file, file_lst):
    filename = relation_words_file
    print('###\t' + str(rate))

    words = set(get_relation_words(filename, rate))
    if len(words) < 1:
        print('non words\n')
        return

    # print(words)

    sakura_lines = []
    other_lines = []
    month = [str(i).zfill(2) for i in range(1, 13)]
    s_l, o_l = 0, 0

    collection = db[p + '2014']
    twis = collection.find()

    for twi in twis:
        text = twi['text'].replace('\n', ' ')
        morpho = [w.rsplit("/",1)[0] for w in twi['morpho_text'].split(' ')]
        created_day = twi['created_at_iso'][5:7] + twi['created_at_iso'][8:10]
        # print(morpho, created_day, created_day in file_lst)
        # if (created_day in file_lst) and len(set(morpho) & words) > 0:
        #     print(created_day, file_lst, set(morpho) & words, twi['sakura_twi'])
        #     return
        if ((created_day in file_lst) and len(set(sakura) & set(morpho)) > 0) and (len(set(morpho) & words) > 0):
            sakura_lines.append("1\t{}\t{}".format(text, twi['created_at']))
        else:
            other_lines.append("0\t{}\t{}".format(text, twi['created_at']))

    print("db count:  {}".format(collection.find().count()))
    print("sakura: {0}\nother: {1}".format(len(sakura_lines), len(other_lines)))
    print("add sakura: {0}\n add other: {1}".format(len(sakura_lines)-s_l, len(other_lines)-o_l))
    s_l = len(sakura_lines)
    o_l = len(other_lines)

    other_limit_lines = random.sample(other_lines, s_l)
    random.shuffle(sakura_lines)

    s_limit = round(s_l / 10)
    o_limit = s_limit

    train_path = '/now24/a.saito/work/train_{}_{}.tsv'.format(p, str(rate))
    # test_path = '/now24/a.saito/work/test_{}_{}.tsv'.format(p, str(rate))
    dev_path = '/now24/a.saito/work/dev_{}_{}.tsv'.format(p, str(rate))

    with open(train_path, 'w') as f:
        for l in range(s_limit * 2, s_l):
            f.write(sakura_lines[l] + '\n')
            f.write(other_limit_lines[l] + '\n')
    # with open(test_path, 'w') as f:
    #     for l in range(0, s_limit):
    #         f.write(sakura_lines[l] + '\n')
    #         f.write(other_limit_lines[l] + '\n')
    with open(dev_path, 'w') as f:
        for l in range(0, s_limit * 2):
            f.write(sakura_lines[l] + '\n')
            f.write(other_limit_lines[l] + '\n')


def main():
    d = {
        'hk': {
            'pname': '北海道',
            'file_lst': ['0429', '0430', '0501', '0502'],
        },
        'is': {
            'pname': '石川県',
            'file_lst': ['0401', '0402', '0403', '0404', '0405', '0406', '0407'],
        },
        # 'tk': {
        #     'pname': '東京都',
        #     'file_lst': ['0325', '0326', '0327', '0328', '0329', '0330'],
        #     'm_lst': ['03']
        # }
    }

    db = setup_mongo('tweet2014')

    for p in d.keys():
        filename = '/now24/a.saito/work/result/relation_word/{0}_soa.txt'.format(p)
        rate = 70
        make_data(db, p, rate, filename, d[p]['file_lst'])


main()
