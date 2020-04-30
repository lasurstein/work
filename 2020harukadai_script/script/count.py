"""
総ツイート数と桜の関連ツイート数を日毎にカウントする
"""

from pymongo import MongoClient, DESCENDING
from datetime import datetime, timedelta
import pandas as pd
from .s_lib import setup_mongo￥

result_dir = '/now24/a.saito/work/result/'


def daterange(start, end):
    for n in range((end - start).days):
        yield start + timedelta(n)

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
        print('file: ' + filename + ', rate: ' + str(rate) + '\n limit: ' +
              str(limit) + ', words_count: ' + str(len(word_lst)))
    return word_lst

# 日毎の全ツイート数を求める
def count_all(p_name, db, start, end):
    all_twi = []
    print(p_name + '_all_count')
    for date in daterange(start, end):
        today = date.isoformat()
        nday = (date + timedelta(days=1)).isoformat()
        all_twi_pipe = {
            'created_at_iso': {
                '$gte': today,
                '$lt': nday
            }
        }
        m = str(date.month).zfill(2)
        d = str(date.day).zfill(2)
        collection = db['2015-' + m]
        all_twi_count = collection.find(all_twi_pipe).count()
        all_twi.append('\t'.join([m, d, str(all_twi_count)]))

    all_twi_path = result_dir + '/count/' + p_name + '_all.txt'
    with open(all_twi_path, 'w') as af:
        af.write('\n'.join(all_twi))

# 日毎の桜の関連ツイート数を数える
def count_sakura(p_name, db, start, end, relation_words_file):
    rates = range(10, 101, 10)
    filename = relation_words_file

    # 関連キーワードの使用割合を上位から10％ごと増やしていく
    for rate in rates:
        print('###\t' + str(rate))
        words = set(get_relation_words(filename, rate))
        sakura_twi = []
        for date in daterange(start, end):
            today = date.isoformat()
            nday = (date + timedelta(days=1)).isoformat()
            sakura_twi_pipe = {
                'sakura_twi': 1,
                'created_at_iso': {
                    '$gte': today,
                    '$lt': nday
                }
            }
            m = str(date.month).zfill(2)
            d = str(date.day).zfill(2)
            collection = db['2015-' + m]

            sakura_twis = collection.find(sakura_twi_pipe)
            sakura_twi_count = 0
            if len(words) > 0:
                for twi in sakura_twis:
                    if len(set(twi['morpho_text'].split()) & words) > 0:
                        sakura_twi_count = sakura_twi_count + 1
            else:
                sakura_twi_count = len(sakura_twis)
            sakura_twi.append('\t'.join([m, d, str(sakura_twi_count)]))

        sakura_twi_path = result_dir + '/count/' + p_name + '_' + str(rate) + '.txt'
        with open(sakura_twi_path, 'w') as sf:
            sf.write('\n'.join(sakura_twi))


def main():
    dbname = 'dbname'
    p_name = 'tk'

    db = setup_mongo('2015_' + p + '_twi')
    start = datetime.strptime('20150217', '%Y%m%d')
    end = datetime.strptime('20151231', '%Y%m%d')

    word_lst = get_relation_words(relation_words_file, rate)

    count_all(p_name, db, start, end)
    count_sakura(p_name, db, start, end, relation_words_file)


main()
