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
from datetime import datetime, timedelta
import pandas as pd
from .s_lib import setup_mongo

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

# BERT用TSVデータ作成
def make_data(db, rate, relation_words_file):
    filename = relation_words_file
    print('###\t' + str(rate))

    words = set(get_relation_words(filename, rate))
    if len(words) > 0:
        return

    sakura_lines = []
    other_lines = []
    month = [str(i).zfill(2) for i in range(2, 13)]
    for m in manth:
        collection = db['2015-' + m]
        twis = collection.find()
        for twi in twis:
            text = twi['text'].replace('\n', ' ')
            if (twi['sakura_twi'] == '1') and (len(set(twi['morpho_text'].split()) & words) > 0):
                sakura_lines.append("{}\t1\t{}\t{}".format(twi['id'], text, twi['created_at']))
            else :
                other_lines.append("{}\t0\t{}\t{}".format(twi['id'], text, twi['created_at']))

    s_limit = round(len(sakura_lines) / 10)
    o_limit = round(len(other_lines) / 10)

    train_path = '/now24/a.saito/work/train_' + str(rate) + '.tsv'
    test_path = '/now24/a.saito/work/test_' + str(rate) + '.tsv'

    with open(train_path, 'w') as f:
        for l in sakura_lines[s_limit:]:
            f.write(l + '\n')
        for l in other_lines[o_limit:]:
            f.write(l + '\n')
    with open(test_path, 'w') as f:
        for l in sakura_lines[:s_limit]:
            f.write(l + '\n')
        for l in other_lines[:o_limit]:
            f.write(l + '\n')

def main():
    dbname = 'dbname'
    p_name = 'tk'

    db = setup_mongo('2015_' + p + '_twi')
#     start = datetime.strptime('20150217', '%Y%m%d')
#     end = datetime.strptime('20151231', '%Y%m%d')

    rate = 70
    count_sakura(p_name, db, rate, relation_words_file)


main()
