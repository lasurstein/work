"""
MongoDBにツイートのJSONデータを突っ込むスクリプト
2015年のデータ前提（評価用）で書いているので,
2014年のデータ(教師用)をDBに入れるときはいい感じに書き直してください.
"""

from pymongo import MongoClient, DESCENDING
from dateutil import parser
from datetime import datetime
from pytz import timezone
from .s_lib import setup_mongo, setup_mecab
import os, sys, MeCab, json, re, glob


# ツイートのクレンジング用正規表現
r_men = re.compile('@(\w)+\s')
r_kigou = re.compile(
    '[!"#$%&\'\\\\()*+,-./:;<=>?@[\\]^_`{|}~「」〔〕“”〈〉『』【】＆＊・（）＄＃＠。、？！｀＋￥％]')
r_url = re.compile('http(s)?://([-\w]+\.)+[-\w]+(/[- ./?%\w&=]*)?')

sakura = set(['桜', 'さくら', 'サクラ'])


# text中の名詞と動詞のみを取り出したリストを返す
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


# ツイートのJSONデータlineをDBに挿入する
# p_name:都道府県名
def insert(line, p_name, collectionlection, mecab):
    jsonline = re.sub('^\d*\t', '', line)
    try:
        textline = json.loads(jsonline)
        pname = textline['reverse_geo']['pname']
        if pname == p_name:
            text = text_cleaning(textline['text'])
            morpho = morpho_text(text, mecab)
            created_at = textline['created_at']
            # (桜, さくら, サクラ) のいずれかが含まれる場合, sakura_twiを1にセット
            sakura_twi = 1 if (len(sakura & set(morpho)) > 0) else 0
            insert_line = {
                'pname': pname,
                'text': text,
                'morpho_text': ' '.join(morpho),
                'sakura_twi': sakura_twi,
                'created_at': created_at,
                'created_at_iso': parser.parse(created_at).astimezone(timezone('Asia/Tokyo')).isoformat()
            }
            collectionlection.insert_one(insert_line)
    except Exception:
        pass
    return


def main():
    # 以下の変数はいい感じに修正してください
    data_path = '/now24/a.saito/data/2015-'
    mecab_path = '/now24/a.saito/local/mecab/lib/mecab/dic/mecab-ipadic-neologd'
    db_name = '2015_tk_twi'
    p_name = '東京都'

    mecab = setup_mecab(mecab_path)
    db = setup_mongo(db_name)
    month = [str(s).zfill(2) for s in range(2, 13)]

    for m in month:
        collection = db['2015-' + m]
        # data/2015-mmにあるファイル名を指定
        for filename in glob.glob(data_path + str(m) + '/*.txt'):
            with open(filename, 'r') as f:
                print('##### insert ' + filename + ' ...')
                line = f.readline()
                while line:
                    insert(line, p_name, collection, mecab)
                    line = f.readline()


main()
