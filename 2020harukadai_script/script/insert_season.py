"""
MongoDBにツイートのJSONデータを突っ込むスクリプト
2014年のデータ用（関連キーワード取得用のデータ）
"""

from pymongo import MongoClient, DESCENDING
from dateutil import parser
from datetime import datetime
from pytz import timezone
from .s_lib import setup_mongo, setup_mecab
import os, MeCab, json, re, glob


r_men = re.compile('@(\w)+\s')
r_kigou = re.compile(
    '[!"#$%&\'\\\\()*+,-./:;<=>?@[\\]^_`{|}~「」〔〕“”〈〉『』【】＆＊・（）＄＃＠。、？！｀＋￥％]')
r_url = re.compile('http(s)?://t.co/\w+')

sakura = set(['桜', 'さくら', 'サクラ'])


def setup_mecab():
    mecab = MeCab.Tagger(
        '-d /now24/a.saito/local/mecab/lib/mecab/dic/mecab-ipadic-neologd')
    mecab.parse('')
    print('mecab ready')
    return mecab


def setup_mongo():
    connection = MongoClient()
    db = connection['2014_sakura_twi_1208']
    print('mongoDB ready')
    return db


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


def insert(line, p, c, mecab):
    jsonline = re.sub('^\d*\t', '', line)
    try:
        textline = json.loads(jsonline)
        pname = textline['reverse_geo']['pname']
        if pname == p:
            text = text_cleaning(textline['text'])
            morpho = morpho_text(text, mecab)
            created_at = textline['created_at']
            sakura_twi = 1 if (len(sakura & set(morpho)) > 0) else 0
            insert_line = {
                'pname': pname,
                'text': text,
                'morpho_text': ' '.join(morpho),
                'sakura_twi': sakura_twi,
                'created_at': created_at,
                'created_at_iso': parser.parse(created_at).astimezone(timezone('Asia/Tokyo')).isoformat()
            }
            c.insert_one(insert_line)
    except Exception:
        pass
    return


def main():
    mecab = setup_mecab()
    db = setup_mongo()

    # 各地域とも2014年の開花日から満開日のデータしか使わないためfile_lstで指定
    d = {
        'hk': {
            'pname': '北海道',
            'file_lst': ['0429', '0430', '0501']
        },
        'is': {
            'pname': '石川県',
            'file_lst': ['0401', '0402', '0403', '0404', '0405', '0406', '0407']
        },
        'tk': {
            'pname': '東京都',
            'file_lst': ['0325', '0326', '0327', '0328', '0329', '0330']
        }
    }

    for key in d:
        for date in d[key]['file_lst']:
            collection = db['season_' + key]
            month, day = date[:2], date[2:]
            filename = '/now24/a.saito/data_2014/2014-' + month + \
                '/json_rg_2014-' + month + '-' + day + '.txt'
            with open(filename, 'r') as f:
                print('##### insert ' + filename + ' ...')
                line = f.readline()
                while line:
                    insert(line, d[key]['pname'], collection, mecab)
                    line = f.readline()


main()
