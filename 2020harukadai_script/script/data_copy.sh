#!/bin/bash
mkdir /now24/a.saito/data_2014
cd /now24/a.saito/data_2014
pwd
for i in 2 3 4 5
  do
    dirname="/poisson3/backup/now09/shimozono/2018/experiment/data_2014/2014-0$i"
    cp -rv $dirname .
  done

mkdir /now24/a.saito/data_2015
cd /now24/a.saito/data_2015
pwd
for i in 2 3 4 5 6 7 8 9
  do
    dirname="/poisson3/backup/now09/shimozono/2018/experiment/data/2015-0$i"
    cp -rv $dirname .
  done
for i in 10 11 12
  do
    dirname="/poisson3/backup/now09/shimozono/2018/experiment/data/2015-0$i"
    cp -rv $dirname .
  done

# データを取ってくるスクリプトです
# 全データ取ってくると容量いっぱいになって怒られるかもしれません(特に2014年ぶん)
# そのときは1ヶ月分コピー→DBに入れる→消すの手順でやるようにしてください