#!/bin/bash
mkdir /now24/a.saito/data_2014
cd /now24/a.saito/data_2014
pwd
for i in 2 3 4 5
  do
    dirname="/poisson3/backup/now09/shimozono/2018/experiment/data_2014/2014-0$i"
    cp -rv $dirname .
  done

work
cp -rv /poisson3/backup/now09/shimozono/2018/experiment/data .
