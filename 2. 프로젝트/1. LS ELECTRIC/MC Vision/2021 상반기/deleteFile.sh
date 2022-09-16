#!/bin/sh
LINE_LIST="MC22b_2 MC22b_3 MC22b_4 MR_1"

for line in $LINE_LIST
do
  find /data/mc/$line -name '*.png' -mtime +5 -delete
  find /data/mc/$line -name '*.xml' -mtime +5 -delete
done

