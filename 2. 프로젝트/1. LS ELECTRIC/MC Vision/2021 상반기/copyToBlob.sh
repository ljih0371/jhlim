#!/bin/sh
YESTERDAY=`/bin/date -d "-$1 days" "+%Y%m%d"`
MON=`/bin/date -d "-$1 days" "+%Y%m"`

CONDITION=$YESTERDAY"*.*.*.*"

LINE_LIST="MC22b_2 MC22b_3"
DIR_LIST="NG NG_OK XML"


for line in $LINE_LIST
do
  for dir in $DIR_LIST
  do
    echo "*******DATA : " $dir/$MON " *******"
    azcopy \
       --source /data/mc/$line/$dir/$MON \
       --destination https://cj01vision002.blob.core.windows.net/mc-vision/$line/$dir/$MON \
       --dest-key XwwdSCl+QHHT9pEQj6+Q7C2+3BixfGHdcvEnU6UU+psu19i2vBN5ep+4yFuidDsd/ucBg43Mli6zaa6xyosHtQ== \
       --include $CONDITION \
       --recursive \
       --quiet
  done
done
