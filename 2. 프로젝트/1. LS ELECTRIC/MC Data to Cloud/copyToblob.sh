#!/bin/sh
YESTERDAY=`/bin/date -d "-1 days" "+%Y-%m-%d"`

#SET=$(seq 1 8)
#for i in $SET
#do
#  echo $i/$YESTERDAY
#done

SET=$(seq 1 8)
for i in $SET
do
  echo "*******Line ID : " $i " *******"
  azcopy \
     --source /data/mc/$i/$YESTERDAY/Raw \
     --destination https://mcstg.blob.core.windows.net/mc-origin-data-jyt/$i/R/$YESTERDAY \
     --dest-key piMsrtvaNgs+SawcW7dVOfXiq0dJHwIQYwuk66bz7MAh53a46uF3yFTBJrkwu8HrdgdNiWz3ndt2ZdWf+4g1MQ== \
     --quiet

  find /data/mc/$i -name '*.h5' -mtime +10 -delete

done
