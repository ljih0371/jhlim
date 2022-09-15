#!/bin/bash
# insertSQL.sh

arg_lid=$1
arg_eventid=$2
arg_curuser=$3

DB_SERVER="165.244.138.57"
DB_USER="lsisadmin"
DB_PASSWD="Tbvjaosvkdnj00$"
DB_NAME="mc_edge"
TABLE="events"
LID=$arg_lid
CUR_USER=$arg_curuser
SEQ="(select if(count(*)=0, 1, seq+1) as seq from (select lid, dt, max(seq) as seq from mc_edge.events where lid=$LID and dt = curdate() group by lid, dt) v)"


EVENT_ID=$arg_eventid

#mysql commands
/usr/bin/mysql -h$DB_SERVER -u$DB_USER -p$DB_PASSWD $DB_NAME << EOF
system echo "Event SQL Start................";
INSERT INTO $TABLE (LID,DT,SEQ,INSERT_USER,INSERT_DATE,EVENT_ID) 
VALUES ($LID, curdate(),$SEQ,"$CUR_USER", now(), "$EVENT_ID");
commit;
system echo "Event SQL End..................";
EOF
