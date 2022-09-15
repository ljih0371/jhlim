#!/bin/bash
# FileName : edgeHubStatusCheck.sh
# 실행방법 : /home/data/edgeHubStatusCheck.sh >> /home/data/logfile/restart/edgeHubStatusCheck.log
# 백그라운드 실행방법 : nohup /home/data/edgeHubStatusCheck.sh >> /home/data/logfile/restart/edgeHubStatusCheck.log&
PS_CNT=`ps -ef | grep -v grep | grep -v sudo | grep edgeHubStatusCheck.sh | wc -l`
TODAY=`/bin/date`
HUB_ERR_CNT=`grep "WRN.*Error sending messages to module" /var/lib/docker/containers/$(docker ps | grep edgeHub | awk '{print $1}')*/*json.log | wc -l`

# 라인 아이디 정의
LID= # " "


# 로그 폴더 생성
#/bin/mkdir -p /home/data/logfile/restart
# 프로세스를 확인하여 구동중이지 않을때
if [ $PS_CNT -le 2 ];then
        echo "Press [CTRL+C] to stop.."

        # 데몬 실행
        while :
        do
                sleep 3
                
                HUB_ERR_LAST_CNT=`grep "WRN.*Error sending messages to module" /var/lib/docker/containers/$(docker ps | grep edgeHub | awk '{print $1}')*/*json.log | wc -l`
				
                TODAY=`/bin/date`

                # 처리해야할 데이터 개수와 ML에서 iotedge 서비스 재시작 요청 확인
                if [ $HUB_ERR_LAST_CNT -gt $HUB_ERR_CNT ];then
                        echo "[$(/bin/date)] EdgeHub에 에러가 발생되어, iotedge를 재기동합니다.....($HUB_ERR_LAST_CNT)"
                        
						/bin/systemctl restart iotedge
											
                        echo "[$(/bin/date)] iotedge 서비스를 준비중입니다...... !"
						
						# sql commands -------------------------------
						/home/data/insertSQL.sh $LID RS001 edgeHubStatusCheck
						sleep 10

                        # iotedge 정상기동을 확인
                        for i in $(seq 1 50); do

                                RUN_CNT=`/usr/bin/iotedge list | awk '{print $2}' | grep running | wc -l`
                                if [ $RUN_CNT -eq 5 ]
                                then
                                        echo "[$(/bin/date)] iotedge running OK..........($RUN_CNT)"
										# sql commands -------------------------------
										
										/home/data/insertSQL.sh $LID RS002 edgeHubStatusCheck
										
                                        sleep 5
										HUB_ERR_CNT=$HUB_ERR_LAST_CNT
                                        break
                                elif [ $i -eq 50 ]
                                then
                                        echo "[$(/bin/date)] iotedge의 정상 기동이 실패되었습니다 !!!!!!!!!!!!!"
                                        echo "[$(/bin/date)] 잠시 후 상태를 확인해주세요.... !"
                                        echo "(상태확인방법) 모두 running 상태인 경우 OK"
                                        echo "(명령어) /usr/bin/iotedge list"
										
										/home/data/insertSQL.sh $LID RS003 edgeHubStatusCheck
										
                                        continue
                                else
                                        echo "[$(/bin/date)] iotedge list running count.......($RUN_CNT)"
                                        sleep 1
                                fi
                        done

                else
                        # 동작 확인 메시지 주석 처리 
						# echo "[$(/bin/date)] 정상 동작중입니다..............("$DATA_FILE_CNT")"
						# /home/data/insertSQL.sh $LID TEST edgeHubStatusCheck
						HUB_ERR_CNT=$HUB_ERR_LAST_CNT
                fi
        done

else
        # 프로세스가 이미 구동중일때는 실행하지 않음
        echo "Demon running.....($PS_CNT)"
fi

