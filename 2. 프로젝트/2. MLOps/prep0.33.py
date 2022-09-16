import pandas as pd
import numpy as np
from datetime import datetime, tzinfo
import os
import json


from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
# from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
import joblib

from azure.storage.blob import BlobServiceClient, BlobClient
from io import StringIO

# import azureml.core
# from azureml.core import Workspace, Datastore, Dataset
# from azureml.core.authentication import ServicePrincipalAuthentication

# 트리거가 실행 된 날짜
# trigger_date = datetime.today().strftime("%Y%m%d_%H%M%S")
trigger_date = datetime.today().date()

connectionString = "DefaultEndpointsProtocol=https;AccountName=mlops001;AccountKey=ZVHz+skHLZ8smlG355ZiJWBSfXT2VNvfR32DpmUBTT1LwCF4PuqtjFQKSSKhdzJpf1Tnduqs0wLo+ASt6SF5NQ==;EndpointSuffix=core.windows.net"
STORAGEACCOUNTURL= 'https://mlops001.blob.core.windows.net/'
STORAGEACCOUNTKEY= 'ZVHz+skHLZ8smlG355ZiJWBSfXT2VNvfR32DpmUBTT1LwCF4PuqtjFQKSSKhdzJpf1Tnduqs0wLo+ASt6SF5NQ=='
LOCALFILENAME= 'localfile.csv'
CONTAINERNAME= 'source'
BLOBNAME= 'raw/kpx+weather_190101_220228_pv_ess_new.csv'

# scaler_filename = f'scaler.cb'
scaler_filename = f'{trigger_date}_scaler.cb'

outputPath1_local = f'{trigger_date}_busan_train.csv'
outputPath2_local = f'{trigger_date}_busan_test.csv'

outputPath1_blob = f'output/{trigger_date}/busan_train.csv'
outputPath2_blob = f'output/{trigger_date}/busan_test.csv'
outputPath3_blob = f'scaler/{scaler_filename}'
outputPath4_blob = f'output/output.json'

blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)

# Establish connection with the blob storage account
blob1 = BlobClient.from_connection_string(conn_str=connectionString, container_name=CONTAINERNAME, blob_name=outputPath1_blob)
blob2 = BlobClient.from_connection_string(conn_str=connectionString, container_name=CONTAINERNAME, blob_name=outputPath2_blob)
blob3 = BlobClient.from_connection_string(conn_str=connectionString, container_name=CONTAINERNAME, blob_name=outputPath3_blob)
blob4 = BlobClient.from_connection_string(conn_str=connectionString, container_name=CONTAINERNAME, blob_name=outputPath4_blob)


busan_start_date = '2018-01-19-00:00:00'
degradation_rate = 0.6 / 365  

# ======================================================================================================================
# ======================================================================================================================
def addColData(df, start_date, rate) :
    
    start_date = pd.to_datetime(start_date,format='%Y-%m-%d-%H:%M:%S')
    start_date = pd.Timestamp.to_datetime64(start_date)

    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S', errors='raise')
    df['days'] = (df['date'] - start_date ) / np.timedelta64(1, 'D')
    df['pv_efficiency'] = df['days'].map(lambda x : - (x * rate))

# ======================================================================================================================
# ======================================================================================================================
def applyScaler(df) :
    cols = ['기온(°C)', '강수량(mm)', '습도(%)', '전운량(10분위)', '적설(cm)', '풍속(m/s)', 'pv_efficiency', 'month', 'hour']

    # MinMaxScaler 선언 및 Fitting
    scaler = MinMaxScaler()
    scaler.fit(df[cols])
    joblib.dump(scaler, scaler_filename)

    # 예측시간 5시 ~ 19시 필터링
    df = df[(df.hour>=5) & (df.hour<=19)].reset_index(drop=True)
    
    # 학습, 테스트 set 분리 
    x_train, x_test, y_train, y_test = train_test_split(df[cols], df['일사(MJ/m2)'], train_size=0.7)

    # 스케일 적용 
    x_train_scaled = scaler.transform(x_train)
    x_test_scaled = scaler.transform(x_test)

    # 데이터 프레임으로 저장 
    x_train_scaled = pd.DataFrame(x_train_scaled, columns=cols)
    x_test_scaled = pd.DataFrame(x_test_scaled, columns=cols)

    y_train = y_train.reset_index(drop=True)
    y_test = y_test.reset_index(drop=True)

    train_set = pd.concat([x_train_scaled, y_train],axis=1)
    test_set = pd.concat([x_test_scaled, y_test],axis=1)

    return train_set, test_set

def main() : 
    # 로컬로 다운로드
    with open(LOCALFILENAME, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)
    

    # LOCALFILE is the file path
    dataframe_blobdata = pd.read_csv(LOCALFILENAME)
    busan_df = dataframe_blobdata[dataframe_blobdata['발전기명']=='Gridsol부산태양광'].reset_index(drop=True)

    # 추가 정보 생성
    addColData(busan_df, busan_start_date, degradation_rate)

    # Scaler 적용
    train_set, test_set = applyScaler(busan_df)

    ## 데이터 셋 저장 !!!!!! 테스트 환경 성능 상 100개 열만!!!!!!!!!!!
    train_output = StringIO()
    test_output  = StringIO()

    train_output = train_set[:100].to_csv(outputPath1_local, encoding = "utf-8-sig")
    test_output = test_set[:100].to_csv(outputPath2_local, encoding = "utf-8-sig")

    # Upload the created file
    # with open(outputPath1_local, "rb") as data:
    #     blob1.upload_blob(data, overwrite=True)


    # with open(outputPath2_local, "rb") as data:
    #     blob2.upload_blob(data, overwrite=True)

    # with open(scaler_filename, "rb") as data:
    #     blob3.upload_blob(data, overwrite=True) 

    with open(outputPath1_local, "rb") as d1, open(outputPath2_local, "rb") as d2, open(scaler_filename, "rb") as d3:
        blob1.upload_blob(d1, overwrite=True)
        blob2.upload_blob(d2, overwrite=True)
        blob3.upload_blob(d3, overwrite=True) 
        

    # remove local file
    os.remove(LOCALFILENAME)
    os.remove(outputPath1_local)
    os.remove(outputPath2_local)
    os.remove(scaler_filename)

    scaler_path = f'{STORAGEACCOUNTURL}{CONTAINERNAME}/{outputPath3_blob}'

    # 결과 저장
    out_dict = {}
    out_dict["scaler_path"] = scaler_path
    print(out_dict)

    # with open('outputs.json','w') as file:
    #     file.write(str(out_dict))
    
    with open("outputs.json", "w") as json_file:
        json.dump(out_dict, json_file)   

    # with open('outputs.json','rb') as file:
    #     blob4.upload_blob(file, overwrite=True)

    return scaler_path

main()
