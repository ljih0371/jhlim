import os
import azureml.core
from azureml.core.authentication import ServicePrincipalAuthentication
import pandas as pd

from azureml.core import (
    Workspace,
    Experiment,
    Dataset,
    Datastore,
    ComputeTarget,
    Environment,
    ScriptRunConfig
)

from azureml.pipeline.core import (
    Pipeline,
    PipelineData,
    PipelineEndpoint,
    PublishedPipeline,
    PipelineRun,
    InputPortBinding
)

from azureml.pipeline.steps import (
    PythonScriptStep,
    DataTransferStep
)
from azureml.pipeline.core.graph import PipelineParameter

from azureml.data.datapath import (
    DataPath, 
    DataPathComputeBinding, 
    DataReference
)

from azureml.data import (OutputFileDatasetConfig)
from azureml.data.dataset_consumption_config import DatasetConsumptionConfig
from azureml.core.compute import AmlCompute
from azureml.core.compute_target import ComputeTargetException

from azureml.core.runconfig import RunConfiguration

from azure.ai.ml import Input

print("SDK version:", azureml.core.VERSION)


## 작업 영역 연결 ##

svc_pr= ServicePrincipalAuthentication(
    tenant_id="247258cc-5eb2-4fd4-9bb2-f272103f0c34",
    service_principal_id="b7cfba68-a51b-4ae3-8885-cef273960a5e",
    service_principal_password="d4f8Q~~8tUXmQelSJyquy7lys17-t8gecKXCrb47")


ws = Workspace.get(subscription_id="7722d447-2b14-4ca2-83c1-b4df9454a55a",
                    resource_group="MLOps_POC",
                    name="mlw-mlops-dev-002",
                    auth=svc_pr)

print(ws.name, ws.resource_group, ws.subscription_id, sep = '\n')


## Experiment ##
experiment_folder = 'python_pipeline'
os.makedirs(experiment_folder, exist_ok = True)

print(experiment_folder)

# choose a name for your cluster ##
cluster_name = "cluster-mlops-jh"

## environment 생성 및 등록 ##
experiment_env = Environment.from_conda_specification("Experiment_env", experiment_folder + "/conda.yml")
experiment_env.register(workspace=ws)

## environment 연결 ##
registered_env = Environment.get(ws, 'Experiment_env')
pipeline_run_config = RunConfiguration()
pipeline_run_config.target = cluster_name
pipeline_run_config.environment = registered_env

print("Run configuration created.")

## 작업 영역에서 datasotre 가져오기 ## 
datastore = Datastore.get(ws, 'busandatastore')

## input Dataset 가져오기 ## 
train_dataset = Dataset.get_by_name(ws,'tab_pvprediction_train')
test_dataset = Dataset.get_by_name(ws,'tab_pvprediction_test')


## 각 스텝 output ##
## PipelineData : 파이프라인의 중간 데이터

model = PipelineData("model",
                     data_type = "UriFolder", 
                     output_mode='upload',
                     output_path_on_compute = "//datastores/busandatastore/paths/azureml/{name}/model/")

scored_data = PipelineData("scored_data", 
                            data_type = "UriFolder", 
                            output_mode='upload',
                            output_path_on_compute = "//datastores/busandatastore/paths/azureml/{name}/scored_data/")


## train_step 파라미터값 ##
param_location = PipelineParameter(name="location", default_value="busan")
param_test_size = PipelineParameter(name="test_size", default_value=0.3)
param_shuffle = PipelineParameter(name="shuffle", default_value=True)
param_random_state = PipelineParameter(name="random_state", default_value=34)
param_message = PipelineParameter(name="message", default_value="AddParameterTest")

train_pipeline_param = PipelineParameter(name="traindata_param", 
                                         default_value=train_dataset)
traindata_input = DatasetConsumptionConfig("traindata",train_pipeline_param)


## score_step 파라미터값 ##
test_pipeline_param = PipelineParameter(name="testdata_param", 
                                           default_value=test_dataset)

## DatasetConsumptionConfig : 데이터 세트를 컴퓨팅 대상에 전달
testdata_input = DatasetConsumptionConfig("testdata",test_pipeline_param)


## step 생성 ##
train_step = PythonScriptStep(
    name="train step",
    source_directory=experiment_folder,
    script_name="train_model.py",
    arguments=[ "--model-path", model,
                "--location", param_location, 
                "--test-size", param_test_size, 
                "--shuffle" , param_shuffle, 
                "--random-state" , param_random_state, 
                "--message", param_message,
                "--param1", traindata_input], 
    
    inputs=[traindata_input],
    outputs= [model],
    
    compute_target=cluster_name,
    runconfig=pipeline_run_config,
    allow_reuse=True
)

print("Pipeline train steps defined")

score_step = PythonScriptStep(
    name="score step",
    source_directory=experiment_folder,
    script_name="score_model.py",
    
    arguments=["--model-path", model,
               "--param1", testdata_input,
               "--scoreddata-path", scored_data],
    
    inputs=[testdata_input, model],
    outputs=[scored_data],
    
    compute_target=cluster_name,
    runconfig=pipeline_run_config,
    allow_reuse=True
)
print("Pipeline score steps defined")

evaluate_step = PythonScriptStep(
    name="evaluate step",
    source_directory=experiment_folder,
    script_name="evaluate_model.py",
    arguments=["--scoreddata-path", scored_data],
    inputs=[scored_data],
    compute_target=cluster_name,
    runconfig=pipeline_run_config,
    allow_reuse=True
)
print("Pipeline evaluate steps defined")


## 파이프라인 실행 ##

pipeline_steps = [train_step, score_step, evaluate_step]
pipeline = Pipeline(workspace=ws, steps=pipeline_steps)
print("Pipeline is built")

## Experiment 개체 생성 ##
exp = Experiment(workspace=ws,name="Pipeline_python_jh")

pipeline_run = exp.submit(pipeline,regenerate_outputs = True)
print("Pipeline submitted for execution.")

pipeline_run.wait_for_completion(show_output=True)


### 파이프라인 publish ##

published_pipeline = pipeline_run.publish_pipeline(
     name="SDK_Published_Pipeline_jh",
     description="My Published Pipeline by SDK",
     version="1.0",
     continue_on_step_failure=True)


### 파이프라인 id 추출 ###
## 파이프라인 목록 불러와서 데이터프레임으로 변경 ##
published_pipeline = PublishedPipeline.list(workspace=ws)
df = pd.DataFrame(published_pipeline, columns=['pipeline'])

# 파이프라인 id만 뽑아 ##
df['Id'] = df.pipeline.astype(str).str.split(',\n').str[1]
df['ID'] = df.Id.astype(str).str.split(':').str[1]

# 첫번째줄 (=최신 생성된 파이프라인) id만 추출, 공백 제거 ##
pipeline_ID = df.iloc[0,2]
pipeline_ID = pipeline_ID.replace(" ", "")

# 파이프라인 ID 넣어서 해당 파이프라인 불러와 ##
latest_pipeline = PublishedPipeline.get(workspace=ws, id = pipeline_ID)


# ## 파이프라인 엔드포인트 생성 및 등록
# pipeline_endpoint_by_name = PipelineEndpoint.publish(workspace=ws, name="PipelineEndpoint_by SDK_jh",
#                                              pipeline=latest_pipeline, description="SDK Test")

# 넣을 파이프라인 엔드포인트 불러와 ##
pipeline_endpoint_by_name = PipelineEndpoint.get(workspace=ws, name="PipelineEndpoint_by SDK_jh")

# 추가 및 default 설정 ##
pipeline_endpoint_by_name.add_default(latest_pipeline)