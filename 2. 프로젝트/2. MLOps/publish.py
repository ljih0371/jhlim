from azureml.pipeline.core.graph import PipelineParameter
from azureml.pipeline.core import PipelineEndpoint
from azureml.pipeline.core import PublishedPipeline
from azureml.pipeline.core import PipelineRun

from azureml.core.authentication import InteractiveLoginAuthentication
from azureml.core.authentication import ServicePrincipalAuthentication
from azureml.core import (
    Workspace,
    Dataset,
    Datastore,
    ComputeTarget,
    Experiment,
    ScriptRunConfig,
)

import pandas as pd
import os

#####################################################################################################################
# interactive_auth = InteractiveLoginAuthentication(tenant_id="247258cc-5eb2-4fd4-9bb2-f272103f0c34")

# ws = Workspace.get(subscription_id='7722d447-2b14-4ca2-83c1-b4df9454a55a', 
#                    resource_group='MLOps_POC',
#                    name="mlw-mlops-dev-002",
#                    auth=interactive_auth
#                    )

#####################################################################################################################

# svc_pr_password = os.environ.get("AZUREML_PASSWORD")

svc_pr= ServicePrincipalAuthentication(
    tenant_id="247258cc-5eb2-4fd4-9bb2-f272103f0c34",
    service_principal_id="b7cfba68-a51b-4ae3-8885-cef273960a5e",
    service_principal_password="d4f8Q~~8tUXmQelSJyquy7lys17-t8gecKXCrb47")

ws = Workspace.get(subscription_id="7722d447-2b14-4ca2-83c1-b4df9454a55a",
                    resource_group="MLOps_POC",
                    name="mlw-mlops-dev-002",
                    auth=svc_pr)

print("Connect WorkSpace success")
#####################################################################################################################
# Experiment "test_mlpipeline"의 job 목록들 불러와서 데이터프레임으로 변환

exp = Experiment(workspace=ws, name="test_mlpipeline")
run_list = pd.DataFrame(exp.get_runs(),columns=['job'])

# Stauts, ID 추출
run_list['id'] = run_list.job.astype(str).str.split(',\n').str[1]
run_list['Status'] = run_list.job.astype(str).str.split(',\n').str[3]

run_list['ID'] = run_list.id.astype(str).str.split('Id:').str[1]

# 첫번째줄(=최신 실행된 job) id만 추출, 공백 제거
job_ID = run_list.iloc[0,3]
job_ID = job_ID.replace(" ", "")

# 파이프라인 publish
pipeline_run = PipelineRun(experiment=Experiment(ws, "test_mlpipeline"), run_id= job_ID)

published_pipeline = pipeline_run.publish_pipeline(name="test_mlpipeline",
                                                      description="My New Pipeline Description",
                                                      version="1.0",
                                                      continue_on_step_failure=True)

print("Publish Pipeline Success")
#####################################################################################################################
## 파이프라인 목록 불러와서 데이터프레임으로 변경
published_pipeline = PublishedPipeline.list(workspace=ws)
df = pd.DataFrame(published_pipeline, columns=['pipeline'])

# 파이프라인 id만 뽑아
df['Id'] = df.pipeline.astype(str).str.split(',\n').str[1]
df['ID'] = df.Id.astype(str).str.split(':').str[1]

# 첫번째줄 (=최신 생성된 파이프라인) id만 추출, 공백 제거
pipeline_ID = df.iloc[0,2]
pipeline_ID = pipeline_ID.replace(" ", "")

#####################################################################################################################
# 파이프라인 ID 넣어서 해당 파이프라인 불러와
latest_pipeline = PublishedPipeline.get(workspace=ws, id = pipeline_ID)

# 넣을 파이프라인 엔드포인트 불러와
pipeline_endpoint_by_name = PipelineEndpoint.get(workspace=ws, name="EndpointTest_jh")

# 추가 및 default 설정
pipeline_endpoint_by_name.add_default(latest_pipeline)

print("Publish Endpoint Success",latest_pipeline)

# # 추가
# pipeline_endpoint_by_name.add(published_pipeline)

# # default 설정
# pipeline_endpoint_by_name.set_default(published_pipeline)

# # 삭제
# pipeline_endpoint_by_name.archive() 

