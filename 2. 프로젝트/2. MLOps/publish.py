from azureml.pipeline.core.graph import PipelineParameter
from azureml.pipeline.core import PipelineEndpoint
from azureml.pipeline.core import PublishedPipeline
from azureml.core import Workspace

# published_pipeline1 = pipeline_run1.publish_pipeline(
#      name="My_Published_Pipeline",
#      description="My Published Pipeline Description",
#      version="1.0")

ws = Workspace.get(name="mlw-mlops-dev-002", 
                   subscription_id='7722d447-2b14-4ca2-83c1-b4df9454a55a', 
                   resource_group='MLOps_POC')


published_pipeline = PublishedPipeline.get(workspace=ws, id="b45b92d6-76a8-445e-bc30-e515559e961d")
pipeline_endpoint = PipelineEndpoint.publish(workspace=ws, name="PipelineEndpointTest",
                                            pipeline=published_pipeline, description="Test description Notebook")


pipeline_endpoint_by_name = PipelineEndpoint.get(workspace=ws, name="PipelineEndpointTest")
# run_id = pipeline_endpoint_by_name.submit("PipelineEndpointExperiment")
# print(run_id)

run_id = pipeline_endpoint_by_name.submit("PipelineEndpointExperiment", pipeline_version="2")
print(run_id)

