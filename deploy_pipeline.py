# deploy_pipeline.py

from databricks_cli.sdk import DeltaPipelinesService
from databricks_cli.sdk.api_client import ApiClient

# Configuration
config = {
    "pipeline_name": "banking-dlt-pipeline",
    "development": True,
    "continuous": False,
    "libraries": [
        {"notebook": {"path": "/pipelines/bronze_layer"}},
        {"notebook": {"path": "/pipelines/silver_layer"}},
        {"notebook": {"path": "/pipelines/gold_layer"}},
        {"notebook": {"path": "/pipelines/main_pipeline"}}
    ],
    "target": "banking.prod",
    "clusters": [
        {
            "label": "default",
            "num_workers": 2,
            "node_type_id": "Standard_D3_v2"
        }
    ],
    "configuration": {
        "pipelines.applyChangesPreviewEnabled": True,
        "pipelines.autoOptimize.managed": True
    }
}

def deploy_pipeline():
    api_client = ApiClient()
    pipelines_service = DeltaPipelinesService(api_client)
    
    # Create or update pipeline
    pipeline_spec = {
        "name": config["pipeline_name"],
        "development": config["development"],
        "continuous": config["continuous"],
        "libraries": config["libraries"],
        "target": config["target"],
        "clusters": config["clusters"],
        "configuration": config["configuration"]
    }
    
    response = pipelines_service.create_pipeline(pipeline_spec)
    print(f"Pipeline created with ID: {response['pipeline_id']}")
    return response

if __name__ == "__main__":
    deploy_pipeline()