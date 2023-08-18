from prefect.deployments import Deployment
from ingest_data import ingest
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import json

def create_blocks(gcp_key: dict, gcs_bucket_name: str) -> None:
    credentials_block = GcpCredentials(
        service_account_info=gcp_key  # enter your credentials info or use the file method.
    )
    credentials_block.save("gcp-creds", overwrite=True)

    bucket_block = GcsBucket(
        gcp_credentials=GcpCredentials.load("gcp-creds"),
        bucket=gcs_bucket_name,  # insert your  GCS bucket name
    )
    bucket_block.save("de-gcs", overwrite=True)   

def deploy_flows(params: dict):
    deployment = Deployment.build_from_flow(
        flow = ingest,
        name = "Data ingestion to GCS",
        parameters = params
    )

    deployment.apply()

if __name__ == "__main__":
    with open("code\gcp_credentials.json") as file:
        gcp_key = json.load(file)
    gcs_bucket_name = "prefect-de-zoomcamp-marcel"
    deploy_flows()
    create_blocks(gcp_key, gcs_bucket_name)