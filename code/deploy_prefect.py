from prefect.deployments import Deployment, run_deployment
from ingest_data import ingest
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import json
import argparse


GCP_BUCKET_NAME = 'gh-pipeline-bucket'  # insert your  GCS bucket name
GCP_CREDS_LOCATION = 'code\gcp_credentials.json'

def create_blocks(gcp_key: dict) -> None:
    
    credentials_block = GcpCredentials(
        service_account_info = gcp_key  # enter your credentials info or use the file method.
    )
    credentials_block.save("gcp-creds", overwrite=True)

    bucket_block = GcsBucket(
        gcp_credentials = GcpCredentials.load("gcp-creds"),
        bucket = GCP_BUCKET_NAME,
    )
    bucket_block.save(GCP_BUCKET_NAME, overwrite=True)   

def deploy_flows(name: str, params: dict, cron: str) -> None:
    
    params["gcp_bucket_name"] = GCP_BUCKET_NAME
    
    if cron == None:
        name_flow = f"historical-{name}"
        deployment = Deployment.build_from_flow(
            flow = ingest,
            name = name_flow,
            parameters = params
        )
    else:
        name_flow = f"daily-{name}"
        deployment = Deployment.build_from_flow(
            flow = ingest,
            name = name,
            parameters = params
        )
    
    deployment.apply()
    
    if cron == None:
        run_deployment(name=f"ingest/{name_flow}")
        
def main(args):
    
    args.bucket_create = bool(args.bucket_create)
    if args.bucket_create == True:
        with open(GCP_CREDS_LOCATION) as file:
            gcp_key = json.load(file)
        create_blocks(gcp_key)
        
    params = json.loads(args.params)  
    
    deploy_flows(args.name, params, args.cron_schedule)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Prefect deployment")
    parser.add_argument('--name', required=True, help='Name of the subflow')
    parser.add_argument('--params', required=True, help='Flow parameters')
    parser.add_argument('--bucket_create', required=False, help='create prefect bucket (enter True if needed)')
    parser.add_argument('--cron_schedule', required=False, help='Repeating interval for the daily data ingestion')
    
    args = parser.parse_args()
    main(args)