
from google.cloud import dataproc_v1
 
def submit_cluster_job(project_id, region, cluster_name, properties, main_py, args):

    """

    Connects to Dataproc JobController and submits a PySpark job.

    """

    # Create the client with the regional endpoint

    client = dataproc_v1.JobControllerClient(

        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}

    )
 
    # Construct the job metadata

    job = {

        "placement": {"cluster_name": cluster_name},

        "pyspark_job": {

            "main_python_file_uri": main_py,

            "args": args,

            "properties": properties

        }

    }
 
    # Submit the request

    operation = client.submit_job(

        request={"project_id": project_id, "region": region, "job": job}

    )

    return operation.reference.job_id

