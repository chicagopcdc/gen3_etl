# gen3_etl
This repo contains the scripts that call the Gen3 API to import, export, and maintain the data in the D4CG data portal. 

## elasticsearch
Scripts to load data into the Gen3 ES index
### configuration
The ETL is controlled by environment variables. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BASE_URL` | `http://localhost` | Gen3 API base URL. |
| `PROJECT_LIST` | `["pcdc-20220808"]` | JSON array of projects to process. Set to a single-element array to run for one project only. |
| `TYPES` | `[]` | JSON array of node types to extract. Empty means all types. |
| `INDEX_NAME` | `pcdc_20220808` | Elasticsearch index name to load into. |
| `ES_HOST` | `localhost` | Elasticsearch host reachable from all Spark executors (hostname only, no `https://` prefix). |
| `ES_PORT` | `9200` | Elasticsearch port. Use `443` for AWS OpenSearch. |
| `ES_SCHEME` | `http` | Connection scheme. Set to `https` for AWS OpenSearch. |
| `ES_BULK_BATCH_SIZE` | `1000` | Records per bulk write batch. |
| `ES_BULK_MAX_TRIES` | `5` | Max retry attempts per bulk batch. |
| `ES_BULK_RETRY_DELAY` | `60` | Base delay in seconds between retries (multiplied by attempt number). |
| `ES_TIMEOUT` | `60` | Timeout in seconds for ES bulk write requests. |
| `SPARK_MASTER` | `local[*]` | Spark master URL. `local[*]` uses all local cores; set to the EMR master URL when running on a cluster. |
| `CREDENTIALS` | `../credentials.json` | Path to the Gen3 API credentials file. |
| `MAPPING_FILE` | `../files/nested_mapping.json` | Path to the Elasticsearch field mapping JSON file. |

To run for a single project, set `PROJECT_LIST` to a one-element JSON array:
```bash
export PROJECT_LIST='["pcdc-20220808"]'
python3 etl.py
```

Or inline for a one-off run:
```bash
PROJECT_LIST='["pcdc-20220808"]' python3 etl.py
```

When submitting as an EMR step, add it to the env-var block in the `Args` string (see step example below).

### load via AWS EMR
Load following files to an S3 bucket (s3://gen3-etl-smoke-test-973342646972/smoke/ in the playground aws account has been used for testing):
    - credentials.json (API key)
    - bootstrap.sh
    ```
    #!/bin/bash
    set -e
    python3 -m venv /home/hadoop/etl_venv
    source /home/hadoop/etl_venv/bin/activate
    # pyspark is provided by the EMR runtime and intentionally omitted here
    pip install gen3==4.5.0 python-dotenv "urllib3<2" requests elasticsearch
    ```
    - etl.py
    - transform.py
    - load.py
    - spark_utils.py

Note: `nested_mapping.json` does not need to be uploaded — it is generated at runtime by the transform step and written to the path set by `MAPPING_FILE`.

#### VPC / networking considerations
The cluster is pinned to a VPC by passing a `SubnetId` in `--ec2-attributes` (the subnet implicitly determines the VPC). A few things to verify before creating the cluster:

- **Subnet type**: a private subnet with a NAT gateway is recommended. The bootstrap needs outbound internet access to run `pip install`; the extract step needs to reach the Gen3 API. Without a NAT (or internet gateway for public subnets) both will fail.
- **DNS**: the VPC must have *DNS resolution* and *DNS hostnames* both enabled (VPC → Actions → Edit DNS settings). EMR requires this.
- **OpenSearch reachability**: since Spark executors write to OpenSearch directly, every node (master + core/task) must be able to reach the OpenSearch endpoint on port 443. Add an inbound rule to the OpenSearch security group allowing port 443 from the EMR-managed security groups.
- **S3 access**: add a [VPC gateway endpoint for S3](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html) to the subnet's route table. It is free, keeps S3 traffic off the internet, and avoids NAT data-transfer costs for bootstrap files, logs, and step files.
- **Elastic IP**: only needed if the Gen3 API (Sheepdog) is in a different VPC and protected by a firewall allowlist. If EMR is deployed in the same VPC as both OpenSearch and Sheepdog, all traffic is private and no elastic IP is required.

#### Elastic IP setup (cross-VPC / firewall-protected Gen3 only)
Skip this section if EMR is in the same VPC as OpenSearch and Sheepdog.

Allocate an elastic IP so the master node has a known stable public IP to add to the Gen3 ALB allowlist:
- `ALLOC_ID=$(aws ec2 allocate-address --domain vpc --profile pcdc_play --region us-east-2 --query AllocationId --output text)`
- `echo "$ALLOC_ID"`

Start the cluster:
- `aws ec2 create-key-pair --key-name emr-cluster-dev --profile pcdc_play --region us-east-2 --query 'KeyMaterial' --output text > emr-cluster-dev.pem`
- `chmod 400 emr-cluster-dev.pem`
- `aws emr create-default-roles --profile luca_dev --region us-east-1`
- `CLUSTER_ID=$(aws emr create-cluster --name "gen3-etl-test" --release-label emr-7.13.0 --applications Name=Spark --instance-type m5.xlarge --instance-count 1 --use-default-roles --ec2-attributes KeyName=emr-cluster-dev,SubnetId=<subnet-id> --bootstrap-actions Path=s3://gen3-etl-smoke-test-973342646972/smoke/bootstrap.sh --log-uri s3://gen3-etl-smoke-test-973342646972/logs/ --profile pcdc_play --region us-east-2 --query ClusterId --output text)`
- `echo "$CLUSTER_ID"`
- `aws emr describe-cluster --cluster-id $CLUSTER_ID --profile pcdc_play --region us-east-2 --query 'Cluster.Status.State' --output text`
- `MASTER_INSTANCE_ID=$(aws emr list-instances --cluster-id $CLUSTER_ID --profile pcdc_play --region us-east-2 --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text)`
- `echo "$MASTER_INSTANCE_ID"`
- `aws ec2 associate-address --instance-id $MASTER_INSTANCE_ID --allocation-id $ALLOC_ID --profile pcdc_play --region us-east-2`
- `aws ec2 describe-addresses --allocation-ids $ALLOC_ID --profile pcdc_play --region us-east-2 --query 'Addresses[0].{PublicIP:PublicIp,InstanceId:InstanceId,PrivateIP:PrivateIpAddress}' --output table`

#### Accessing the master node
**Same-VPC deployment (recommended)**: use AWS Systems Manager Session Manager — no public IP or open port 22 required:
- `aws ssm start-session --target $MASTER_INSTANCE_ID --profile pcdc_play --region us-east-2`
- Once on the node, set required env vars and run: `export ES_HOST='<opensearch-endpoint>' && export ES_PORT='443' && export ES_SCHEME='https' && export PROJECT_LIST='["pcdc-20260414"]' && export INDEX_NAME='pcdc_20260414' && /home/hadoop/etl_venv/bin/python3 etl.py`

**Cross-VPC / elastic IP deployment**: SSH via the elastic IP:
- Get your current IP to add to the security group: `curl -4 https://ifconfig.me`
- Look up the master node's security group: `aws ec2 describe-instances --instance-ids $MASTER_INSTANCE_ID --profile pcdc_play --region us-east-2 --query 'Reservations[0].Instances[0].SecurityGroups[].{Name:GroupName,GroupId:GroupId}' --output table`
- Authorize your IP (replace `<sg-id>` and `<your-ip>`): `aws ec2 authorize-security-group-ingress --group-id <sg-id> --protocol tcp --port 22 --cidr <your-ip>/32 --profile pcdc_play --region us-east-2`
- SSH in (replace `<elastic-ip>` with the public IP from the describe-addresses output above): `ssh -i "emr-cluster-dev.pem" hadoop@<elastic-ip>`
- Once on the node, set required env vars and run: `export ES_HOST='<opensearch-endpoint>' && export ES_PORT='443' && export ES_SCHEME='https' && export PROJECT_LIST='["pcdc-20260414"]' && export INDEX_NAME='pcdc_20260414' && /home/hadoop/etl_venv/bin/python3 etl.py`

Or you can send as a step / task for the EMR cluster for example:
```
cat > steps.json << EOF
[
  {
    "Type": "CUSTOM_JAR",
    "Name": "Test",
    "ActionOnFailure": "CONTINUE",
    "Jar": "command-runner.jar",
    "Args": [
      "bash", "-c",
      "{ export USER_API='https://portal-dev.pedscommons.org/user'; export FORCE_ISSUER='true'; export PROJECT_LIST='[\"pcdc-20260414\"]'; export INDEX_NAME='pcdc_20260414'; export ES_HOST='vpc-pcdc-dev-1-gen3-metadata-pwkasjp3g6sf6tkqys6m3senga.us-east-1.es.amazonaws.com'; export ES_PORT='443'; export ES_SCHEME='https'; export MAPPING_FILE='./nested_mapping.json'; aws s3 cp s3://gen3-etl-smoke-test-973342646972/smoke/credentials.json ./credentials.json && aws s3 cp s3://gen3-etl-smoke-test-973342646972/smoke/etl.py ./etl.py && aws s3 cp s3://gen3-etl-smoke-test-973342646972/smoke/transform.py ./transform.py && aws s3 cp s3://gen3-etl-smoke-test-973342646972/smoke/load.py ./load.py && aws s3 cp s3://gen3-etl-smoke-test-973342646972/smoke/spark_utils.py ./spark_utils.py && /home/hadoop/etl_venv/bin/python3 etl.py ; } > /tmp/output.txt 2>&1; aws s3 cp /tmp/output.txt s3://gen3-etl-smoke-test-973342646972/manual-logs/output.txt"
    ]
  }
]
EOF

aws emr add-steps --cluster-id $CLUSTER_ID --steps file://steps.json --region us-east-1 --profile luca_dev
```

Terminate the cluster:
- `aws emr terminate-clusters --cluster-ids $CLUSTER_ID --profile pcdc_play --region us-east-2`
- `aws emr describe-cluster --cluster-id $CLUSTER_ID --profile pcdc_play --region us-east-2 --query 'Cluster.Status.State' --output text`


Where used, the `allocation-id` is an elastic IP assigned to the EMR master node so it can connect with firewall-protected environments like dev and staging by adding its IP to the environment's ALB allowlist. This is only needed when EMR is not in the same VPC as Sheepdog.
Remember to update all IDs, subnet IDs, and endpoint names accordingly.


## graph
Scripts to import, export, and maintain data in the Gen3 graph db


# TODO
- terraform code to setup the basic infrastructure / networking / S3 used by EMR