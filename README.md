# gen3_etl
This repo contains the scripts that call the Gen3 API to import, export, and maintain the data in the D4CG data portal. 

## elasticsearch
Scripts to load data into the Gen3 ES index
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

Generate an elastic IP for the master node or deploy EMR in the same VPC as the env you are loading data to:
- `ALLOC_ID=$(aws ec2 allocate-address --domain vpc --profile pcdc_play --region us-east-2 --query AllocationId --output text)`
- `echo "$ALLOC_ID"`

Start the cluster:
- `aws ec2 create-key-pair --key-name emr-cluster-dev --profile pcdc_play --region us-east-2 --query 'KeyMaterial' --output text > emr-cluster-dev.pem`
- `chmod 400 emr-cluster-dev.pem`
- `CLUSTER_ID=$(aws emr create-cluster --name "gen3-etl-test" --release-label emr-7.13.0 --applications Name=Spark --instance-type m5.xlarge --instance-count 1 --use-default-roles --ec2-attributes KeyName=emr-cluster-dev --bootstrap-actions Path=s3://gen3-etl-smoke-test-973342646972/smoke/bootstrap.sh --log-uri s3://gen3-etl-smoke-test-973342646972/logs/ --profile pcdc_play --region us-east-2 --query ClusterId --output text)`
- `echo "$CLUSTER_ID"`
- `aws emr describe-cluster --cluster-id $CLUSTER_ID --profile pcdc_play --region us-east-2 --query 'Cluster.Status.State' --output text`
- `MASTER_INSTANCE_ID=$(aws emr list-instances --cluster-id $CLUSTER_ID --profile pcdc_play --region us-east-2 --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text)`
- `echo "$MASTER_INSTANCE_ID"`
- `aws ec2 associate-address --instance-id $MASTER_INSTANCE_ID --allocation-id $ALLOC_ID --profile pcdc_play --region us-east-2`
- `aws ec2 describe-addresses --allocation-ids $ALLOC_ID --profile pcdc_play --region us-east-2 --query 'Addresses[0].{PublicIP:PublicIp,InstanceId:InstanceId,PrivateIP:PrivateIpAddress}' --output table`

You can SSH and run the script directly on the machine:
- Get your current IP to add to the security group: `curl -4 https://ifconfig.me`
- Look up the master node's security group: `aws ec2 describe-instances --instance-ids $MASTER_INSTANCE_ID --profile pcdc_play --region us-east-2 --query 'Reservations[0].Instances[0].SecurityGroups[].{Name:GroupName,GroupId:GroupId}' --output table`
- Authorize your IP (replace `<sg-id>` and `<your-ip>`): `aws ec2 authorize-security-group-ingress --group-id <sg-id> --protocol tcp --port 22 --cidr <your-ip>/32 --profile pcdc_play --region us-east-2`
- SSH in (replace `<elastic-ip>` with the public IP from the describe-addresses output above): `ssh -i "emr-cluster-dev.pem" hadoop@<elastic-ip>`

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
      "{ export USER_API='https://portal-dev.pedscommons.org/user'; export FORCE_ISSUER='true'; aws s3 cp s3://<bucket>/smoke/credentials.json ./credentials.json && aws s3 cp s3://<bucket>/smoke/etl.py ./etl.py && /home/hadoop/etl_venv/bin/python3 etl.py ; } > /tmp/output.txt 2>&1; aws s3 cp /tmp/output.txt s3://<bucket>/manual-logs/output.txt"
    ]
  }
]
EOF

aws emr add-steps --cluster-id $CLUSTER_ID --steps file://steps.json
```

Terminate the cluster:
- `aws emr terminate-clusters --cluster-ids $CLUSTER_ID --profile pcdc_play --region us-east-2`
- `aws emr describe-cluster --cluster-id $CLUSTER_ID --profile pcdc_play --region us-east-2 --query 'Cluster.Status.State' --output text`


Where the `allocation-id` is an elastic IP assigned to the EMR master node so it can connect with firewall protected ENVs like dev and staging by adding it's IP to the ENV ALB.
And remember to update all the IDs and ENV names accordingly.


## graph
Scripts to import, export, and maintain data in the Gen3 graph db
