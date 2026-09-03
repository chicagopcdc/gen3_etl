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
    pip install gen3==4.5.0 python-dotenv "urllib3<2" requests elasticsearch
    ```
    - the diagnostic script you sent me

Generate an elastic IP for the master node or deploy EMR in the same VPC as the env you are loading data to:
- `aws ec2 allocate-address --domain vpc`

Start the cluster:
- `aws ec2 create-key-pair --key-name emr-cluster-dev --profile pcdc_play --region us-east-2 --query 'KeyMaterial' --output text > emr-cluster-dev.pem`
- `chmod 400 emr-cluster-dev.pem`
- `CLUSTER_ID=$(aws emr create-cluster --name "gen3-etl-test" --release-label emr-7.13.0 --applications Name=Spark --instance-type m5.xlarge --instance-count 1 --use-default-roles --ec2-attributes KeyName=emr-cluster-dev --bootstrap-actions Path=s3://gen3-etl-smoke-test-973342646972/smoke/bootstrap.sh --log-uri s3://gen3-etl-smoke-test-973342646972/logs/ --profile pcdc_play --region us-east-2 --query ClusterId --output text)`
- `echo "$CLUSTER_ID"`
- `aws emr describe-cluster --cluster-id j-03061582532RL3A6SFGC --profile pcdc_play --region us-east-2 --query 'Cluster.Status.State' --output text`
- `MASTER_INSTANCE_ID=$(aws emr list-instances --cluster-id j-03061582532RL3A6SFGC --profile pcdc_play --region us-east-2 --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text)`
- `echo "$MASTER_INSTANCE_ID"`
- `aws ec2 associate-address --instance-id i-01f5818109eff4322 --allocation-id eipalloc-0a32d5da8e69c9cae --profile pcdc_play --region us-east-2`
- `aws ec2 describe-addresses --allocation-ids eipalloc-0a32d5da8e69c9cae --profile pcdc_play --region us-east-2 --query 'Addresses[0].{PublicIP:PublicIp,InstanceId:InstanceId,PrivateIP:PrivateIpAddress}' --output table`

You can SSH and run the script directly on the machine:
```
- `ssh -i "emr-cluster-dev.pem" hadoop@3.140.55.62`
    - Get my current ip to add to the sec group to be able to ssh to it: `curl -4 https://ifconfig.me`
    - `aws ec2 authorize-security-group-ingress --group-id sg-071e060206bca8ab0 --protocol tcp —port 22 --cidr 205.208.121.123/32 --profile pcdc_play --region us-east-2`
- `aws ec2 describe-instances --instance-ids i-01f5818109eff4322 --profile pcdc_play --region us-east-2 --query 'Reservations[0].Instances[0].SecurityGroups[].{Name:GroupName,GroupId:GroupId}' --output table`
```

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
      "{ export USER_API='https://portal-dev.pedscommons.org/user'; export FORCE_ISSUER='true'; aws s3 cp s3://<bucket>/smoke/credentials.json ./credentials.json && aws s3 cp s3://<bucket>/smoke/luca_env_test.py ./luca_env_test.py && /home/hadoop/etl_venv/bin/python3 luca_env_test.py ; } > /tmp/output.txt 2>&1; aws s3 cp /tmp/output.txt s3://<bucket>/manual-logs/output.txt"
    ]
  }
]
EOF

aws emr add-steps --cluster-id $CLUSTER_ID --steps file://steps.json
```

Terminate the cluster:
- `aws emr terminate-clusters   --cluster-ids j-03061582532RL3A6SFGC   --profile pcdc_play   --region us-east-1`
- `aws emr describe-cluster --cluster-id j-03061582532RL3A6SFGC --profile pcdc_play --region us-east-2 --query 'Cluster.Status.State' --output text`


Where the `allocation-id` is an elastic IP assigned to the EMR master node so it can connect with firewall protected ENVs like dev and staging by adding it's IP to the ENV ALB.
And remember to update all the IDs and ENV names accordingly.


## graph
Scripts to import, export, and maintain data in the Gen3 graph db
