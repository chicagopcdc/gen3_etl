""" Shared Spark session helper for extract/transform/load """
import os


def get_spark_session() -> 'SparkSession':
    """
    Get (or create) the Spark session used to distribute ETL work across a cluster.

    Passes HOME to YARN executor containers via spark.executorEnv.HOME so that
    Gen3Auth (which calls os.makedirs(os.path.expanduser('~') + '/.cache/...'))
    resolves to the writable hadoop user home (/home/hadoop) rather than the
    unwritable parent directory (/home) that YARN sometimes injects as HOME.
    """
    from pyspark.sql import SparkSession
    # Use /tmp as HOME for executor containers: YARN mounts /home/hadoop read-only
    # so any library that writes to $HOME/.cache (e.g. Gen3Auth token cache) will
    # fail. /tmp is always writable inside a YARN container.
    builder = SparkSession.builder \
        .appName('gen3_etl') \
        .master(os.environ.get('SPARK_MASTER', 'local[*]')) \
        .config('spark.executorEnv.HOME', '/tmp') \
        .config('spark.yarn.appMasterEnv.HOME', '/tmp')

    # Propagate AWS credentials to YARN executor containers so boto3 can sign
    # OpenSearch requests with the same IAM user the driver uses. Without this,
    # executors fall back to the instance profile (EMR_EC2_DefaultRole) which may
    # not have OpenSearch permissions.
    for _var in ('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',
                 'AWS_DEFAULT_REGION'):
        _val = os.environ.get(_var, '')
        if _val:
            builder = builder.config(f'spark.executorEnv.{_var}', _val)

    return builder.getOrCreate()
