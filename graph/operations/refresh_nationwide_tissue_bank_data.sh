#!/bin/bash
aws lambda invoke --function-name nationwide-tissue-bank-staging-data_pull_service-lambda --profile nationwide-tissue-bank-staging --invocation-type Event --region us-east-1 --payload '{}' response.json
