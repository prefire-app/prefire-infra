# prefire-infra
Infrastructure for prefire app

# /lambda
Deployment code for the API COG retriever lambda

## Deployment

`cdk bootstrap`

`cdk synth`

Docker desktop needs to be open
`cdk deploy --context env=dev`

## Testing

### Integration test

`python test/aws_test.py`