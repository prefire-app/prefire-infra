from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from constructs import Construct

'''
CDK stack for the Prefire API. This defines the S3 buckets, Lambda function, and API Gateway.
- COG_BUCKET: The S3 bucket where the Cloud Optimized GeoTIFFs are stored. This is read-only for the Lambda.
- OUTPUT_BUCKET: The S3 bucket where the Lambda writes the extracted subset COGs. This bucket has a lifecycle rule to automatically delete objects after 1 day.
'''
class PrefireStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # set up env variables and bucket names
        env = self.node.try_get_context("env") or "dev"
        bucket_name = "prefire-prod-cog" if env == "prod" else "prefire-dev-cog"
        cog_bucket = s3.Bucket.from_bucket_name(self, "CogBucket", bucket_name)

        output_bucket = s3.Bucket(self, "OutputBucket", bucket_name=f"prefire-{env}-output",
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(1))]
        )

        fn = _lambda.DockerImageFunction(
            self, "CogHandler",
            code=_lambda.DockerImageCode.from_image_asset("."),
            environment={
                "COG_BUCKET": cog_bucket.bucket_name,
                "OUTPUT_BUCKET": output_bucket.bucket_name,
                "ENV": env,
            },
            memory_size=512,
            timeout=Duration.seconds(30),
        )

        # grant permissions
        cog_bucket.grant_read(fn)
        output_bucket.grant_read_write(fn)  # write subset, then sign the URL

        api = apigw.LambdaRestApi(self, "CogApi", handler=fn,
            deploy_options=apigw.StageOptions(stage_name=env),
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=apigw.Cors.ALL_METHODS,
            )
        )

        CfnOutput(self, "ApiUrl", value=api.url, description="API Gateway endpoint URL")
