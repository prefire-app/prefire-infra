#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.prefire_stack import PrefireStack

app = cdk.App()
PrefireStack(app, "PrefireStack")
app.synth()
