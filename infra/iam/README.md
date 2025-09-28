## AWS IAM policies (samples)

Attach these to the role used by pulsar_neuron in AWS (EC2/Batch/Lambda/ECS).
Scope down ARNs to your account/region.

- `policy-aws-secrets-manager.json`: read a specific secret by name/ARN.
- `policy-aws-ssm-parameter-store.json`: read parameters under a given prefix.

> IMPORTANT: Principle of least privilege — restrict Resources to exact ARNs.
