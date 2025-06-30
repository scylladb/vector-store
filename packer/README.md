# ScyllaDB vector service Image

## Requirements
- Packer >= v1.10.0
- Packer AWS and GCP plugins

To install the required plugins, run the following:

```shell
packer plugins install github.com/hashicorp/googlecompute
packer plugins install github.com/hashicorp/amazon
```

## Build
To build the ScyllaDB vector store Image, make sure you have [Authentication](#authentication) set up, and run the following command from the `vector-store/packer` directory:

```shell
packer build -var vector_version="0.1.0" vector-store-template.json
```
You can build a specific cloud only by using the `-only` flag. for example:
```shell
# AWS only
packer build -only=amazon-ebs -var vector_version="0.1.0" vector-store-template.json

# GCP only
packer build -only=googlecompute -var vector_version="0.1.0" vector-store-template.json
```
## Variables


The ScyllaDB vector store Image uses default variables that are declared in the packer template file, for example `aws_source_ami`, `gcp_project_id` etc.
You can override these default variables by creating a `variables.json` with the desired variable values, for example:

```json
{
  "vector_version": "0.1.0",
  "aws_subnet_id": "your_aws_subnet_id",
  "gcp_project_id": "your_gcp_project_id",
  "gcp_zone": "your_gcp_zone"
}
```
And when running the packer build command, include the `-var-file` option to specify the `variables.json` file:

```shell
packer build -var-file=variables.json vector-store-template.json
```


## Authentication

### AWS
Ensure `aws_access_key_id` and `aws_secret_access_key` are configured either in a local credentials file (ex. `~/.aws/credentials`) or as environment variables.

#### GCP
Set your GCP service account key as an environment variable:
```shell
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-file.json"
```
