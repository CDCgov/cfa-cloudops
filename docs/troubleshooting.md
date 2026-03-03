# Troubleshooting

## Potential Issues and Solutions

### Authentication/Credential Error

The default authentication method for the `CloudClient` is a Managed Identity. If your Managed Identity on your VM is not setup at all or not setup correctly, you will experience issues authenticating.

Solution: confirm your VM has the right Managed Identity setup for the Azure environment. If working at CFA, please reach out to the CFA Tools Teams. An easy way to check your Managed Identity is to run `az login --identity` in your terminal.


### Error Instanstiating CloudClient

If you experience when creating an instance of `CloudClient()` using a .env file, it's possible the issue is coming from the .env file itself. Make sure the keys in your .env file match exactly with the keys in the sample .env. If all keys are present, it's likely an issue with a value in the .env. Confirm all values are correct.

### File Not Found During Job

If you are interacting with files during a job and getting errors like a file is not found, it can be originating from two places:
1. incorrect mount reference. The blob container should be mounted during pool creation and referenced at the root of the Docker container. For example, a container called `my-container` would be referenced as `/my-container` in code.
2. file not present in container. If you are a referencing a file that should exist in your Docker container, confirm the path where it exists. Note that Docker sets a working directory so any relative paths will start from the working directory specified in your Docker container.
