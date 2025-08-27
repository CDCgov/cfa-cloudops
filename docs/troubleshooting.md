# Troubleshooting

## Potential Issues and Solutions

### Authentication/Credential Error

The default authentication method for the `CloudClient` is a Managed Identity. If your Managed Identity on your VM is not setup at all or not setup correctly, you will experience issues authenticating.

Solution: confirm your VM has the right Managed Identity setup for the Azure environment. If working at CFA, please reach out to the CFA Tools Teams. An easy way to check your Managed Identity is to run `az login --identity` in your terminal.
