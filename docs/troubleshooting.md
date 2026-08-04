# Troubleshooting

## Potential Issues and Solutions

### Authentication/Credential Error

The default authentication method for the `CloudClient` is a Managed Identity. If your Managed Identity on your VM is not setup at all or not setup correctly, you will experience issues authenticating.

Solution: confirm your VM has the right Managed Identity setup for the Azure environment. If working at CFA, please reach out to the CFA Tools Teams. An easy way to check your Managed Identity is to run `az login --identity` in your terminal.

### Unexpected Logging Output

The package logging behavior can be configured with environment variables.

- `LOG_LEVEL`: controls logging verbosity.
	- Supported values: `none`, `debug`, `info`, `warning`/`warn`, `error`, `critical`.
	- Default when unset: `warning`.
	- To disable package logging, set `LOG_LEVEL=none`.
- `LOG_OUTPUT`: controls where logs are written.
	- `stdout` (or unset): write logs to stdout.
	- `file`: write logs to `./logs/<timestamp>.log`.
	- `both`: write logs to both stdout and file.

If logs are not appearing as expected, verify these environment variables are set correctly for your execution context (.env, shell session, or CI workflow).
