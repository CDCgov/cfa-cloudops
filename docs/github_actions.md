# Using `cfa-cloudops` with GitHub Actions

The `cfa-cloudops` package was designed to require minimal changes when switching from local development to GitHub Actions execution. Please refer to [this example repo](https://github.com/cdcent/cfa-cloudops-example) for help structuring your GitHub repo to use `cfa-cloudops`. For this documentation, it is assumed the main way of using `cfa-cloudops` is with the `CloudClient`. 

## Initializing

Existing code from a local workflow can easily transferred to run in GitHub Actions. The main difference between execution environments is the initialization of the `CloudClient`. We use the parameter `use_federated=True` when initializing the client to coordinate with GitHub to use federated credentials. When we login with federated credentials during the workflow (more on this later), the client will then pick these up to use for authentication.

### Example

```python
cc = CloudClient(use_federated = True)
```

## Secrets

Just like using a .env file for local `cloudops` execution, GitHub needs a way to store/access environment variables. Because of the possible sensitivity of some of these values, we cannot store the .env file directly as a file in the repo. The values in the .env need to be included as Secrets in your repository's Actions Secrets And Variables section within the repo Settings. More info on this can be found [here](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets).

## Workflows

