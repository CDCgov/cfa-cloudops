from cfa.cloudops import CloudClient

if __name__ == "__main__":
    client = CloudClient(dotenv_path="metaflow.env")
    client.package_and_upload_dockerfile(
        registry_name="cfaprdbatchcr",
        repo_name="cfa-metaflow",
        tag="latest",
        path_to_dockerfile="./BKDockerfile",
    )
