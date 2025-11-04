import logging
import os
import subprocess as sp

import docker
from docker.errors import DockerException

logger = logging.getLogger(__name__)


def get_log_level() -> int:
    """Get the logging level from the LOG_LEVEL environment variable.

    Reads the LOG_LEVEL environment variable and returns the corresponding logging
    level constant. If the variable is not set, returns a value higher than CRITICAL
    to effectively disable logging. If the value is unrecognized, defaults to DEBUG.

    Returns:
        int: Logging level constant (e.g., logging.DEBUG, logging.INFO, etc.).

    Example:
        Set log level in your environment and retrieve it:

            os.environ["LOG_LEVEL"] = "info"
            level = get_log_level()
            logger.setLevel(level)

    Note:
        Recognized values (case-insensitive): none, debug, info, warning, warn, error, critical.
        Unrecognized values will trigger a warning and default to DEBUG.
    """
    log_level = os.getenv("LOG_LEVEL")

    if log_level is None:
        return logging.CRITICAL + 1

    match log_level.lower():
        case "none":
            return logging.CRITICAL + 1
        case "debug":
            logger.info("Log level set to DEBUG")
            return logging.DEBUG
        case "info":
            logger.info("Log level set to INFO")
            return logging.INFO
        case "warning" | "warn":
            logger.info("Log level set to WARNING")
            return logging.WARNING
        case "error":
            logger.info("Log level set to ERROR")
            return logging.ERROR
        case "critical":
            logger.info("Log level set to CRITICAL")
            return logging.CRITICAL
        case ll:
            logger.warning(f"Did not recognize log level string {ll}. Using DEBUG")
            return logging.DEBUG


def package_and_upload_dockerfile(
    registry_name: str,
    repo_name: str,
    tag: str,
    path_to_dockerfile: str = "./Dockerfile",
    use_device_code: bool = False,
):
    """Build a Docker image from a Dockerfile and upload it to Azure Container Registry.

    Builds a Docker image from the specified Dockerfile and uploads it to the given
    Azure Container Registry (ACR) repository with the provided tag. Supports device
    code authentication for environments without a web browser.

    Args:
        registry_name (str): Name of the Azure Container Registry (without .azurecr.io).
        repo_name (str): Name of the repository within the container registry.
        tag (str): Tag to assign to the uploaded Docker image (e.g., "latest", "v1.0").
        path_to_dockerfile (str, optional): Path to the Dockerfile to build. Default is "./Dockerfile".
        use_device_code (bool, optional): Whether to use device code authentication for Azure CLI login. Default is False.

    Returns:
        str: Full container image name that was uploaded, in the format "registry.azurecr.io/repo:tag".

    Raises:
        DockerException: If Docker is not running or cannot be reached.
        Exception: If the Dockerfile does not exist at the specified path.

    Example:
        Build and upload from default Dockerfile:

            image_name = package_and_upload_dockerfile(
                registry_name="myregistry",
                repo_name="batch-app",
                tag="v1.0"
            )
            print(f"Uploaded: {image_name}")

        Build from custom Dockerfile location with device code:

            image_name = package_and_upload_dockerfile(
                registry_name="myregistry",
                repo_name="data-processor",
                tag="latest",
                path_to_dockerfile="./docker/worker/Dockerfile",
                use_device_code=True
            )

    Note:
        This function requires Docker to be installed and the Azure CLI to be available and authenticated.
        The resulting image name is returned as a string for use in Azure Batch pools or jobs.
    """
    # check if Dockerfile exists
    logger.debug("Trying to ping docker daemon.")
    try:
        d = docker.from_env(timeout=10).ping()
        logger.debug("Docker is running.")
    except DockerException:
        logger.error("Could not ping Docker. Make sure Docker is running.")
        logger.warning("Container not packaged/uploaded.")
        logger.warning("Try again when Docker is running.")
        raise DockerException("Make sure Docker is running.") from None

    if os.path.exists(path_to_dockerfile) and d:
        full_container_name = f"{registry_name}.azurecr.io/{repo_name}:{tag}"
        logger.info(f"full container name: {full_container_name}")
        # Build container
        logger.debug("Building container.")
        sp.run(
            f"docker image build -f {path_to_dockerfile} -t {full_container_name} .",
            shell=True,
        )
        # Upload container to registry
        # upload with device login if desired
        if use_device_code:
            logger.debug("Logging in with device code.")
            sp.run("az login --use-device-code", shell=True)
        else:
            logger.debug("Logging in to Azure.")
            sp.run("az login --identity", shell=True)
        sp.run(f"az acr login --name {registry_name}", shell=True)
        logger.debug("Pushing Docker container to ACR.")
        sp.run(f"docker push {full_container_name}", shell=True)
        return full_container_name
    else:
        logger.error("Dockerfile does not exist in the root of the directory.")
        raise Exception(
            "Dockerfile does not exist in the root of the directory."
        ) from None


def upload_docker_image(
    image_name: str,
    registry_name: str,
    repo_name: str,
    tag: str = "latest",
    use_device_code: bool = False,
):
    """Upload an existing Docker image to Azure Container Registry.

    Tags a local Docker image and uploads it to the specified Azure Container Registry (ACR)
    repository with the provided tag. Supports device code authentication for environments
    without a web browser.

    Args:
        image_name (str): Name of the local Docker image to upload. Should match the name
            as shown in `docker images` output.
        registry_name (str): Name of the Azure Container Registry (without .azurecr.io).
        repo_name (str): Name of the repository within the container registry.
        tag (str, optional): Tag to assign to the uploaded Docker image. Default is "latest".
        use_device_code (bool, optional): Whether to use device code authentication for Azure CLI login. Default is False.

    Returns:
        str: Full container image name that was uploaded, in the format "registry.azurecr.io/repo:tag".

    Raises:
        DockerException: If Docker is not running or cannot be reached.
        docker.errors.ImageNotFound: If the specified image does not exist locally.

    Example:
        Upload a locally built image:

            image_name = upload_docker_image(
                image_name="my-local-app:latest",
                registry_name="myregistry",
                repo_name="batch-app",
                tag="v1.0"
            )
            print(f"Uploaded: {image_name}")

        Upload with device code authentication:

            image_name = upload_docker_image(
                image_name="data-processor:dev",
                registry_name="myregistry",
                repo_name="processors",
                tag="development",
                use_device_code=True
            )

    Note:
        This function requires Docker to be installed and the Azure CLI to be available and authenticated.
        The resulting image name is returned as a string for use in Azure Batch pools or jobs.
    """
    full_container_name = f"{registry_name}.azurecr.io/{repo_name}:{tag}"

    # check if docker is running
    logger.debug("Trying to ping docker daemon.")
    try:
        docker_env = docker.from_env(timeout=8)
        docker_env.ping()
        logger.debug("Docker is running.")
    except DockerException:
        logger.error("Could not ping Docker. Make sure Docker is running.")
        logger.warning("Container not uploaded.")
        logger.warning("Try again when Docker is running.")
        raise DockerException("Make sure Docker is running.") from None

    # Tagging the image with the unique tag
    logger.debug(f"Tagging image {image_name} with {full_container_name}.")
    try:
        image = docker_env.images.get(image_name)
        image.tag(full_container_name)
    except docker.errors.ImageNotFound:
        # Log available images to guide the user
        available_images = [img.tags for img in docker_env.images.list()]
        logger.error(
            f"Image {image_name} does not exist. Available images are: {available_images}"
        )
        raise

    # Log in to ACR and upload container to registry
    # upload with device login if desired
    if use_device_code:
        logger.debug("Logging in with device code.")
        sp.run("az login --use-device-code", shell=True)
    else:
        logger.debug("Logging in to Azure.")
        sp.run("az login --identity", shell=True)
    sp.run(f"az acr login --name {registry_name}", shell=True)
    logger.debug("Pushing Docker container to ACR.")
    sp.run(f"docker push {full_container_name}", shell=True)

    return full_container_name


def format_rel_path(rel_path: str) -> str:
    """Format a relative path into the correct format for Azure services.

    Removes leading forward slashes from relative paths to ensure compatibility
    with Azure service mount path requirements.

    Args:
        rel_path (str): Relative mount path that may contain a leading forward slash.

    Returns:
        str: The formatted relative path with leading forward slash removed if present.

    Example:
        Remove leading slash from path:

            formatted_path = format_rel_path("/data/input")
            print(formatted_path)  # "data/input"

        Path without leading slash is unchanged:

            formatted_path = format_rel_path("data/output")
            print(formatted_path)  # "data/output"

    Note:
        This function is used to ensure Azure storage mount paths are in the correct
        format expected by Azure services.
    """
    if rel_path.startswith("/"):
        rel_path = rel_path[1:]
        logger.debug(f"path formatted to {rel_path}")
    return rel_path
