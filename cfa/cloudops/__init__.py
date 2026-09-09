import datetime
import logging
import warnings

from .helpers import configure_logging

warnings.filterwarnings(
    "ignore",
    category=SyntaxWarning,
    module=r"azure\.mgmt\.(compute|web)\.models\._models",
)
logging.getLogger("azure.identity._credentials.environment").setLevel(logging.ERROR)


__all__ = [
    "CloudClient",
    "ContainerAppClient",
    "FunctionAppClient",
    "run_experiment",
    "run_tasks",
    "Task",
]


def __getattr__(name):
    if name == "CloudClient":
        from ._cloudclient import CloudClient

        return CloudClient
    if name == "ContainerAppClient":
        from ._containerappclient import ContainerAppClient

        return ContainerAppClient
    if name == "FunctionAppClient":
        from ._function_app_client import FunctionAppClient

        return FunctionAppClient
    if name in {"run_experiment", "run_tasks"}:
        from .automation import run_experiment, run_tasks

        return run_experiment if name == "run_experiment" else run_tasks
    if name == "Task":
        from .batch_helpers import Task

        return Task
    raise AttributeError(f"module 'cfa.cloudops' has no attribute '{name}'")


logger = logging.getLogger(__name__)
run_time = datetime.datetime.now()
configure_logging()
