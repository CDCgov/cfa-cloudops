import pytest

from cfa.cloudops.scripts import create_pool


@pytest.fixture
def mock_arguments(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        [
            "script_name.py",
            "--dotenv_path",
            ".env.test",
            "--use_sp",
            "--use_federated",
            # "--job_name", "test-job",
            "--pool_name",
            "test-pool",
            "--container_image_name",
            "test-image",
        ],
    )


def test_create_pool(mocker, mock_arguments):
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.__init__", return_value=None)
    mocker.patch("cfa.cloudops._cloudclient.CloudClient.create_pool", return_value=None)
    create_pool()
