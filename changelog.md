# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).
The versioning pattern is `major.minor.patch`.

---
## v0.2.9
- fixed issue when no mounts were provided during pool creation
- added extra checks for mount strings
- added replace_existing_pool flag in create_pool() to reduce accidental overriding of pools

## v0.2.8
- removed `read_only` parameter from blob upload functions since this is now being implemented through `update_blob_protection` operation

## v0.2.7
- add `legal_hold` and `immutability_lock_days` options to `blob.py` for Blob protection. Also added `update_blob_protection` operation to CloudClient

## v0.2.6
- add tagging automation on merges to main

## v0.2.5
- change location of Task in imports. It is now located in cfa.cloudops.

## v0.2.4
- fixing ns packaging and added pre-commit hook to keep the `__init__.py` file out of ./cfa/

## v0.2.3
- add `list_available_images` method to CloudClient for listing all verified Docker images supported by Azure Batch

## v0.2.2
- removed the option to turn on or off dependencies at the job level. The option for dependencies is always on going forward

## v0.2.1
- added tags to uploaded blobs and filtering feature for blobs by tag expression

## v0.2.0
- switched to async ContainerClient for asynchrnonous folder downloads and uploads

## v0.1.9
- added `add_task_collection` method to CloudClient for creating multiple tasks as a batch

## v0.1.8
- changed task working directory to container image's working directory

## v0.1.7
- updated/added documentation

## v0.1.6
- added ability to install on windows

## v0.1.5
- switched to async BlobServiceClient for asynchrnonous folder downloads and uploads

## v0.1.4
- fix issue with mounts in add_task method.

## v0.1.3
- added unit tests in `tests` folder.

## v0.1.2
- added download_task_output parameter to `CloudClient.monitor_job()` to download stdout and stderr of each task when the task completes

## v0.1.1
- increase logging throughout the library

## v0.1.0
- significant changes to mounting structure in create_pool and add_task

## v0.0.9
- change pre-commit to enforce LF over CRLF line endings

## v0.0.8
- added ruff.toml config file for pre-commit fixes

## v0.0.5
- changed type hints for command_line
- add a credential check to CloudClient

## v0.0.4
- Added ability to use federated credentials
- Fixed SP authentication

## v0.0.3
- Added CLI capabilities

## v0.0.2

- Updated ContainerAppClient
- New documentation for ContainerAppClient

## v0.0.1

- Added CloudClient, ContainerAppClient, and other modules
- Added documentation and examples
- Added other structure to repo
