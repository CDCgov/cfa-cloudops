# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).
The versioning pattern is `major.minor.patch`.

---
## v0.1.3
- added support for installation in Windows environments

## v0.1.2
- added download_task_output parameter to `CloudClient.monitor_job()` to download stdout and stderr of each task when the task completes.

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
