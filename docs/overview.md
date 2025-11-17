# `cfa-cloudops` Overview

# Outline
- [Description](#description)
- [Getting Started](#getting-started)

# Description
The `cfa-cloudops` python module is intended to ease the challenge of working in the cloud (currently limited to Azure) for data scientists at CFA. Typically, it takes deep knowledge to authenticate and complex coding patterns to interact with the cloud via python, which takes away time and resources from data scientists doing predictions, modeling, and more impactful work. `cfa-cloudops` simplifies many repeated workflows when interacting with the cloud and unifies CFA's approach in developer interaction with the cloud. For example, creating a pool and running jobs in Azure may take several credentials and different clients  to complete, but with `cfa-cloudops`, this is reduced to a small number of functions with only a few user-provided parameters.

# Getting Started
To use `cfa-cloudops`, you need [Python 3.9 or higher](https://www.python.org/downloads/), [Azure CLI](https://learn.microsoft.com/en-us/cli/azure), and any python package manager.

Currently `cfa-cloudops` is only available to install from GitHub.

To install using pip:
```bash
pip install git+https://github.com/CDCgov/cfa-cloudops.git
```

# Components of `cfa-cloudops`

There are several components of this repo that provide benefits to developers interacting with the Cloud (currently Azure only). They are:

- low-level python functions to assist in cloud interaction
  - auth
  - blob
  - client
  - endpoints
  - job
  - task
- CloudClient object for easy interaction with the cloud
  - more info found [here](./CloudClient/index.md)

- automation component to run jobs/tasks from a configuration file
