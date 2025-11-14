![Version](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2FCDCgov%2Fcfa-cloudops%2Frefs%2Fheads%2Fmain%2Fpyproject.toml&query=project.version&style=plastic&logoColor=lightGray&label=version)

![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&style=plastic&link=https://raw.githubusercontent.com/CDCgov/cfa_azure/refs/heads/master/.pre-commit-config.yaml)
![pre-commit](https://github.com/CDCgov/cfa_azure/workflows/pre-commit/badge.svg?style=plastic&link=https://github.com/CDCgov/cfa-cloudops/actions/workflows/pre-commit.yaml)
![CI](https://github.com/CDCgov/cfa_azure/workflows/Python%20Unit%20Tests%20with%20Coverage/badge.svg?style=plastic&link=https://github.com/CDCgov/cfa-cloudops/actions/workflows/pre-commit.yaml&link=https://github.com/CDCgov/cfa-cloudops/actions/workflows/ci.yaml)
![GitHub License](https://img.shields.io/github/license/cdcgov/cfa-cloudops?style=plastic&link=https://github.com/CDCgov/cfa_azure/blob/master/LICENSE)
![Python](https://img.shields.io/badge/python-3670A0?logo=python&logoColor=ffdd54&style=plastic)
![Azure](https://img.shields.io/badge/Microsoft-Azure-blue?logo=microsoftazure&logoColor=white&style=plastic)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/cdcgov/cfa-cloudops?style=plastic)

# cfa-cloudops
## Created by Ryan Raasch (Peraton) for CFA

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

View the documentation [here](docs/index.md) for more help getting started with the package.

To build documentation locally, clone this repo, navigate to the root of this repo, then run `poetry run mkdocs serve`.

## Warning
`cfa-cloudops` is intended for use on Linux or WSL environments. You may encounter issues when trying to install or use the package on Windows or Mac machines.

## Public Domain Standard Notice
This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License Standard Notice
The repository utilizes code licensed under the terms of the Apache Software
License and therefore is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](DISCLAIMER.md)
and [Code of Conduct](code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records Management Standard Notice
This repository is not a source of government records, but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).

## Additional Standard Notices
Please refer to [CDC's Template Repository](https://github.com/CDCgov/template) for more information about [contributing to this repository](https://github.com/CDCgov/template/blob/main/CONTRIBUTING.md), [public domain notices and disclaimers](https://github.com/CDCgov/template/blob/main/DISCLAIMER.md), and [code of conduct](https://github.com/CDCgov/template/blob/main/code-of-conduct.md).
