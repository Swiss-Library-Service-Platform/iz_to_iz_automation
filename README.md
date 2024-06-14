# IZ to IZ automation
* Author: Raphaël Rey (raphael.rey@slsp.ch)
* Date: 2024-06-14
* Version: 1.0.0

## Description

This project is an automation script for the Speicherbibliothek.
It is used to automate the process of copying items from one IZ to the VKSS IZ.

## Usage

To start the workflow with a small dataset, run the following command:

```bash
python start_process.py -size SMALL
```

To start the workflow with a large dataset, run the following command:

```bash
python start_process.py -size LARGE
```

## Installation
.env file is required to run the script. The file should contain the access to the
SFTP server. An .env file is available in main directory for
productive environment. A second .env file is available in the test directory for
testing environment.


```bash
pip install -r requirements.txt
```

The almapiwrapper library requires the use of
API keys. They need to be configured according
to the documentation: https://almapi-wrapper.readthedocs.io/en/latest/

The configurations are available in the `config.py` file.
For running the tests it could be necessary to change the `LARGE_TASK_HOUR`
variable.

## License
GNU General Public License v3.0
