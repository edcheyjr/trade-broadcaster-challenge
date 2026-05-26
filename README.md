# Advanced Computing Challenge

## General instructions

* The solution should be inside trading-broadcaster folder
* DO NOT MODIFY the pyproject.toml or poetry.lock dependencies as that may result in a test failure. You can update the build commands as long as it continues to work in the provided sample.
* A sample of the API to list the available symbols is provided in the symbol-api folder. In this folder you can see all endpoints provided by the Symbol API. There is no need to change anything in that folder, and any change in that folder may result in a test failure.
* A sample of the data-provider is available on data-provider folder. You can use that in your tests

## Build the solution
This solution requires Python 3.10 and Poetry 1.1 installed.

To build the solution just install the python dependencies
```shell
poetry install
```

After finishing your challenge make sure the the build step is working.

## Run solution locally
To run the solution locally just run this command (symbol-api will run on port 3000, trading broadcaster on port 9000 and data provider on port 9001 by default)
```shell
poetry run python app.py
```