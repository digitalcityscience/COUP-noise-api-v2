# CUT Prototype Noise API V2


## Local Dev

### Initial Setup

The `CUT Prototype Noise API V2` is run on `Docker`, however it is still necessary to create a `Python` virtual environment to run tests and enable linting for pre-commit hooks. Run the following command to set up your environment: 


```
$ make venv
```

This command will create a virtualenv, install all dependencies including pre-commit hooks and create a `.env` file based on `./.env.example`. 

After the command runs, make sure to adapt your `.env` file with secure secrets, etc.  If your `IDE` does not activate your virtualenv automatically, run: 

```
$ source .venv/bin/activate
```

> [!IMPORTANT]
> This repository uses `Makefile` to run commands, in case you can't use Make, just run the correspondent commands as in [this file](./Makefile).


### Running the API

To run the API: 

```
$ make start
```

After the image is built and containers initialise, you can access the following in your browser: 

| Service    | URL                                | Access                                      |
|------------|------------------------------------|---------------------------------------------|
| Swagger UI | http://0.0.0.0:8002/noise/docs           | Not password protected                       |
| Redoc      | http://0.0.0.0:8002/noise/redoc          | Not password protected                       |
| OpenAPI    | http://0.0.0.0:8002/noise/openapi.json   | Not password protected                       |

### Tests 

To run the Docker container in interactive mode:

```bash
make test-it
```

Once the container terminal is available, to run tests: 

```bash
pytest
```

To run tests only, without interactive mode: 

```bash
make test-docker
```

### Formating/ linting code

```
$ make fmt
```

```
$ make lint
```

