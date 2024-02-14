# CUT Prototype Noise API V2
It offers an OGC-API-Processes conform service, with geojson based inputs/outputs to simulate urban traffic noise.

In general this is a celery-based app using 3 containers each: api,worker, redis. The api container excepts requests and creates tasks in the redis db. Workers look for tasks in the redis db, calculate result and publish it in redis. Results (also cached ones) can be accessed via the api container that get’s the result from the redis db.


## Traffic-Noise Simulation

The noise module is used for investigating traffic noise patterns. The goal of the module is to identify the areas of the neighborhood exposed to high noise levels. The noise simulation is adapted from the software [NoiseModelling](https://noise-planet.org/noisemodelling.html), a free and open-source tool (GPL 3) for producing environmental noise maps
using a simplified implementation of the French national method NMPB-08.

The software is developed by the French Institute of Science and Technology for Transport, Development and Networks (Ifsttar).

### Simulation Inputs

The inputs for the simulation are buildings and streets. The buildings are represented as 2D building footprints saved as a GeoJSON.

For the street network, a GeoJSON of the streets and rails is needed that includes values for the planned traffic volume and traffic speed. Custom inputs for traffic quota and max speed will be applied to all roads with a property traffic_settings_adjustable: true

#### Traffic Quota

Volume of motorized traffic (cars, trucks). Selecting 100% shows the traffic volume according to current planning assumptions (predicted traffic volume). Selecting 25%, for example, shows 25% of the planned traffic volume specified in the streets geojson.

#### Max speed

Max speed value in [km/h] that will be applied to the streets.

#### Wall absorption

["wall_absorption"] float value between 0-1 to indicate wall absorption qualities. The higher the more absorption.

#### Request
See example requests on /noise/docs
To run a simulation
```
curl --location --request POST 'http://localhost:{APP_PORT}/noise/traffic-noise/execution' \
--header 'Content-Type: application/json' \
--data-raw '{
   "max_speed": 42, "traffic_quota": 40, "wall_absorption": 0.23", "buildings": BUILDINGS
}
```
with BUILDINGS being a geojson like _noise_api/models/jsons/buildings.json_


### Results
Noise levels are divided into 8 categories. Specified in the "idiso" property of the result geojson.

EU treshold for "relevant" noise is 55db

 < 45 dB(A) ’ WHERE IDISO=0

 45 <> 50 dB(A) ’ WHERE IDISO=1

 50 <> 55 dB(A) ’ WHERE IDISO=2

 55 <> 60 dB(A) ’ WHERE IDISO=3

 60 <> 65 dB(A) ’ WHERE IDISO=4

 65 <> 70 dB(A) ’ WHERE IDISO=5

 70 <> 75 dB(A) ’ WHERE IDISO=6

 '>' 75 dB(A) ’ WHERE IDISO=7

Example result
![example_result.png](example_result.png)

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

