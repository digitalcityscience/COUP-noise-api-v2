import os
from noise_api.utils import load_json_file
from noise_api.models.calculation_input import NoiseCalculationInput, NoiseTask, BUILDINGS, ROADS


"""
Uses openapi.json (generated from fastapi + pydantic models) 
to create and serve OGC-API-PROCESSES compatible description jsons for 
- processes
- processes/process-id
- conformances
- landingpage
"""


def get_landingpage_json():
    return {
        "title": os.environ["APP_TITLE"],
        "description": "Simulate urban traffic noise for given roads and buildings. \
        Based on NoiseModelling v. by IFSTTAR",
        "links": [
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "The OpenAPI definition as JSON",
                "href": "/openapi.json"
            },
            {
                "rel": "conformance",
                "type": "application/json",
                "title": "Conformance",
                "href": "/conformance"
            },
            {
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/processes",
                "type": "application/json",
                "title": "Processes",
                "href": "/processes"
            },
        ]
    }


def get_processes(openapi_json: dict) -> dict:
    processes = []

    for path in openapi_json["paths"]:
        if post_request_info := openapi_json["paths"][path].get("post", None):
            if "process" in post_request_info["tags"]:
                processes.append(generate_process_description(openapi_json, path))

    return {"processes": processes}


def generate_process_description(openapi_json: dict, process_path: str) -> dict:
    print("generating process description for", process_path)

    for path in openapi_json["paths"]:
        if path == process_path:
            desc = {
                "id": path.split("processes/")[1].split("/")[0],
                "title": openapi_json["paths"][path]["post"]["summary"],
                "description": openapi_json["paths"][path]["post"]["summary"],
                "outputTransmission": ["value"],
                "jobControlOptions": ["async-execute"],
                "keywords": openapi_json["paths"][path]["post"]["summary"].split(" "),
                "inputs": {}
            }
            inputs_info_path = \
                openapi_json["paths"][path]["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"]
            inputs_class_name = inputs_info_path.split("/")[-1]
            inputs_info = openapi_json["components"]["schemas"][inputs_class_name]

            for input in inputs_info["properties"].keys():
                desc["inputs"][input] = {
                    "title": inputs_info["properties"][input]["title"],
                    "description": inputs_info["properties"][input].get(
                        "description",
                        inputs_info["properties"][input]["title"]
                    ),
                    "schema": inputs_info["properties"][input],
                    "minOccurs": int(input in inputs_info["required"]),
                    "maxOccurs": 1
                }

            desc["output"] = {
                "result": {
                    "geojson": {
                        "title": "Result geojson with column 'idiso' for the results in dB categories",
                        "description": "Noise levels are divided into 8 categories. Specified in the 'idiso' property.\
                            EU treshold for 'relevant' noise is 55db \
                            < 45 dB(A) ’ WHERE IDISO=0 \
                            45 <> 50 dB(A) ’ WHERE IDISO=1 \
                            50 <> 55 dB(A) ’ WHERE IDISO=2 \
                            55 <> 60 dB(A) ’ WHERE IDISO=3 \
                            60 <> 65 dB(A) ’ WHERE IDISO=4 \
                            65 <> 70 dB(A) ’ WHERE IDISO=5 \
                            70 <> 75 dB(A) ’ WHERE IDISO=6 \
                            '>' 75 dB(A) ’ WHERE IDISO=7",
                        "schema": {
                            "type": "object",
                            "contentMediaType": "application/geo+json",
                            "$ref": "https://geojson.org/schema/FeatureCollection.json"
                        }
                    }
                }
            }

            return desc


def get_conformance():
    return {
        "conformsTo": [
            "http://www.opengis.net/spec/ogcapi-processes/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-processes/1.0/conf/json",
        ]
    }


def get_openapi_examples():
    return {
        "without_global_traffic_settings": {
            "summary": "Without global traffic settings",
            "description": "Max speed and traffic loads as stated in 'roads' parameter will not be changed",
            "value": {
                "buildings": load_json_file(BUILDINGS),
                "roads": load_json_file(ROADS)
            }
        },
        "global_traffic_settings": {
            "summary": "Global traffic settings",
            "description": "Max speed and traffic quota[%] will be applied to all roads \
                           with 'traffic_settings_adjustable' == true",
            "value": {
                "max_speed": 42,
                "traffic_quota": 40,
                "wall_absorption": 0.23,
                "buildings": load_json_file(BUILDINGS),
                "roads": load_json_file(ROADS),
            }
        }
    }
