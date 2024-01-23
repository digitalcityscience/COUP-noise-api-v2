import json
import time
from pathlib import Path

import pytest
import geopandas

TEST_CASES_DIR = Path(__file__).parent / "test_cases"


def load_test_cases(directory: Path) -> list[dict]:
    json_data_list = []

    # Iterate over each JSON file in the directory
    for file_path in directory.glob("*.json"):
        # Read and parse the JSON file
        try:
            with open(file_path, "r") as file:
                data = json.loads(file.read())
                json_data_list.append(data)
        except Exception as e:
            return f"Error reading file {file_path.name}: {e}"

    print(f"test cases {json_data_list}")

    return json_data_list


@pytest.mark.parametrize(
    "test_case",
    load_test_cases(TEST_CASES_DIR),
)
def test_noise_calculation(unauthorized_api_test_client, test_case):
    with unauthorized_api_test_client as client:
        response = client.post(
            "/noise/processes/traffic-noise/execution", json=test_case["request"]
        )
        assert response.status_code == 200
        job_id = response.json()["job_id"]

        time.sleep(60)

        response = client.get(f"/noise/jobs/{job_id}/results")
        result = response.json()["result"]

        # assert result == test_case["response"]["result"]
        print(test_case["name"])
        gdf = geopandas.GeoDataFrame.from_features(result["geojson"]["features"])
        assert round(gdf["value"].max(), 2) == test_case["test_stats"]["max_value"]
        assert round(gdf["value"].mean(), 2) == test_case["test_stats"]["mean_value"]

