import json
import pytest
from pathlib import Path

data_dir = Path(__file__).parent / "data"


@pytest.fixture
def traffic_noise_desc():
    with open(data_dir / "traffic-noise_ogc_process_description.json", "r") as f:
        return json.load(f)


def test_noise_calculation(unauthorized_api_test_client, traffic_noise_desc):
    with unauthorized_api_test_client as client:
        response = client.get(
            "/noise/processes/traffic-noise"
        )

        with open("data/traffic-noise_ogc_process_description.json", "w") as f:
            json.dump(response.json(), f)
        assert response.status_code == 200
        assert traffic_noise_desc == response.json()
