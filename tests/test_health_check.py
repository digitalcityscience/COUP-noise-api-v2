def test_health_check(unauthorized_api_test_client):
    with unauthorized_api_test_client as client:
        response = client.get("/noise/health_check")
        assert response.status_code == 200
        assert response.json() == "ok"
