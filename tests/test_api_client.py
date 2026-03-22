from app.clients.open_brewery import BreweryAPIClient


class DummyResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


def test_fetch_all_pages(monkeypatch):
    pages = {
        1: [{"id": "1"}, {"id": "2"}],
        2: [{"id": "3"}],
        3: [],
    }

    def fake_get(url, params, timeout):
        return DummyResponse(pages[params["page"]])

    monkeypatch.setattr("app.clients.open_brewery.requests.get", fake_get)

    client = BreweryAPIClient()
    payload = client.fetch_all()

    assert payload == [{"id": "1"}, {"id": "2"}, {"id": "3"}]
