from app.integrations.bundle import Integrations


def test_bundle_holds_dummyjson_client():
    dummyjson = object()
    bundle = Integrations(dummyjson=dummyjson)
    assert bundle.dummyjson is dummyjson
