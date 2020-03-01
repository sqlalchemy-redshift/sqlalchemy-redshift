from botocore.stub import Stubber

import boto3
import pytest
import sqlalchemy as sa
import sqlalchemy.engine.url as _url


@pytest.fixture(autouse=True)
def redshift_mock():
    client = boto3.client("redshift")
    return [client, Stubber(client)]


def prepare_stub(stubber, user, db, cluster_id, endpoint_dns, port):
    stubber.deactivate()
    response = {"Clusters": [{"Endpoint": {"Address": endpoint_dns, "Port": port}}]}
    stubber.add_response("describe_clusters", response)

    stubber.add_response(
        "get_cluster_credentials",
        {"DbUser": "IAM:{}".format(user), "DbPassword": "randompassword"},
        {
            "DbUser": user,
            "DbName": db,
            "ClusterIdentifier": cluster_id,
            "AutoCreate": False,
        },
    )
    stubber.activate()


def test_url_transformations(redshift_mock):
    redshift, stubber = redshift_mock

    # Create a test engine
    engine = sa.create_engine("redshift+psycopg2://test")

    basic_url = _url.make_url("redshift://the_user@clusterid/the_database?iam=true")
    args = basic_url.translate_connect_args(username="user")
    assert args["host"] == "clusterid"
    assert not "password" in args

    prepare_stub(
        stubber,
        args["user"],
        cluster_id=args["host"],
        db=args["database"],
        endpoint_dns="full.dns",
        port=8192,
    )
    cparams = engine.dialect._do_generate_iam_auth_token(redshift, args)

    assert cparams["user"] == "IAM:the_user"
    assert cparams["host"] == "full.dns"
    assert cparams["password"] == "randompassword"
    assert cparams["database"] == args["database"]


def test_invalid_parameters():
    engine = sa.create_engine("redshift+psycopg2://test")

    # Passwords are not allowed when using IAM, but can be used when IAM is not used
    with pytest.raises(ValueError):
        basic_url = _url.make_url(
            "redshift://the_user:password@clusterid/the_database?iam=true"
        )
        engine.dialect.create_connect_args(basic_url)

    basic_url = _url.make_url("redshift://the_user:password@clusterid/the_database")
    assert engine.dialect.create_connect_args(basic_url) is not None
