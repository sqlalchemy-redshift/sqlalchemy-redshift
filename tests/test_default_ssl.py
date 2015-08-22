import sqlalchemy as sa


CERT = b"""-----BEGIN CERTIFICATE-----
MIIDeDCCAuGgAwIBAgIJALPHPDcjk979MA0GCSqGSIb3DQEBBQUAMIGFMQswCQYD
VQQGEwJVUzETMBEGA1UECBMKV2FzaGluZ3RvbjEQMA4GA1UEBxMHU2VhdHRsZTET
MBEGA1UEChMKQW1hem9uLmNvbTELMAkGA1UECxMCQ00xLTArBgkqhkiG9w0BCQEW
HmNvb2tpZS1tb25zdGVyLWNvcmVAYW1hem9uLmNvbTAeFw0xMjExMDIyMzI0NDda
Fw0xNzExMDEyMzI0NDdaMIGFMQswCQYDVQQGEwJVUzETMBEGA1UECBMKV2FzaGlu
Z3RvbjEQMA4GA1UEBxMHU2VhdHRsZTETMBEGA1UEChMKQW1hem9uLmNvbTELMAkG
A1UECxMCQ00xLTArBgkqhkiG9w0BCQEWHmNvb2tpZS1tb25zdGVyLWNvcmVAYW1h
em9uLmNvbTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEAw949t4UZ+9n1K8vj
PVkyehoV2kWepDmJ8YKl358nkmNwrSAGkslVttdpZS+FrgIcb44UbfVbB4bOSq0J
qd39GYVRzSazCwr2tpibFvH87PyAX4VVUBDlCizJToEYsXkAKecs+IRqCDWG2ht/
pibO2+T5Wp8jaxUBvDmoHY3BSgkCAwEAAaOB7TCB6jAdBgNVHQ4EFgQUE5KUaWSM
Uml+6MZQia7DjmfjvLgwgboGA1UdIwSBsjCBr4AUE5KUaWSMUml+6MZQia7Djmfj
vLihgYukgYgwgYUxCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpXYXNoaW5ndG9uMRAw
DgYDVQQHEwdTZWF0dGxlMRMwEQYDVQQKEwpBbWF6b24uY29tMQswCQYDVQQLEwJD
TTEtMCsGCSqGSIb3DQEJARYeY29va2llLW1vbnN0ZXItY29yZUBhbWF6b24uY29t
ggkAs8c8NyOT3v0wDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQUFAAOBgQCYZSRQ
zJNHXyKACrqMB5j1baUGf5NA0cZ/8s5iWeC9Gkwi7cXyiq9OrBaUtJBzAJTzfWbH
dfVaBL5FWuQsbkJWHe0mV+l4Kzl5bh/FSDSkhYR1duYRmdCXckQk6mAF6xG+1mpn
8YlJmbEhkDmBgJ8C8p0LCMNaO2xFLlNU0O+0ng==
-----END CERTIFICATE-----
"""


def test_ssl_args():
    engine = sa.create_engine('redshift+psycopg2://test')
    dialect = engine.dialect
    url = engine.url

    cargs, cparams = dialect.create_connect_args(url)

    assert cargs == []
    assert cparams.pop('host') == 'test'
    assert cparams.pop('sslmode') == 'verify-full'
    with open(cparams.pop('sslrootcert'), 'rb') as cert:
        assert cert.read() == CERT
    assert cparams == {}
