import boto3
import requests
from airflow.models import Variable

S3 = boto3.resource('s3').Bucket('zogsolutions-eu-west-2-mwaa')
BASE_URL = 'https://uk.pt-x.com/'


def get_session_id(ti):
    endpoint = 'payments-service/api/security/handshake'
    handshake = requests.get(BASE_URL + endpoint)
    print("[+] Endpoint - ", handshake.request.url)
    print("[+] HTTP Request Status - ", handshake.status_code)
    assert handshake.status_code == 200, "[+] HTTP Request failed"
    print('[+] Cookies - ', handshake.cookies)
    print('[+] Response Headers - ', handshake.headers)
    print('[+] JSESSION ID - ', handshake.cookies['JSESSIONID'])
    print('[+] X-CSRF', handshake.headers['X-CSRF'])
    endpoint_auth = 'payments-service/api/security/login HTTP/1.1'
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'api-v2.5',
        'X-CSRF': handshake.headers['X-CSRF'],
    }
    data = {
        "loginTokens": [{
            "key": "com.bottomline.security.provider.login.email",
            "value": "Marcos.Martinez@outfoxthemarket.co.uk"
        },{
                "key": "com.bottomline.security.provider.login.password",
                "value": Variable.get("PTX_API_KEY")
        }],
        "apiVersion": "{\"major\": \"1\",\"minor\": \"0\",\"patch\": \"0\",\"build\": \"0\"}",
        "purpose": "cpay-auth",
        "tokenLocation": "HEADER"
    }
    response = requests.post(BASE_URL + endpoint_auth, headers=headers, data=data, cookies=handshake.cookies)
    print("[+] Endpoint Authentication - ", response.request.url)
    print("[+] HTTP Request Status - ", response.status_code)
    print('[+] Request Headers', response.request.headers)
    print('[+] Request Body', response.request.body)
    print('[+] Reason - ', response.reason)
    assert response.status_code == 200, "[+] HTTP Request failed"
