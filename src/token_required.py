from dataclasses import dataclass
from typing import Any
import requests
# from functools import wraps
import jwt
import json
import logging

from config import getCognitoRegion, getCognitoUserPoolId

logger = logging.getLogger(__name__)

@dataclass
class ProcessedAuth:
    token: str
    payload: dict[str, Any]
    success: bool 


def processAuth(authHeader: str):
    if not authHeader:
        raise Exception('Unauthorized - request is missing auth header')

    token = authHeader.split('Bearer')[1].strip()

    if not token:
        return ProcessedAuth(token, {}, False)
    try:
        #for JWKS that contain multiple JWK
        jwks = requests.get(f'https://cognito-idp.{getCognitoRegion()}.amazonaws.com/{getCognitoUserPoolId()}/.well-known/jwks.json').json()
        for jwk in jwks['keys']:
            kid = jwk['kid']
            jwks[kid] = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))

        kid = jwt.get_unverified_header(token)['kid']
        key = jwks[kid]

        payload = jwt.decode(token, key=key, algorithms=['RS256'])
    except Exception as e:
        logger.error(e)
        return ProcessedAuth(token, {}, False)

    return ProcessedAuth(token, payload, True)