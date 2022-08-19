from dataclasses import dataclass
from typing import Any
from flask import request, jsonify
import requests
from functools import wraps
import jwt
import json
import logging

from config import getCognitoRegion, getCognitoUserPoolId

logger = logging.getLogger(__name__)

@dataclass
class ProcessedAuth:
    token: str
    payload: dict[str, Any] 


def tokenRequired(f):
   @wraps(f)
   def decorator(*args, **kwargs):
       token = None
       if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(' ')[1]
 
       if not token:
           return jsonify({'message': 'a valid token is missing'})
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
            return jsonify({'message': 'token is invalid'})
 
       return f(ProcessedAuth(token, payload), *args, **kwargs)
   return decorator