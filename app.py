from flask import Flask, request, jsonify
import base64
from functools import wraps
import jwt
import json

app = Flask(__name__)

def token_required(f):
   @wraps(f)
   def decorator(*args, **kwargs):
       token = None
       print(request.headers)
       if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(' ')[1]
 
       if not token:
           return jsonify({'message': 'a valid token is missing'})
       try:
            print(token)
            #for JWKS that contain multiple JWK
            jwks = {"keys":[{"alg":"RS256","e":"AQAB","kid":"pgKq2nRpp+/cRsHEQDV7zCyq/rwkEkwq+SbH4bG5RIc=","kty":"RSA","n":"yFWlAtwg6-PDQZd5TydRLnydQ95g9Ee8K5rw1fZfIjiYm9NC_oFOF0xiki_iP2SOpDEkqNR9Ln1slv6vrCJeZdBN8mDuM-tjs8Cpv1a0GuFHpJdoauE6fXjOw7ij8Xebv1_hiIoeB02e_tOIOtinovQ6X1taExRD0tFmxmTcgsS6Z5HHNjRwPUI38Rk-IBuRS8zHF9sNf778mCI-ZRk8ZGR_iEx6y7E7FlorBtGnHiCB0Qq0uKEBrbOPsEMikqM3BCwC-mp8w8vJPMH8qVUPOTUQ9OM0NIevUqIi2MfTdhBMX3e0iTCVgo1RXTjmdleFWlCYUVXHcHEc3uAvNsk2OQ","use":"sig"},{"alg":"RS256","e":"AQAB","kid":"9wqqix+K23yBHLpl8YLqZ8KTF6djI4UhkRhZ56NswYg=","kty":"RSA","n":"yShjYnY9LI9kCTexXByC1pUL_WQe_XJ8d0iR5yeXlBC9x6ZmnU9izkgHj46V3LwXqQiHhifErKb7ds0SWq02TThol-4dH5sHobbs86WtKYUwIMRWh4CPOd8LF0-a6diURjWdayg0dCMAv-IH_DhWgBfG_MWtl6AnngvSIjFHjcJKe-wNKTnOYhSXjaB3mX7rMzC3zJZBFnKGm2FX4u1vqPkCr6eUL51GXa-4kWTYvz0zY4WoXHVmJ7Y8q0MRwsdfAOcv3h3WMxqqw1IX2pl2NpmYcVy3H9HVpv8SCcCi6In5Y38Jdy_V-aDuOoYycz3oBT4mzmHs7hRqlXOBSBbSfQ","use":"sig"}]}
            for jwk in jwks['keys']:
                kid = jwk['kid']
                jwks[kid] = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))

            kid = jwt.get_unverified_header(token)['kid']
            key = jwks[kid]

            payload = jwt.decode(token, key=key, algorithms=['RS256'])

            print(payload)
       except Exception as e:
            print(e)
            return jsonify({'message': 'token is invalid'})
 
       return f(payload, token, *args, **kwargs)
   return decorator

@app.route("/execute", methods=['POST'])
@token_required
def parseSQL(*args):
    """
    --------jwt------------ 
    get from auth header
    """

    # body = request.json

    # newDataQuery = base64toUTF8(body['newDataQuery'])
    # historyDataQuery = base64toUTF8(body['newDataQuery'])

    # dbtQueryParamKey = 'is_dbt'

    # if dbtQueryParamKey in queryParameters and queryParameters[dbtQueryParamKey] == 'true':
    #     parsedSQL = sqlfluff.parse(sql, queryParameters['dialect'], './.sqlfluff')
    # else:
    #     parsedSQL = sqlfluff.parse(sql, queryParameters['dialect'])

    print('hey')
    print(args[1])

    

    return args[0]
    


def base64toUTF8(base64String):
    base64Encoded = base64String.encode("UTF-8")
    base64BytesDecoded = base64.b64decode(base64Encoded)
    return base64BytesDecoded.decode('utf-8') 
