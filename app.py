from flask import Flask, request
import sqlfluff
import base64

app = Flask(__name__)

@app.route("/execute", methods=['POST'])
def parseSQL():
    queryParameters = request.args
    body = request.json

    sql = base64toUTF8(body['sql'])

    dbtQueryParamKey = 'is_dbt'

    if dbtQueryParamKey in queryParameters and queryParameters[dbtQueryParamKey] == 'true':
        parsedSQL = sqlfluff.parse(sql, queryParameters['dialect'], './.sqlfluff')
    else:
        parsedSQL = sqlfluff.parse(sql, queryParameters['dialect'])

    return parsedSQL
    


def base64toUTF8(base64String):
    base64Encoded = base64String.encode("UTF-8")
    base64BytesDecoded = base64.b64decode(base64Encoded)
    return base64BytesDecoded.decode('utf-8') 
