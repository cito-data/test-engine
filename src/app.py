from flask import Flask, request, jsonify
from domain.services.token_required import tokenRequired
import base64

from dotenv import load_dotenv

load_dotenv()


app = Flask(__name__)

@app.route("/execute", methods=['POST'])
@tokenRequired
def parseSQL(*args):
    

    body = request.json

    newDataQuery = base64toUTF8(body['newDataQuery'])
    historyDataQuery = base64toUTF8(body['historyDataQuery'])

    print(args[0])

    

    return args[1]
    



