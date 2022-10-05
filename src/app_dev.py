import json
import logging
import traceback
from flask import Flask, request

from .base_controller import Request
from .token_required import processAuth

from .execute_test_controller import ExecuteTestController

from .register import register
register = register()

# import requests

app = Flask(__name__)

# todo - add security

@app.route("/tests/<testSuiteId>/execute", methods=['POST'])
def executeTest(testSuiteId):
    print(request)
    try:
        processedAuthObject = processAuth(request.headers.get('Authorization'))

        if(not processedAuthObject.success):
            return json.dumps({'message': 'Unauthorized'}), 401

        body = request.json
        mappedBody = {'testType': body['testType']}

        targetOrganizationIdKey = 'targetOrganizationId'

        mappedBody[targetOrganizationIdKey] = body[targetOrganizationIdKey] if targetOrganizationIdKey in body else None

        controllerRequest = Request(None, {'testId': testSuiteId}, None, mappedBody, processedAuthObject)

        controller = ExecuteTestController(register['executeTest'], register['getAccounts'])
        result = controller.execute(controllerRequest)

        return result.body, result.statusCode
    except Exception as e:
        logging.error(traceback.format_exc())
        return json.dumps({'error':{'message': str(e)}}), 500


    



