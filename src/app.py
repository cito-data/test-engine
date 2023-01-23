import os
from ioc_register import createIOCRegister
import json
import traceback

from base_controller import Request
from token_required import processAuth

from execute_test_controller import ExecuteTestController

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# import requests

register = createIOCRegister()

print(f'Running in {os.environ.get("ENVIRONMENT")}')


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e

    try:
        request = event

        processedAuthObject = processAuth(request['headers']['Authorization'])

        if (not processedAuthObject.success):
            return {
                "statusCode": 401,
                "body": json.dumps({'message': 'Unauthorized'}),
            }

        body = json.loads(request['body']) if isinstance(
            request['body'], str) else request['body']

        mappedBody = {'testType': body['testType']}

        targetOrgIdKey = 'targetOrgId'

        mappedBody[targetOrgIdKey] = body[targetOrgIdKey] if targetOrgIdKey in body else None

        testId = request['pathParameters']['testSuiteId']

        logger.debug(f'Executing test with id {testId}')

        controllerRequest = Request(
            None, {'testId': testId}, None, mappedBody, processedAuthObject)

        controller = ExecuteTestController(
            register['getAccounts'], register['querySnowflake'])
        result = controller.execute(controllerRequest)

        return {
            "statusCode": result.statusCode,
            "body": result.body,
        }
    except Exception as e:
        logger.exception(f'error: {e}' if e.args[0] else f'error: unknown')
        return {
            "statusCode": 500,
            "body": json.dumps({'message': 'Internal Error occurred'}),
        }

    # return {
    #     "statusCode": 200,
    #     "body": json.dumps({
    #         "message": "hello world",
    #         # "location": ip.text.replace("\n", "")
    #     }),
    # }
