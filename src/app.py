from flask import Flask, Response, request
from infrastructure.shared.token_required import tokenRequired

from dotenv import load_dotenv
from domain.test.execute_test import ExecuteTest

from infrastructure.api.controllers.execute_test_controller import ExecuteTestController
from infrastructure.persistence.integration_api_repo import IntegrationApiRepo
from register import register

load_dotenv()

register = register()

app = Flask(__name__)

@app.route("/tests/<testId>/execute", methods=['POST'])
@tokenRequired
def executeTest(*args, testId):
    controller = ExecuteTestController(register['executeTest'], register['getAccounts'])
    result = controller.execute(request, args[0], {'testId': testId})
    
    return result.payload, result.statusCode
    



