from flask import Flask, Response, request, jsonify
from src.infrastructure.shared.token_required import tokenRequired

from dotenv import load_dotenv
from src.domain.test.execute_test import ExecuteTest

from src.infrastructure.api.controllers.execute_test_controller import ExecuteTestController
from src.infrastructure.persistence.integration_api_repo import IntegrationApiRepo
from register import register

load_dotenv()

register = register()

app = Flask(__name__)

@app.route("/execute", methods=['POST'])
@tokenRequired
def executeTest(*args):
    response = Response()
    controller = ExecuteTestController(register['executeTest'], register['getAccounts'])
    return controller.execute(request, response, args[0])
    



