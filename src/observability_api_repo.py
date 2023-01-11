import requests
import json
from dataclasses import asdict

from i_observability_api_repo import IObservabilityApiRepo
from snowflake_query_result_dto import SnowflakeQueryResultDto
import logging
from config import getObservabilityApiRoot
from test_execution_result import QualTestExecutionResult, QuantTestExecutionResult

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ObservabilityApiRepo(IObservabilityApiRepo):
    def __init__(self) -> None:
        self._version = 'v1'
        self._apiRoot = getObservabilityApiRoot()

    def sendQuantTestExecutionResult(self, result: QuantTestExecutionResult, jwt: str) -> None:
        data = json.dumps(asdict(result))

        response = requests.post(f'{self._apiRoot}/api/{self._version}/test-suite/{result.testSuiteId}/result',
                                 data=data, headers={'Authorization': f'Bearer {jwt}', 'Content-Type': 'application/json'})
        if response.status_code == 201:
            return

        raise Exception(
            response.text if response.text else 'Unknown Error')

    def sendQualTestExecutionResult(self, result: QualTestExecutionResult, jwt: str) -> None:
        data = json.dumps(asdict(result))

        response = requests.post(f'{self._apiRoot}/api/{self._version}/qual-test-suite/{result.testSuiteId}/result',
                                 data=data, headers={'Authorization': f'Bearer {jwt}', 'Content-Type': 'application/json'})

        if response.status_code == 201:
            return

        raise Exception(
            response.text if response.text else 'Unknown Error')
