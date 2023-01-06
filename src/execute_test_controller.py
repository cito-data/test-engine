from dataclasses import asdict
import json
from typing import Any

from observability_api_repo import ObservabilityApiRepo
from query_snowflake import QuerySnowflake
from get_accounts import GetAccounts
from base_controller import Request, Response
from execute_test import ExecuteTest, ExecuteTestAuthDto, ExecuteTestRequestDto
from base_controller import BaseController, CodeHttp, UserAccountInfo

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# def base64toUTF8(base64String):
#     base64Encoded = base64String.encode("UTF-8")
#     base64BytesDecoded = base64.b64decode(base64Encoded)
#     return base64BytesDecoded.decode('utf-8')


class ExecuteTestController(BaseController):

    _getAccounts: GetAccounts
    _observabilityApiRepo: ObservabilityApiRepo
    _querySnowflake: QuerySnowflake

    def __init__(self, getAccounts: GetAccounts, observabilityApiRepo: ObservabilityApiRepo, querySnowflake: QuerySnowflake) -> None:
        super().__init__()
        self._getAccounts = getAccounts
        self._observabilityApiRepo = observabilityApiRepo
        self._querySnowflake = querySnowflake

    def _buildRequestDto(self, body: "dict[str, Any]", pathParams: "dict[str, str]") -> ExecuteTestRequestDto:
        testId = pathParams['testId']
        targetOrgId = body['targetOrgId']
        testType = body['testType']

        return ExecuteTestRequestDto(testId, testType, targetOrgId)

    def _buildAuthDto(self, jwt: str, userAccountInfo: UserAccountInfo) -> ExecuteTestAuthDto:
        return ExecuteTestAuthDto(jwt, userAccountInfo.callerOrgId, userAccountInfo.isSystemInternal)

    def executeImpl(self, req: Request) -> Response:
        try:
            getUserAccountInfoResult = ExecuteTestController.getUserAccountInfo(
                req.auth, self._getAccounts)

            if not getUserAccountInfoResult.success:
                return ExecuteTestController.unauthorized(getUserAccountInfoResult.error)
            if not getUserAccountInfoResult.value:
                raise Exception('Authorization failed')

            requestDto = self._buildRequestDto(req.body, req.pathParams)
            authDto = self._buildAuthDto(
                req.auth.token, getUserAccountInfoResult.value)

            logger.info(
                f'Executing test suite {requestDto.testSuiteId} for organization {requestDto.targetOrgId if requestDto.targetOrgId else authDto.callerOrgId}...')

            result = ExecuteTest(self._observabilityApiRepo, self._querySnowflake).execute(
                requestDto, authDto)

            if not result.success:
                return ExecuteTestController.badRequest(result.error)
            if not result.value:
                raise Exception('Test result not provided')

            logger.info(
                f'...Test suite {requestDto.testSuiteId} successfully executed for organization {requestDto.targetOrgId if requestDto.targetOrgId else authDto.callerOrgId}')

            return ExecuteTestController.ok(json.dumps(asdict(result.value)), CodeHttp.CREATED.value)
        except Exception as e:
            logger.exception(f'error: {e}' if e.args[0] else f'error: unknown')
            return ExecuteTestController.fail(e)
