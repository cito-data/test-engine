from dataclasses import asdict
import json
from typing import Any

from src.integration_api_repo import IntegrationApiRepo
from src.query_snowflake import QuerySnowflake
from .get_accounts import GetAccounts
from .base_controller import Request, Response
from .execute_test import ExecuteTest, ExecuteTestAuthDto, ExecuteTestRequestDto
from .base_controller import BaseController, CodeHttp, UserAccountInfo

import logging

logger = logging.getLogger(__name__)

# def base64toUTF8(base64String):
#     base64Encoded = base64String.encode("UTF-8")
#     base64BytesDecoded = base64.b64decode(base64Encoded)
#     return base64BytesDecoded.decode('utf-8') 

class ExecuteTestController(BaseController):

  def __init__(self, getAccounts: GetAccounts, integrationApiRepo: IntegrationApiRepo, querySnowflake: QuerySnowflake) -> None:
    super().__init__()
    self._getAccounts = getAccounts
    self._integrationApiRepo = integrationApiRepo
    self._querySnowflake = querySnowflake


  def _buildRequestDto(self, body: dict[str, Any], pathParams: dict[str, str]) -> ExecuteTestRequestDto:
    testId = pathParams['testId']
    targetOrganizationId = body['targetOrganizationId']
    testType = body['testType']
    
    return ExecuteTestRequestDto(testId, testType, targetOrganizationId)

  def _buildAuthDto(self, jwt: str, userAccountInfo: UserAccountInfo) -> ExecuteTestAuthDto:
    return ExecuteTestAuthDto(jwt, userAccountInfo.callerOrganizationId, userAccountInfo.isSystemInternal)

  def executeImpl(self, req: Request) -> Response:
    try:
      getUserAccountInfoResult = ExecuteTestController.getUserAccountInfo(req.auth, self._getAccounts)
      
      if not getUserAccountInfoResult.success:
        return ExecuteTestController.unauthorized(getUserAccountInfoResult.error)
      if not getUserAccountInfoResult.value:
        raise Exception('Authorization failed')

      requestDto = self._buildRequestDto(req.body, req.pathParams)
      authDto = self._buildAuthDto(req.auth.token, getUserAccountInfoResult.value)

      result =  ExecuteTest(self._integrationApiRepo, self._querySnowflake).execute(requestDto, authDto)

      if not result.success:
        return ExecuteTestController.badRequest(result.error)
      if not result.value:
        raise Exception('Test result not provided')

      return ExecuteTestController.ok(json.dumps(asdict(result.value)), CodeHttp.CREATED.value)
    except Exception as e:
      logger.error(e)
      return ExecuteTestController.fail(e)