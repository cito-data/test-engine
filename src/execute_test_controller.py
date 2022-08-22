import base64
from cmath import log
from dataclasses import asdict
import json
from typing import Any
from get_accounts import GetAccounts
from base_controller import Response
from token_required import ProcessedAuth
from execute_test import ExecuteTest, ExecuteTestAuthDto, ExecuteTestRequestDto
from base_controller import BaseController, CodeHttp, UserAccountInfo

import logging

logger = logging.getLogger(__name__)

# def base64toUTF8(base64String):
#     base64Encoded = base64String.encode("UTF-8")
#     base64BytesDecoded = base64.b64decode(base64Encoded)
#     return base64BytesDecoded.decode('utf-8') 

class ExecuteTestController(BaseController):

  def __init__(self, executeTest: ExecuteTest, getAccounts: GetAccounts ) -> None:
    super().__init__()
    self._executeTest = executeTest
    self._getAccounts = getAccounts


  def _buildRequestDto(self, req: Any, urlParams: dict[str, str]) -> ExecuteTestRequestDto:
    body = json.loads(req['body']) if isinstance(req['body'], str) else req['body']

    testId = urlParams['testId']
    targetOrganizationId = body['targetOrganizationId']
    
    return ExecuteTestRequestDto(testId, targetOrganizationId)

  def _buildAuthDto(self, jwt: str, userAccountInfo: UserAccountInfo) -> ExecuteTestAuthDto:
    return ExecuteTestAuthDto(jwt)

  def executeImpl(self, req: Any, processedAuth: ProcessedAuth, urlParams: dict[str, str]) -> Response:
    try:
      getUserAccountInfoResult = ExecuteTestController.getUserAccountInfo(processedAuth, self._getAccounts)
      
      if not getUserAccountInfoResult.success:
        return ExecuteTestController.unauthorized(getUserAccountInfoResult.error)
      if not getUserAccountInfoResult.value:
        raise Exception('Authorization failed')

      requestDto = self._buildRequestDto(req, urlParams)
      authDto = self._buildAuthDto(processedAuth.token, getUserAccountInfoResult.value)

      result = self._executeTest.execute(requestDto, authDto)

      if not result.success:
        return ExecuteTestController.badRequest(result.error)
      if not result.value:
        raise Exception('Test result not provided')

      return ExecuteTestController.ok(json.dumps(asdict(result.value)), CodeHttp.CREATED.value)
    except Exception as e:
      logger.error(e)
      return ExecuteTestController.fail(e)