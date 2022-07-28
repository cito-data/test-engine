import base64
from dataclasses import asdict
from flask import Response, request
from src.domain.account_api.get_accounts import GetAccounts
from src.infrastructure.shared.token_required import ProcessedAuth
from src.domain.test.execute_test import ExecuteTest, ExecuteTestAuthDto, ExecuteTestRequestDto
from src.infrastructure.shared.base_controller import BaseController, CodeHttp, UserAccountInfo

import logging

logger = logging.getLogger(__name__)

def base64toUTF8(base64String):
    base64Encoded = base64String.encode("UTF-8")
    base64BytesDecoded = base64.b64decode(base64Encoded)
    return base64BytesDecoded.decode('utf-8') 

class ExecuteTestController(BaseController):

  def __init__(self, executeTest: ExecuteTest, getAccounts: GetAccounts ) -> None:
    super().__init__()
    self._executeTest = executeTest
    self._getAccounts = getAccounts


  def _buildRequestDto(self, req: request) -> ExecuteTestRequestDto:
    body = req.json

    newDataQuery = base64toUTF8(body['newDataQuery'])
    historyDataQuery = base64toUTF8(body['historyDataQuery'])
    
    return ExecuteTestRequestDto(newDataQuery, historyDataQuery)

  def _buildAuthDto(self, jwt: str, userAccountInfo: UserAccountInfo) -> ExecuteTestAuthDto:
    return ExecuteTestAuthDto(jwt, userAccountInfo.organizationId)

  def executeImpl(self, req: request, processedAuth: ProcessedAuth) -> Response:
    try:
      getUserAccountInfoResult = ExecuteTestController.getUserAccountInfo(processedAuth, self._getAccounts)
      
      if not getUserAccountInfoResult.success:
        return ExecuteTestController.unauthorized(getUserAccountInfoResult.error)
      if not getUserAccountInfoResult.value:
        raise Exception('Authorization failed')

      requestDto = self._buildRequestDto(req)
      authDto = self._buildAuthDto(processedAuth.token, getUserAccountInfoResult.value)

      result = self._executeTest.execute(requestDto, authDto)

      if not result.success:
        return ExecuteTestController.badRequest(result.error)
      if not result.value:
        raise Exception('Test result not provided')
      return ExecuteTestController.ok(asdict(result.value), CodeHttp.OK.value)
    except Exception as e:
      logger.error(e)
      return ExecuteTestController.fail(e)