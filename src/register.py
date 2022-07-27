from src.domain.account_api.get_accounts import GetAccounts
from src.domain.integration_api.snowflake.query_snowflake import QuerySnowflake
from src.domain.test.execute_test import ExecuteTest
from src.infrastructure.persistence.account_api_repo import AccountApiRepo
from src.infrastructure.persistence.integration_api_repo import IntegrationApiRepo


def register():
  integrationApiRepo = IntegrationApiRepo()
  accountApiRepo = AccountApiRepo()

  querySnowflake = QuerySnowflake(integrationApiRepo)
  getAccounts = GetAccounts(accountApiRepo)

  executeTest = ExecuteTest(integrationApiRepo, querySnowflake)


  return {
    'integrationApiRepo': integrationApiRepo,
    'accountApiRepo': accountApiRepo,

    'querySnowflake': querySnowflake,
    'getAccounts': getAccounts,

    'executeTest': executeTest,


  }