from get_accounts import GetAccounts
from query_snowflake import QuerySnowflake
from execute_test import ExecuteTest
from account_api_repo import AccountApiRepo
from integration_api_repo import IntegrationApiRepo


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