from get_accounts import GetAccounts
from query_snowflake import QuerySnowflake
from account_api_repo import AccountApiRepo
from integration_api_repo import IntegrationApiRepo


def createIOCRegister():
    integrationApiRepo = IntegrationApiRepo()
    accountApiRepo = AccountApiRepo()

    querySnowflake = QuerySnowflake(integrationApiRepo)
    getAccounts = GetAccounts(accountApiRepo)

    return {
        'integrationApiRepo': integrationApiRepo,
        'accountApiRepo': accountApiRepo,

        'querySnowflake': querySnowflake,
        'getAccounts': getAccounts,
    }
