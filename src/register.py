from get_accounts import GetAccounts
from query_snowflake import QuerySnowflake
from account_api_repo import AccountApiRepo
from integration_api_repo import IntegrationApiRepo
from observability_api_repo import ObservabilityApiRepo


def register():
    integrationApiRepo = IntegrationApiRepo()
    observabilityApiRepo = ObservabilityApiRepo()
    accountApiRepo = AccountApiRepo()

    querySnowflake = QuerySnowflake(integrationApiRepo)
    getAccounts = GetAccounts(accountApiRepo)

    return {
        'integrationApiRepo': integrationApiRepo,
        'accountApiRepo': accountApiRepo,

        'querySnowflake': querySnowflake,
        'getAccounts': getAccounts,
    }
