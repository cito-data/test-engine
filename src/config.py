import os


def getMode():
    return os.environ.get('ENVIRONMENT')


def getCognitoUserPoolId():
    if os.environ.get('ENVIRONMENT') == 'development':
        return 'eu-central-1_0Z8JhFj8z'
    elif os.environ.get('ENVIRONMENT') == 'staging':
        return ''
    elif os.environ.get('ENVIRONMENT') == 'production':
        return 'eu-central-1_0muGtKMk3'


def getCognitoRegion():
    return 'eu-central-1'


def getIntegrationApiRoot():
    return os.environ.get('API_ROOT_INTEGRATION_SERVICE')


def getAccountApiRoot():
    return os.environ.get('API_ROOT_ACCOUNT_SERVICE')

def getMongoDetails():
    return (os.environ.get('MONGODB_DB_NAME'), 
            os.environ.get('MONGODB_DB_URL'))