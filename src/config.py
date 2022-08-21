import os

def getMode():
  return os.environ.get('ENVIRONMENT')

def getCognitoUserPoolId():
  if os.environ.get('ENVIRONMENT') == 'development':
    return 'eu-central-1_0Z8JhFj8z'
  elif os.environ.get('MENVIRONMENTODE') == 'staging':
    return ''
  elif os.environ.get('ENVIRONMENT') == 'production':
    return 'eu-central-1_0muGtKMk3'

def getCognitoRegion():
  return 'eu-central-1'

