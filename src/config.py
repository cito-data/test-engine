import os

def getMode():
  return os.environ('ENVIRONMENT')

def getCognitoUserPoolId():
  if os.environ.get('ENVIRONMENT') == 'development':
    return 'eu-central-1_HYLD4MoTL'
  elif os.environ.get('MENVIRONMENTODE') == 'staging':
    return 'eu-central-1_htA4V0E1g'
  elif os.environ.get('ENVIRONMENT') == 'production':
    return 'eu-central-1_fttc090sQ'

def getCognitoRegion():
  return 'eu-central-1'

