import logging

from ...config import getMode

logger = logging.getLogger(__name__)

def getRoot(gateway: str, path: str) -> str:
  try:
    mode = getMode()
    if mode == 'development': 
      return f'http://localhost:{port}/{path}'
    if mode == 'production': 
      return f'https://{gateway}.execute-api.eu-central-1.amazonaws.com/production/${path}';
    raise Exception('Environment variable misalignment')

#     console.log(serviceName);
   
#     // const discoveredService: DiscoveredService = await discoverService(
#     //   serviceDiscoveryNamespace,
#     //   `${serviceName}-service`
#     // );
  except Exception as e:
    logger.error(e)
