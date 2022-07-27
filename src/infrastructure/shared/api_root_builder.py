import os
import logging

logger = logging.getLogger(__name__)

def getRoot(serviceName: str, port: str, path: str) -> str:
  try:
    env = os.environ.get('ENVIRONMENT')
    if env == 'development': return f'http://localhost:{port}/{path}'
    return f'http://localhost:{port}/{path}'

#     console.log(serviceName);
   
#     // const discoveredService: DiscoveredService = await discoverService(
#     //   serviceDiscoveryNamespace,
#     //   `${serviceName}-service`
#     // );
  except Exception as e:
    logger.error(e)
