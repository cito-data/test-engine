import jsonschema
import logging
from src.domain.integration_api.snowflake.snowflake_query_result_dto import SnowflakeQueryResultDto

logger = logging.getLogger(__name__)

def validateJson(jsonData: dict):
    try:
        jsonschema.validate(instance=jsonData, schema=SnowflakeQueryResultDto)
    except jsonschema.exceptions.ValidationError as e:
        logger.error(e)
        return False
    return True