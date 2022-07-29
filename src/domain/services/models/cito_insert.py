from enum import Enum
from typing import Any, Tuple

from domain.services.schema.schema_deserialization import getSchemaObject

class MaterializationType(Enum):
  TESTS = 'tests',
  TEST_HISTORY = 'test_history',
  TEST_RESULTS = 'test_results',
  ALERTS = 'alerts'

def _getMaterializationSchema(type: str) -> dict[str, Any]:
  schema = getSchemaObject()
  tables = schema['tables']
  matchingTables = [table for table in tables if table['name'] == type]
  if len(matchingTables) != 1:
    raise Exception('More than one potential schema found')
  return matchingTables[0]

def getInsert(valueSets: list[Tuple], type: MaterializationType):
  schema = _getMaterializationSchema(type.value)

  return f"""
  set materialization_address = cito_wh.cito_db.public.{schema.name};

  create table [if not exists] $materialization_address (
{', '.join( str(' '.join(element for element in [column.name, column.type])) for column in schema.columns )}
  )

  insert into $materialization_address
  values
  {', '.join(str(valueSet) for valueSet in valueSets)};"""