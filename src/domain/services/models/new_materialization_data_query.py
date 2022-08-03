from enum import Enum
class MaterializationType(Enum):
  Table = 'table'
  View = 'view'

def getRowCountQuery(db: str, schema: str, materializationName: str, materializationType: MaterializationType):
  return f"""select row_count from {db}.information_schema.{materializationType.value}s 
  where table_schema = '{schema}' and table_name = '{materializationName}' limit 1;
  """

def getColumnCountQuery(db: str, materializationName: str):
  return f"""select count(column_name) from {db}.information_schema.columns
  where table_name = {materializationName};
  """

def getFreshnessQuery(db: str, schema: str, materializationName: str, materializationType: MaterializationType):
  return f"""select convert_timezone('UTC', last_altered) from {db}.information_schema.{materializationType.value}s
  where table_schema = '{schema}' and table_name = '{materializationName}' limit 1;"""
