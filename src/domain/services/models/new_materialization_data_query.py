from enum import Enum
class MaterializationType(Enum):
  Table = 'Table'
  View = 'View'

def getRowCountQuery(dbName: str, schemaName: str, materializationName: str, materializationType: MaterializationType):
  
  if materializationType == MaterializationType.Table:
    return f"""select row_count from {dbName}.information_schema.tables 
    where table_schema = '{schemaName}' and table_name = '{materializationName}' limit 1;
    """

  return f"""select count(*) as row_count from {dbName}.{schemaName}.{materializationName};
  """

def getColumnCountQuery(dbName: str, materializationName: str):
  return f"""select count(column_name) as column_count from {dbName}.information_schema.columns
  where table_name = {materializationName};
  """

def getFreshnessQuery(dbName: str, schemaName: str, materializationName: str, materializationType: MaterializationType):
  return f"""select convert_timezone('UTC', last_altered) as newest_datetime from {dbName}.information_schema.{materializationType.value}s
  where table_schema = '{schemaName}' and table_name = '{materializationName}' limit 1;"""
