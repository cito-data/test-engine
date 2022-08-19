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

def getColumnCountQuery(dbName: str, schemaName: str, materializationName: str):
  return f"""select count(column_name) as column_count from {dbName}.information_schema.columns
  where table_schema = {schemaName} and table_name = {materializationName};
  """

def getFreshnessQuery(dbName: str, schemaName: str, materializationName: str, materializationType: MaterializationType):
  return f"""convert_timezone('UTC', last_altered)::timestamp as last_altered_converted, 
  sysdate() as now, 
  datediff(minute, last_altered_converted, now) as time_diff
  from {dbName}.information_schema.{materializationType.value}s
  where table_schema = '{schemaName}' and table_name = '{materializationName}' limit 1;"""
