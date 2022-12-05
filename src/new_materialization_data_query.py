from enum import Enum
class MaterializationType(Enum):
  Table = 'Table'
  View = 'View'

def getRowCountQuery(dbName: str, schemaName: str, materializationName: str, materializationType: MaterializationType):
  
  if materializationType == MaterializationType.Table:
    return f"""select row_count from "{dbName}".information_schema.tables 
    where table_schema = '{schemaName}' and table_name = '{materializationName}' limit 1;
    """

  return f"""select count(*) as row_count from "{dbName}"."{schemaName}"."{materializationName}";
  """

def getColumnCountQuery(dbName: str, schemaName: str, materializationName: str):
  return f"""select count(column_name) as column_count from "{dbName}".information_schema.columns
  where table_schema = '{schemaName}' and table_name = '{materializationName}';
  """

def getFreshnessQuery(dbName: str, schemaName: str, materializationName: str, materializationType: MaterializationType):
  return f"""select convert_timezone('UTC', last_altered)::timestamp as last_altered_converted, 
  sysdate() as now, 
  datediff(minute, last_altered_converted, now) as time_diff
  from "{dbName}".information_schema.{materializationType}s
  where table_schema = '{schemaName}' and table_name = '{materializationName}' limit 1;"""

def getSchemaChangeQuery(dbName: str, schemaName: str, tableName: str):
  return f"""with
  schema_cte as (select column_name, data_type, is_identity, is_nullable, ordinal_position from "{dbName}".information_schema.columns 
  where table_catalog = '{dbName}' and table_schema = '{schemaName}' and table_name = '{tableName}'
  order by ordinal_position)
  select object_construct(*) as column_definition from schema_cte
  """
