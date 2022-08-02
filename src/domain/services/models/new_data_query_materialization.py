def getRowCount(db: str, tableSchema: str, tableName: str, materializationType: str):
  return f"""select row_count from {db}.information_schema.{materializationType} 
  where table_schema = '{tableSchema}' and table_name = '{tableName}' limit 1;
  """

def getColumnCount(db: str, tableName: str):
  return f"""select count(column_name) from {db}.information_schema.columns
  where table_name = {tableName};
  """

def getFreshness(db: str, tableSchema: str, tableName: str, materializationType: str):
  return f"""select convert_timezone('UTC', last_altered) from {db}.information_schema.{materializationType} 
  where table_schema = '{tableSchema}' and table_name = '{tableName}' limit 1;"""
