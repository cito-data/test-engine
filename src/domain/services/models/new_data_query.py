def getRowCount(materializationAddress: str, tableSchema: str, tableName: str):
  return f"""select row_count from {materializationAddress} 
  where table_schema = '{tableSchema}' and table_name = '{tableName}' limit 1
  """