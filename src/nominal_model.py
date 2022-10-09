
from dataclasses import dataclass, asdict
import datetime
import json
from typing import Union

@dataclass
class MaterializationSchema:
    column_name: str
    data_type: str
    is_identity: bool
    is_nullable: bool
    ordinal_position: int

@dataclass
class SchemaDiff:
    column_name: tuple[Union[str, None], Union[str, None]]
    ordinal_position: tuple[Union[int, None], Union[int, None]]
    data_type: Union[tuple[Union[str, None], Union[str, None]], None]
    is_identity: Union[tuple[Union[bool, None], Union[bool, None]], None]
    is_nullable: Union[tuple[Union[bool, None], Union[bool, None]], None]

@dataclass
class ResultDto:  
  isIdentical: bool
  expectedValue: Union[MaterializationSchema, None]
  value: MaterializationSchema
  deviations: list[SchemaDiff]
  executedOn: str

class SchemaChangeModel():

  _oldSchema: MaterializationSchema
  _newSchema: MaterializationSchema

  _schemaDiffs: list[SchemaDiff]
  
  def __init__(self, newSChema: MaterializationSchema, oldSchema: Union[MaterializationSchema, None]) -> None:
    self._oldSchema = oldSchema
    self._newSchema = newSChema
      
  def run(self) -> ResultDto:
    if not self._oldSchema:
        return ResultDto(True, self._oldSchema, self._newSchema, [], datetime.datetime.utcnow().isoformat())

    columnCountOldSchema = len(self._oldSchema)
    columnCountNewSchema = len(self._newSchema)

    columnNameSchemaKey = 'COLUMN_NAME'
    dataTypeSchemaKey = 'DATA_TYPE'
    ordinalPositionSchemaKey = 'ORDINAL_POSITION'
    isIdentitySchemaKey = 'IS_IDENTITY'
    isNullableSchemaKey = 'IS_NULLABLE'

    schemaDiffs: list[SchemaDiff] = []
    for i in range(1, (columnCountNewSchema if columnCountNewSchema > columnCountOldSchema else columnCountOldSchema) + 1):
        oldColumnSchemaDefinition = self._oldSchema[str(i)] if i <= columnCountOldSchema else None
        newColumnSchemaDefinition = self._newSchema[str(i)] if i <= columnCountNewSchema else None

        if(oldColumnSchemaDefinition and not newColumnSchemaDefinition):
            schemaDiff = SchemaDiff((oldColumnSchemaDefinition[columnNameSchemaKey], None),
                (oldColumnSchemaDefinition[ordinalPositionSchemaKey], None),
                (oldColumnSchemaDefinition[dataTypeSchemaKey], None),
                (oldColumnSchemaDefinition[isIdentitySchemaKey], None),
                (oldColumnSchemaDefinition[isNullableSchemaKey], None))

            schemaDiffs.append(schemaDiff)
        elif(newColumnSchemaDefinition and not oldColumnSchemaDefinition):

            schemaDiff = SchemaDiff((None, newColumnSchemaDefinition[columnNameSchemaKey]),
                (None, newColumnSchemaDefinition[ordinalPositionSchemaKey]),
                (None, newColumnSchemaDefinition[dataTypeSchemaKey]),
                (None, newColumnSchemaDefinition[isIdentitySchemaKey]),
                (None, newColumnSchemaDefinition[isNullableSchemaKey]))
            
            
            schemaDiffs.append(schemaDiff)
        else:
            oldColumnName = oldColumnSchemaDefinition[columnNameSchemaKey]
            newColumnName = newColumnSchemaDefinition[columnNameSchemaKey]
            columnNameIdentical = oldColumnName == newColumnName

            oldDataType = oldColumnSchemaDefinition[dataTypeSchemaKey]
            newDataType = newColumnSchemaDefinition[dataTypeSchemaKey]
            dataTypeIdentical = oldDataType == newDataType

            oldOrdinalPosition = oldColumnSchemaDefinition[ordinalPositionSchemaKey]
            newOrdinalPosition = newColumnSchemaDefinition[ordinalPositionSchemaKey]
            ordinalPositionIdentical =  oldOrdinalPosition == newOrdinalPosition

            oldIsIdentity = oldColumnSchemaDefinition[isIdentitySchemaKey]
            newIsIdentity = newColumnSchemaDefinition[isIdentitySchemaKey]
            isIdentityIdentical =  oldIsIdentity == newIsIdentity

            oldIsNullable = oldColumnSchemaDefinition[isNullableSchemaKey]
            newIsNullable = newColumnSchemaDefinition[isNullableSchemaKey]
            isNullableIdentical = oldIsNullable == newIsNullable

            anyDiff = not (columnNameIdentical and ordinalPositionIdentical and dataTypeIdentical and isIdentityIdentical and isNullableIdentical)

            if anyDiff:
               
                schemaDiff = SchemaDiff((oldColumnName, newColumnName) if anyDiff else None,
                    (oldOrdinalPosition, newOrdinalPosition) if anyDiff else None,
                    (oldDataType, newDataType) if not dataTypeIdentical else None,
                    (oldIsIdentity, newIsIdentity) if not isIdentityIdentical else None,
                    (oldIsNullable, newIsNullable) if not isNullableIdentical else None
                    )
                    
                schemaDiffs.append(schemaDiff)

    deviations = json.dumps( [asdict(el) for el in schemaDiffs])

    return ResultDto(not len(schemaDiffs), self._oldSchema, self._newSchema, deviations, datetime.datetime.utcnow().isoformat())

  

  

     
