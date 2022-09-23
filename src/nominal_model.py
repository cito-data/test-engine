
from dataclasses import dataclass
import datetime
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
  isAnomaly: bool
  expectedValue: MaterializationSchema
  value: MaterializationSchema
  deviation: list[SchemaDiff]
  executedOn: str

class SchemaChangeModel():

  _oldSchema: MaterializationSchema
  _newSchema: MaterializationSchema

  _schemaDiffs: list[SchemaDiff]
  
  def __init__(self, oldSchema: MaterializationSchema, newSChema: MaterializationSchema) -> None:
    self._oldSchema = oldSchema
    self._newSchema = newSChema
      
  def run(self) -> ResultDto:
    columnCountOldSchema = len(self._oldSchema)
    columnCountNewSchema = len(self._newSchema)

    columnNameSchemaKey = 'column_name'
    dataTypeSchemaKey = 'data_type'
    ordinalPositionSchemaKey = 'ordinal_position'
    isIdentitySchemaKey = 'is_identity'
    isNullableSchemaKey = 'is_nullable'

    schemaDiffs: list[SchemaDiff] = []
    for i in range(1, (columnCountNewSchema if columnCountNewSchema > columnCountOldSchema else columnCountOldSchema) + 1):
        oldColumnSchemaDefinition = self._oldSchema[str(i)] if i <= columnCountOldSchema else None
        newColumnSchemaDefinition = self._newSchema[str(i)] if i <= columnCountNewSchema else None

        if(oldColumnSchemaDefinition and not newColumnSchemaDefinition):
            schemaDiff = SchemaDiff((oldColumnSchemaDefinition[columnNameSchemaKey.capitalize()], None),
                (oldColumnSchemaDefinition[ordinalPositionSchemaKey.capitalize()], None),
                (oldColumnSchemaDefinition[dataTypeSchemaKey.capitalize()], None),
                (oldColumnSchemaDefinition[isIdentitySchemaKey.capitalize()], None),
                (oldColumnSchemaDefinition[isNullableSchemaKey.capitalize()], None))

            schemaDiffs.append(schemaDiff)
        elif(newColumnSchemaDefinition and not oldColumnSchemaDefinition):

            schemaDiff = SchemaDiff((None, newColumnSchemaDefinition[columnNameSchemaKey.capitalize()]),
                (None, newColumnSchemaDefinition[ordinalPositionSchemaKey.capitalize()]),
                (None, newColumnSchemaDefinition[dataTypeSchemaKey.capitalize()]),
                (None, newColumnSchemaDefinition[isIdentitySchemaKey.capitalize()]),
                (None, newColumnSchemaDefinition[isNullableSchemaKey.capitalize()]))
            
            
            schemaDiffs.append(schemaDiff)
        else:
            oldColumnName = oldColumnSchemaDefinition[columnNameSchemaKey.capitalize()]
            newColumnName = newColumnSchemaDefinition[columnNameSchemaKey.capitalize()]
            columnNameIdentical = oldColumnName == newColumnName

            oldDataType = oldColumnSchemaDefinition[dataTypeSchemaKey.capitalize()]
            newDataType = newColumnSchemaDefinition[dataTypeSchemaKey.capitalize()]
            dataTypeIdentical = oldDataType == newDataType

            oldOrdinalPosition = oldColumnSchemaDefinition[ordinalPositionSchemaKey.capitalize()]
            newOrdinalPosition = newColumnSchemaDefinition[ordinalPositionSchemaKey.capitalize()]
            ordinalPositionIdentical =  oldOrdinalPosition == newOrdinalPosition

            oldIsIdentity = oldColumnSchemaDefinition[isIdentitySchemaKey.capitalize()]
            newIsIdentity = newColumnSchemaDefinition[isIdentitySchemaKey.capitalize()]
            isIdentityIdentical =  oldIsIdentity == newIsIdentity

            oldIsNullable = oldColumnSchemaDefinition[isNullableSchemaKey.capitalize()]
            newIsNullable = newColumnSchemaDefinition[isNullableSchemaKey.capitalize()]
            isNullableIdentical = oldIsNullable == newIsNullable

            anyDiff = not (columnNameIdentical and ordinalPositionIdentical and dataTypeIdentical and isIdentityIdentical and isNullableIdentical)

            if not anyDiff:
                return schemaDiffs

            schemaDiff = SchemaDiff((oldColumnName, newColumnName) if anyDiff else None,
                (oldOrdinalPosition, newOrdinalPosition) if anyDiff else None,
                (oldDataType, newDataType) if not dataTypeIdentical else None,
                (oldIsIdentity, newIsIdentity) if not isIdentityIdentical else None,
                (oldIsNullable, newIsNullable) if not isNullableIdentical else None
                  )
                
            schemaDiffs.append(schemaDiff)

    return ResultDto(len(schemaDiffs) == True, schemaDiffs, datetime.datetime.utcnow().isoformat())

  

  

     
