
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Sequence, Union
import pandas as pd 

class ModelType(Enum):
  ROW_COUNT = 'ROW_COUNT'

@dataclass
class ResultDto:
  executionId: str
  threshold: int
  type: str

  meanAbsoluteDeviation: Union[float, None]
  medianAbsoluteDeviation: float
  modifiedZScore: float
  
  newDatapoint: float
  isAnomaly: bool

class StatisticalModel(ABC):

  _newData: list[float]
  _historicalData: list[float]
  _type: ModelType
  _threshold: int
  _executionId: str

  _newDataPoint: float
  _dataSeries: pd.Series

  _median: int
  _medianAbsoluteDeviation: float
  _meanAbsoluteDeviation: Union[float, None] = None
  _modifiedZScore: float

  @abstractmethod
  def __init__(self, newData: list[float] , historicalData: list[float], type: ModelType, threshold: int, executionId: str) -> None:
    self._newData = newData
    self._historicalData = historicalData
    self._type = type
    self._threshold = threshold
    self._executionId = executionId
  
  def _absoluteDeviation(self, x) -> float:
    return abs(x - self._median)

  def _calculateMedianAbsoluteDeviation(self) -> float:
    self._median = self._dataSeries.median()
    absoluteDeviation = self._dataSeries.apply(self._absoluteDeviation)
    return absoluteDeviation.median()

  def _calculateModifiedZScore(self, x) -> float:
    # https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=terms-modified-z-score
    if self._medianAbsoluteDeviation == 0:
      self._meanAbsoluteDeviation = self._dataSeries.mad()
      return (x - self._median)/(1.253314*self._meanAbsoluteDeviation)
    return (x - self._median)/(1.486*self._medianAbsoluteDeviation)

  def _isAnomaly(self) -> bool:
    return bool(abs(self._modifiedZScore) > self._threshold)
    
  def run(self) -> ResultDto:
    self._newDataPoint = pd.Series(self._newData).median()
    self._dataSeries = pd.Series(self._newDataPoint + self._historicalData)
    self._medianAbsoluteDeviation = self._calculateMedianAbsoluteDeviation()

    self._modifiedZScore = self._calculateModifiedZScore(self._newDataPoint)
    return ResultDto(self._executionId, self._threshold, self._type.value, self._meanAbsoluteDeviation, self._medianAbsoluteDeviation, self._modifiedZScore, self._newDataPoint, self._isAnomaly())

class RowCountModel(StatisticalModel):
  def __init__(self, newData: list[float], historicalData: list[float], threshold: int, executionId: str) -> None:
    super().__init__(newData, historicalData, ModelType.ROW_COUNT, threshold, executionId)

  

  

     
