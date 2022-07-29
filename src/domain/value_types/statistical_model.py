
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Sequence, Union
import pandas as pd 

class AnomalyMessage(Enum):
  ROW_COUNT = 'todo - row count message'

class ModelType(Enum):
  ROW_COUNT = 'ROW_COUNT'

@dataclass
class ResultDto:
  threshold: int
  type: str

  meanAbsoluteDeviation: Union[float, None]
  medianAbsoluteDeviation: float
  modifiedZScore: float
  
  newDatapoint: float
  isAnomaly: bool

  expectedValue: float
  expectedValueUpperBoundary: float
  expectedValueLowerBoundary: float

  deviation: float

class StatisticalModel(ABC):

  _newData: list[float]
  _historicalData: list[float]
  _type: ModelType
  _threshold: int

  _newDataPoint: float
  _dataSeries: pd.Series

  _median: float
  _medianAbsoluteDeviation: float
  _meanAbsoluteDeviation: Union[float, None] = None
  _modifiedZScore: float

  _expectedValue: float
  _expectedValueUpperBoundary: float
  _expectedValueLowerBoundary: float

  _deviation: float
  

  @abstractmethod
  def __init__(self, newData: list[float] , historicalData: list[float], type: ModelType, threshold: int) -> None:
    self._newData = newData
    self._historicalData = historicalData
    self._type = type
    self._threshold = threshold
  
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

  def _calculateBoundary(self, modifiedZScore: int) -> float:
    if self._medianAbsoluteDeviation == 0:
      self._meanAbsoluteDeviation = self._dataSeries.mad()
      return (1.253314*self._meanAbsoluteDeviation)*modifiedZScore + self._median
    return (1.486*self._medianAbsoluteDeviation)*modifiedZScore + self._median

  def _isAnomaly(self) -> bool:
    return bool(abs(self._modifiedZScore) > self._threshold)
    
  def run(self) -> ResultDto:
    self._newDataPoint = pd.Series(self._newData).median()
    self._dataSeries = pd.Series(self._newDataPoint + self._historicalData)
    self._medianAbsoluteDeviation = self._calculateMedianAbsoluteDeviation()

    self._modifiedZScore = self._calculateModifiedZScore(self._newDataPoint)

    self._expectedValue = self._median
    self._expectedValueUpperBoundary = self._calculateBoundary(self._threshold*1)
    self._expectedValueLowerBoundary = self._calculateBoundary(self._threshold*-1)

    self._deviation = self._newDataPoint/self._expectedValue

    return ResultDto(self._threshold, self._type.value, self._meanAbsoluteDeviation, self._medianAbsoluteDeviation, self._modifiedZScore, self._newDataPoint, self._isAnomaly(), self._expectedValue, self._expectedValueUpperBoundary, self._expectedValueLowerBoundary, self._deviation)

class RowCountModel(StatisticalModel):
  def __init__(self, newData: list[float], historicalData: list[float], threshold: int, ) -> None:
    super().__init__(newData, historicalData, ModelType.ROW_COUNT, threshold)

  

  

     
