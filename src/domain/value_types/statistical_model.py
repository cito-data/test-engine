
from abc import ABC, abstractmethod
from enum import Enum
import pandas as pd 

class ModelType(Enum):
  ROW_COUNT = 0



class StatisticalModel(ABC):

  _newData: pd.Series
  _historicalData: pd.Series
  _type: ModelType

  _newDataPoint: float
  _dataSeries: pd.Series

  _median: int
  _medianAbsoluteDeviation: float
  _meanAbsoluteDeviation: float
  _modifiedZScores: pd.Series

  @abstractmethod
  def __init__(self, newData: list[float] , historicalData: list[float], type: ModelType, threshold: int) -> None:
    self._newData = pd.Series(newData)
    self._historicalData = pd.Series(historicalData)
    self._type = type
  
  def _absoluteDeviation(self, x) -> float:
    return abs(x - self._median)

  def _calculateMedianAbsoluteDeviation(self) -> float:
    self._median = self._dataSeries.median()
    absoluteDeviation = self._dataSeries.apply(self._absoluteDeviation)
    return absoluteDeviation.median()

  def _calculateModifiedZScore(self, x) -> float:
    # https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=terms-modified-z-score
    if self._medianAbsoluteDeviation == 0:
      return (x - self._median)/(1.253314*self._dataSeries.mad())
    return (x - self._median)/(1.486*self._medianAbsoluteDeviation)
    


  def run(self):
    self._newDataPoint = self._newData.mean()
    self._dataSeries = pd.concat([self._historicalData,pd.Series([self._newDataPoint])])
    self._medianAbsoluteDeviation = self._calculateMedianAbsoluteDeviation()

    self._modifiedZScores = self._dataSeries.apply(self._calculateModifiedZScore)
    print(self._dataSeries)
    print(self._modifiedZScores)  

class RowCountModel(StatisticalModel):
  def __init__(self, newData: list[float], historicalData: list[float]) -> None:
    super().__init__(newData, historicalData, ModelType.ROW_COUNT)

  

  

     
