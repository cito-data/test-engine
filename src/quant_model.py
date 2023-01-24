from test_type import QuantColumnTest, QuantMatTest
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from typing import Union
import pandas as pd
from prophet import Prophet


@dataclass
class _AnalysisResult(ABC):
    expectedValue: float
    expectedValueUpper: float
    expectedValueLower: float
    deviation: float
    isAnomaly: bool


@dataclass
class _ZScoreResult(_AnalysisResult):
    median: float
    medianAbsoluteDeviation: float
    meanAbsoluteDeviation: Union[float, None]
    modifiedZScore: float


@dataclass
class _AnomalyResult:
    isAnomaly: bool
    importance: Union[float, None]


@dataclass
class ResultDto:
    meanAbsoluteDeviation: Union[float, None]
    medianAbsoluteDeviation: float
    modifiedZScore: float

    expectedValue: float
    expectedValueUpper: float
    expectedValueLower: float

    deviation: float

    anomaly: _AnomalyResult


class _Analysis(ABC):
    _newDataPoint: pd.DataFrame
    _historicalData: pd.DataFrame
    _threshold: int
    _testType: Union[QuantMatTest, QuantColumnTest]

    @abstractmethod
    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int, testType: Union[QuantMatTest, QuantColumnTest]) -> None:
        self._newDataPoint = self._buildNewDataPointFrame(newDataPoint)
        self._historicalData = self._buildHistoricalDF(historicalData)
        self._threshold = threshold
        self._testType = testType

    def _buildNewDataPointFrame(self, newDataPoint: "tuple[str, float]") -> pd.DataFrame:
        return pd.DataFrame({'ds': pd.Series([newDataPoint[0]]), 'y': pd.Series([newDataPoint[1]])})

    def _buildHistoricalDF(self, historicalData: "list[tuple[str, float]]") -> pd.DataFrame:
        executedAt = []
        values = []

        for el in historicalData:
            executedAt.append(el[0])
            values.append(el[1])

        frame = {'ds': pd.Series(executedAt), 'y': pd.Series(values)}

        return pd.DataFrame(frame)

    @abstractmethod
    def _runAnomalyCheck(self):
        return

    @abstractmethod
    def analyze(self):
        return


class _ZScoreAnalysis(_Analysis):
    _median: float
    _medianAbsoluteDeviation: float
    _meanAbsoluteDeviation: Union[float, None]
    _modifiedZScore: float
    _expectedValue: float
    _expectedValueUpper: float
    _expectedValueLower: float

    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int, testType: Union[QuantMatTest, QuantColumnTest]) -> None:
        super().__init__(newDataPoint, historicalData, threshold, testType)

    def _absoluteDeviation(self, x) -> float:
        return abs(x - self._median)

    def _calculateMedianAbsoluteDeviation(self) -> float:
        allData = pd.concat(
            [self._historicalData, self._newDataPoint], ignore_index=True)
        values = allData['y']
        self._median = float(values.median())
        absoluteDeviation = values.apply(self._absoluteDeviation)
        return float(absoluteDeviation.median())

    def _mad(self):
        allData = pd.concat(
            [self._historicalData, self._newDataPoint], ignore_index=True)
        values = allData['y']
        return (values - values.mean()).abs().mean()

    def _calculateModifiedZScore(self) -> float:
        # https://www.ibm.com/docs/en/cognos-analytics/11.1.0?topic=terms-modified-z-score
        y = self._newDataPoint['y'].values[0]

        if y == None:
            raise Exception(
                'Cannot calc modified z-score. New data value not found')

        if self._medianAbsoluteDeviation == 0 and self._meanAbsoluteDeviation == 0:
            return 0.0
        if self._medianAbsoluteDeviation == 0:
            self._meanAbsoluteDeviation = self._mad()
            return (y - self._median)/(1.253314*self._meanAbsoluteDeviation)
        return (y - self._median)/(1.486*self._medianAbsoluteDeviation)

    def _calculateBound(self, zScoreThreshold: int) -> float:
        if self._meanAbsoluteDeviation == None:
            raise Exception(
                'Cannot calc bound. Mean abs deviation not available')

        if self._medianAbsoluteDeviation == 0:
            return (1.253314*self._meanAbsoluteDeviation)*zScoreThreshold + self._median
        return (1.486*self._medianAbsoluteDeviation)*zScoreThreshold + self._median

    def _runAnomalyCheck(self) -> _AnalysisResult:
        y = self._newDataPoint['y'].values[0]

        if y == None:
            raise Exception(
                'Cannot run anomaly check. New data value not found')

        isAnomaly = bool(abs(self._modifiedZScore) > self._threshold)
        deviation = y / \
            self._expectedValue - 1 if self._expectedValue > 0 else 0

        return _AnalysisResult(self._expectedValue, self._expectedValueUpper, self._expectedValueLower, deviation, isAnomaly)

    def analyze(self) -> _ZScoreResult:
        self._medianAbsoluteDeviation = self._calculateMedianAbsoluteDeviation()
        self._meanAbsoluteDeviation = self._mad()

        self._modifiedZScore = self._calculateModifiedZScore()

        self._expectedValue = self._median
        self._expectedValueUpper = self._calculateBound(self._threshold*1)

        lowerBound = self._calculateBound(
            self._threshold*-1)
        self._expectedValueLower = lowerBound if self._testType == QuantColumnTest.ColumnDistribution or self._testType == QuantColumnTest.ColumnDistribution.value or QuantColumnTest.ColumnFreshness or self._testType == QuantColumnTest.ColumnFreshness.value or lowerBound > 0 else 0

        anomalyCheckResult = self._runAnomalyCheck()

        return _ZScoreResult(anomalyCheckResult.expectedValue, anomalyCheckResult.expectedValueUpper, anomalyCheckResult.expectedValueLower, anomalyCheckResult.deviation, anomalyCheckResult.isAnomaly, self._median, self._medianAbsoluteDeviation, self._meanAbsoluteDeviation, self._modifiedZScore)


class _ForecastAnalysis(_Analysis):
    _yhat: float
    _yhat_lower: float
    _yhat_upper: float
    _daily: Union[float, None]
    _daily_lower: Union[float, None]
    _daily_upper: Union[float, None]
    _weekly: Union[float, None]
    _weekly_lower: Union[float, None]
    _weekly_upper: Union[float, None]
    _yearly: Union[float, None]
    _yearly_lower: Union[float, None]
    _yearly_upper: Union[float, None]

    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int, testType: Union[QuantMatTest, QuantColumnTest]) -> None:
        super().__init__(newDataPoint, historicalData, threshold, testType)

    def _runAnomalyCheck(self) -> _AnalysisResult:
        y = self._newDataPoint['y'].values[0]

        if y == None:
            raise Exception(
                'Cannot run anomaly check. New data value not found')

        if self._daily:
            if self._daily_lower == None or self._daily_upper == None:
                raise Exception('Missing daily lower or upper boundary')
            if y <= self._daily_upper and y >= self._daily_lower:
                deviation = (y / self._daily if self._daily !=
                             0 else y / 0.0001) - 1
                return _AnalysisResult(self._daily, self._daily_upper, self._daily_lower, deviation, False)

        if self._weekly:
            if self._weekly_lower == None or self._weekly_upper == None:
                raise Exception('Missing weekly lower or upper boundary')
            if y <= self._weekly_upper and y >= self._weekly_lower:
                deviation = (y / self._weekly if self._weekly !=
                             0 else y / 0.0001) - 1
                return _AnalysisResult(self._weekly, self._weekly_upper, self._weekly_lower, deviation, False)

        if self._yearly:
            if self._yearly_lower == None or self._yearly_upper == None:
                raise Exception('Missing yearly lower or upper boundary')
            if not y <= self._yearly_upper and y >= self._yearly_lower:
                deviation = (y / self._yearly if self._yearly !=
                             0 else y / 0.0001) - 1
                return _AnalysisResult(self._yearly, self._yearly_upper, self._yearly_lower, deviation, False)

        deviation = (y / self._yhat if self._yhat != 0 else y / 0.0001) - 1
        isAnomaly = bool(
            y > self._yhat_upper or y < self._yhat_lower)

        return _AnalysisResult(self._yhat, self._yhat_upper, self._yhat_lower, deviation, isAnomaly)

    def analyze(self) -> _AnalysisResult:
        m = Prophet()
        m.fit(self._historicalData)

        dates = pd.date_range(
            end=pd.Timestamp.now(),
            periods=1,
        )

        future = pd.DataFrame({'ds': dates})

        forecast = m.predict(future)

        self._yhat = forecast['yhat'].values[0]
        self._yhat_lower = forecast['yhat_lower'].values[0] if self._testType == QuantColumnTest.ColumnDistribution or self._testType == QuantColumnTest.ColumnDistribution.value or QuantColumnTest.ColumnFreshness or self._testType == QuantColumnTest.ColumnFreshness.value or forecast['yhat_lower'].values[0] > 0 else 0
        self._yhat_upper = forecast['yhat_upper'].values[0]
        self._daily = forecast['daily'].values[0] if 'daily' in forecast.columns else None
        self._daily_lower = forecast['daily_lower'].values[0] if 'daily_lower' in forecast.columns else None
        self._daily_upper = forecast['daily_upper'].values[0] if 'daily_upper' in forecast.columns else None
        self._weekly = forecast['weekly'].values[0] if 'weekly' in forecast.columns else None
        self._weekly_lower = forecast['weekly_lower'].values[0] if 'weekly_lower' in forecast.columns else None
        self._weekly_upper = forecast['weekly_upper'].values[0] if 'weekly_upper' in forecast.columns else None
        self._yearly = forecast['yearly'].values[0] if 'yearly' in forecast.columns else None
        self._yearly_lower = forecast['yearly_lower'].values[0] if 'yearly_lower' in forecast.columns else None
        self._yearly_upper = forecast['yearly_upper'].values[0] if 'yearly_upper' in forecast.columns else None

        anomalyCheckResult = self._runAnomalyCheck()

        return _AnalysisResult(anomalyCheckResult.expectedValue, anomalyCheckResult.expectedValueUpper, anomalyCheckResult.expectedValueLower, anomalyCheckResult.deviation, anomalyCheckResult.isAnomaly)


class _QuantModel(ABC):
    _newDataPoint: "tuple[str, float]"

    _zScoreAnalysis: _ZScoreAnalysis
    _forecastAnalysis: _ForecastAnalysis

    _importanceThreshold: Union[float, None]

    @ abstractmethod
    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int, testType: Union[QuantMatTest, QuantColumnTest], importanceThreshold: float) -> None:
        self._zScoreAnalysis = _ZScoreAnalysis(
            newDataPoint, historicalData, threshold, testType)
        self._forecastAnalysis = _ForecastAnalysis(
            newDataPoint, historicalData, threshold, testType)
        self._newDataPoint = newDataPoint
        self._importanceThreshold = importanceThreshold

    @staticmethod
    def _calcAnomalyImportance(y, upper, lower) -> float:
        boundaryInterval = upper - lower
        yAbsoluteBoundaryDistance = y - \
            upper if y > upper else lower - y
        importance = yAbsoluteBoundaryDistance/boundaryInterval
        return importance

    def run(self) -> ResultDto:
        zScoreAnalysisResult = self._zScoreAnalysis.analyze()
        forecastAnalysisResult = self._forecastAnalysis.analyze()

        expectedValueLower = zScoreAnalysisResult.expectedValueLower if zScoreAnalysisResult.expectedValueLower < forecastAnalysisResult.expectedValueLower else forecastAnalysisResult.expectedValueLower
        expectedValueUpper = zScoreAnalysisResult.expectedValueUpper if zScoreAnalysisResult.expectedValueUpper > forecastAnalysisResult.expectedValueUpper else forecastAnalysisResult.expectedValueUpper
        expectedValue = zScoreAnalysisResult.expectedValue if abs(zScoreAnalysisResult.expectedValue - self._newDataPoint[1]) <= abs(
            forecastAnalysisResult.expectedValue - self._newDataPoint[1]) else forecastAnalysisResult.expectedValue

        isAnomaly = zScoreAnalysisResult.isAnomaly and forecastAnalysisResult.isAnomaly and (
            self._newDataPoint[1] < expectedValueLower or self._newDataPoint[1] > expectedValueUpper)

        globalImportanceThreshold = 1.5

        importance = None
        if (isAnomaly):
            if self._importanceThreshold == None:
                raise Exception('Missing importance threshold')

            importance = self._calcAnomalyImportance(
                self._newDataPoint[1], expectedValueUpper, expectedValueLower)
            isAnomaly = importance > globalImportanceThreshold and importance > self._importanceThreshold

        deviation = zScoreAnalysisResult.deviation if abs(zScoreAnalysisResult.expectedValue - self._newDataPoint[1]) <= abs(
            forecastAnalysisResult.expectedValue - self._newDataPoint[1]) else forecastAnalysisResult.deviation

        return ResultDto(zScoreAnalysisResult.meanAbsoluteDeviation, zScoreAnalysisResult.medianAbsoluteDeviation, zScoreAnalysisResult.modifiedZScore, expectedValue, expectedValueUpper, expectedValueLower, deviation, _AnomalyResult(isAnomaly, importance))


class CommonModel(_QuantModel):
    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int, testType: Union[QuantMatTest, QuantColumnTest], importanceThreshold: float) -> None:
        super().__init__(newDataPoint, historicalData,
                         threshold, testType, importanceThreshold)
