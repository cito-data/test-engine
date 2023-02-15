from test_type import QuantColumnTest, QuantMatTest
from abc import ABC, abstractmethod
from dataclasses import dataclass
import datetime
from typing import Union
import pandas as pd
import numpy as np
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
    boundsIntervalRelative: Union[float, None]


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


def _closestValue(arr: "list[float]", x: float) -> float:
    if (not len(arr)):
        raise Exception('Empty array provided. Cannot find closest val.')
    closestValue = arr[0]
    for value in arr:
        if abs(value - x) < abs(closestValue - x):
            closestValue = value
    return closestValue


def _adjustValue(value: float, testType: Union[QuantMatTest, QuantColumnTest]) -> float:
    return value if testType == QuantColumnTest.ColumnDistribution or testType == QuantColumnTest.ColumnDistribution.value or testType == QuantColumnTest.ColumnFreshness or testType == QuantColumnTest.ColumnFreshness.value or value > 0 else 0


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

        self._expectedValue = _adjustValue(self._median, self._testType)
        self._expectedValueUpper = _adjustValue(
            self._calculateBound(self._threshold*1), self._testType)
        self._expectedValueLower = _adjustValue(self._calculateBound(
            self._threshold*-1), self._testType)

        anomalyCheckResult = self._runAnomalyCheck()

        return _ZScoreResult(anomalyCheckResult.expectedValue, anomalyCheckResult.expectedValueUpper, anomalyCheckResult.expectedValueLower, anomalyCheckResult.deviation, anomalyCheckResult.isAnomaly, self._median, self._medianAbsoluteDeviation, self._meanAbsoluteDeviation, self._modifiedZScore)


class _ForecastAnalysis(_Analysis):
    _yhat: float
    _yhat_lower: float
    _yhat_upper: float
    _trend: float
    _trend_lower: float
    _trend_upper: float
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

        expectedValues: list[float] = [el for el in [
            self._yhat, self._trend] if el is not None]

        bounds: list[float] = [el for el in [self._yhat_lower, self._yhat_upper,
                                             self._trend_lower, self._trend_upper] if el is not None]
        upperBound = max(bounds)
        lowerBound = min(bounds)
        expectedValue = _closestValue(
            expectedValues, (upperBound + lowerBound)/2)

        deviation = y / expectedValue - \
            1 if expectedValue != 0 else -9999
        isAnomaly = bool(
            y > upperBound or y < lowerBound)

        return _AnalysisResult(expectedValue, upperBound, lowerBound, deviation, isAnomaly)

    def analyze(self) -> _AnalysisResult:
        # m = Prophet(changepoint_prior_scale=0.1)
        m = Prophet()
        m.fit(self._historicalData)

        dates = pd.date_range(
            end=pd.Timestamp.now(),
            periods=1,
        )

        future = pd.DataFrame({'ds': dates})

        forecast = m.predict(future)

        self._yhat = _adjustValue(forecast['yhat'].values[0], self._testType)
        self._yhat_lower = _adjustValue(
            forecast['yhat_lower'].values[0], self._testType)
        self._yhat_upper = _adjustValue(
            forecast['yhat_upper'].values[0], self._testType)
        self._trend = _adjustValue(forecast['trend'].values[0], self._testType)
        self._trend_lower = _adjustValue(
            forecast['trend_lower'].values[0], self._testType)
        self._trend_upper = _adjustValue(
            forecast['trend_upper'].values[0], self._testType)
        self._daily = _adjustValue(
            forecast['daily'].values[0], self._testType) if 'daily' in forecast.columns else None
        self._daily_lower = _adjustValue(
            forecast['daily_lower'].values[0], self._testType) if 'daily_lower' in forecast.columns else None
        self._daily_upper = _adjustValue(
            forecast['daily_upper'].values[0], self._testType) if 'daily_upper' in forecast.columns else None
        self._weekly = _adjustValue(
            forecast['weekly'].values[0], self._testType) if 'weekly' in forecast.columns else None
        self._weekly_lower = _adjustValue(
            forecast['weekly_lower'].values[0], self._testType) if 'weekly_lower' in forecast.columns else None
        self._weekly_upper = _adjustValue(
            forecast['weekly_upper'].values[0], self._testType) if 'weekly_upper' in forecast.columns else None
        self._yearly = _adjustValue(
            forecast['yearly'].values[0], self._testType) if 'yearly' in forecast.columns else None
        self._yearly_lower = _adjustValue(
            forecast['yearly_lower'].values[0], self._testType) if 'yearly_lower' in forecast.columns else None
        self._yearly_upper = _adjustValue(
            forecast['yearly_upper'].values[0], self._testType) if 'yearly_upper' in forecast.columns else None

        anomalyCheckResult = self._runAnomalyCheck()

        return _AnalysisResult(anomalyCheckResult.expectedValue, anomalyCheckResult.expectedValueUpper, anomalyCheckResult.expectedValueLower, anomalyCheckResult.deviation, anomalyCheckResult.isAnomaly)


class _QuantModel(ABC):
    _newDataPoint: "tuple[str, float]"

    _zScoreAnalysis: _ZScoreAnalysis
    _forecastAnalysis: _ForecastAnalysis

    _importanceThreshold: float
    _boundsIntervalRelative: float

    _testType: Union[QuantMatTest, QuantColumnTest]

    @ abstractmethod
    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int, testType: Union[QuantMatTest, QuantColumnTest], importanceThreshold: float, boundsIntervalRelative: float) -> None:
        self._zScoreAnalysis = _ZScoreAnalysis(
            newDataPoint, historicalData, threshold, testType)
        self._forecastAnalysis = _ForecastAnalysis(
            newDataPoint, historicalData, threshold, testType)
        self._newDataPoint = newDataPoint
        self._importanceThreshold = importanceThreshold
        self._boundsIntervalRelative = boundsIntervalRelative

    @ staticmethod
    def _calcAnomalyImportance(y: float, lower: float, upper: float) -> float:
        boundsIntervalAbsolute = upper - lower
        yAbsoluteBoundaryDistance = y - \
            upper if y > upper else lower - y
        if yAbsoluteBoundaryDistance == 0 and boundsIntervalAbsolute == 0:
            raise Exception(
                'Detected unusual bounds and y value. Cannot calculate importance')
        importance = yAbsoluteBoundaryDistance / \
            boundsIntervalAbsolute
        return importance

    @ staticmethod
    def _calcImportanceThreshold(boundsIntervalRelative: float, importance: Union[float, None]) -> float:
        slope = -10
        yOffset = importance - slope*boundsIntervalRelative if importance else 1.7
        threshold = slope * boundsIntervalRelative + yOffset
        return threshold if threshold > 0 else 0

    def run(self) -> ResultDto:
        zScoreAnalysisResult = self._zScoreAnalysis.analyze()
        forecastAnalysisResult = self._forecastAnalysis.analyze()

        expectedValueLower = zScoreAnalysisResult.expectedValueLower if zScoreAnalysisResult.expectedValueLower < forecastAnalysisResult.expectedValueLower else forecastAnalysisResult.expectedValueLower
        expectedValueUpper = zScoreAnalysisResult.expectedValueUpper if zScoreAnalysisResult.expectedValueUpper > forecastAnalysisResult.expectedValueUpper else forecastAnalysisResult.expectedValueUpper
        expectedValue = _closestValue(
            [zScoreAnalysisResult.expectedValue, forecastAnalysisResult.expectedValue], (expectedValueLower + expectedValueUpper)/2)

        isAnomaly = zScoreAnalysisResult.isAnomaly and forecastAnalysisResult.isAnomaly and (
            self._newDataPoint[1] < expectedValueLower or self._newDataPoint[1] > expectedValueUpper)

        importance = None
        localBoundsIntervalRelative = None
        if (isAnomaly):
            if self._importanceThreshold == None:
                raise Exception('Missing importance threshold')

            y = self._newDataPoint[1]

            importance = self._calcAnomalyImportance(
                y, expectedValueLower, expectedValueUpper)

            globalImportanceThreshold = .8 if self._testType == QuantColumnTest.ColumnNullness or self._testType == QuantColumnTest.ColumnNullness.value \
                or self._testType == QuantColumnTest.ColumnUniqueness or self._testType == QuantColumnTest.ColumnUniqueness.value else .8

            isAnomaly = importance > globalImportanceThreshold

        deviation = zScoreAnalysisResult.deviation if abs(zScoreAnalysisResult.expectedValue - self._newDataPoint[1]) <= abs(
            forecastAnalysisResult.expectedValue - self._newDataPoint[1]) else forecastAnalysisResult.deviation

        return ResultDto(zScoreAnalysisResult.meanAbsoluteDeviation, zScoreAnalysisResult.medianAbsoluteDeviation, zScoreAnalysisResult.modifiedZScore, expectedValue, expectedValueUpper, expectedValueLower, deviation, _AnomalyResult(isAnomaly, importance, localBoundsIntervalRelative))


class CommonModel(_QuantModel):
    def __init__(self, newDataPoint: "tuple[str, float]", historicalData: "list[tuple[str, float]]", threshold: int, testType: Union[QuantMatTest, QuantColumnTest], importanceThreshold: float, boundsIntervalRelative: float) -> None:
        super().__init__(newDataPoint, historicalData,
                         threshold, testType, importanceThreshold, boundsIntervalRelative)
