"""Tests for regime detection."""

from __future__ import annotations

import pandas as pd

from src.processors.regime import RegimeDetector


def test_regime_detector_and_historical_analog():
    detector = RegimeDetector(
        {
            "pmi": 49.0,
            "pmi_trend": "falling",
            "cpi": 0.4,
            "cpi_trend": "rising",
            "policy_stance": "dilemma",
            "credit_impulse": "stable",
            "dxy_state": "strengthening",
        }
    )
    result = detector.detect_regime()
    assert result["current_regime"] == "stagflation"

    history = pd.DataFrame(
        [
            {"period": "2019Q1", "regime": "recovery", "pmi": 50.5, "cpi_monthly": 0.2, "policy_stance": "easing", "credit_impulse": "expanding", "dxy_state": "weakening"},
            {"period": "2022Q3", "regime": "stagflation", "pmi": 48.7, "cpi_monthly": 0.7, "policy_stance": "neutral", "credit_impulse": "contracting", "dxy_state": "strengthening"},
        ]
    )
    analog = detector.find_historical_analog(history)
    assert analog["period"] == "2022Q3"
