import pytest
from tasks.polymarket.detect import calculate_price_change, should_alert


def test_calculate_price_change():
    current = [0.55, 0.45]
    previous = [0.50, 0.50]
    
    change = calculate_price_change(current, previous)
    assert abs(change - 10.0) < 0.01


def test_calculate_price_change_no_previous():
    current = [0.55, 0.45]
    change = calculate_price_change(current, None)
    assert change == 0.0


def test_should_alert():
    assert should_alert(1.5, 1.0) == True
    assert should_alert(0.5, 1.0) == False
    assert should_alert(1.0, 1.0) == True
