from typing import Dict, Any, Optional


def calculate_price_change(current_prices: list, previous_prices: Optional[list]) -> float:
    """Calculate max price change percentage across outcomes."""
    if not previous_prices:
        return 0.0
    
    max_change = 0.0
    for curr, prev in zip(current_prices, previous_prices):
        # Convert to float if they're strings
        curr_val = float(curr) if isinstance(curr, str) else curr
        prev_val = float(prev) if isinstance(prev, str) else prev
        
        if prev_val > 0:
            change = abs((curr_val - prev_val) / prev_val * 100)
            max_change = max(max_change, change)
    
    return max_change


def should_alert(price_change: float, threshold: float = 1.0) -> bool:
    """Check if price change exceeds threshold."""
    return price_change >= threshold
