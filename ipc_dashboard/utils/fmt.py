import math


def naira(val, decimals=1) -> str:
    """Format a number as Nigerian Naira (e.g. ₦1.5M, ₦250K)."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "₦0"
    val = float(val)
    neg = val < 0
    abs_val = abs(val)
    if abs_val >= 1_000_000_000:
        s = f"₦{abs_val / 1_000_000_000:.{decimals}f}B"
    elif abs_val >= 1_000_000:
        s = f"₦{abs_val / 1_000_000:.{decimals}f}M"
    elif abs_val >= 1_000:
        s = f"₦{abs_val / 1_000:.{decimals}f}K"
    else:
        s = f"₦{abs_val:,.0f}"
    return f"-{s}" if neg else s


def pct(val, decimals=1) -> str:
    if val is None:
        return "0%"
    return f"{float(val):.{decimals}f}%"


def count(val) -> str:
    if val is None:
        return "0"
    return f"{int(val):,}"
