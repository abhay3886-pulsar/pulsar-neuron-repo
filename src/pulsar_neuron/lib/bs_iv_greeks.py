# ------------------------ bs_iv_greeks.py ------------------------
from __future__ import annotations
import math
from datetime import datetime, timezone

SQRT_2PI = math.sqrt(2.0 * math.pi)
SECONDS_PER_YEAR = 365.0 * 24 * 3600  # calendar-year fraction

def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / SQRT_2PI

def _norm_cdf(x: float) -> float:
    # stable Φ(x) using erf
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def _d1(S: float, K: float, T: float, r: float, q: float, sigma: float) -> float:
    if sigma <= 0 or T <= 0 or S <= 0 or K <= 0:
        return float('nan')
    return (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))

def _d2(d1: float, sigma: float, T: float) -> float:
    return d1 - sigma * math.sqrt(T)

def bs_price(S: float, K: float, T: float, r: float, q: float, sigma: float, kind: str) -> float:
    """Black–Scholes price for European option (kind='CE' or 'PE') with continuous dividend yield q."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        # at expiry fallback (intrinsic, no time value)
        if kind == "CE":
            return max(0.0, S - K)
        else:
            return max(0.0, K - S)
    d1 = _d1(S, K, T, r, q, sigma)
    d2 = _d2(d1, sigma, T)
    df_r = math.exp(-r * T)
    df_q = math.exp(-q * T)
    if kind == "CE":
        return df_q * S * _norm_cdf(d1) - df_r * K * _norm_cdf(d2)
    else:
        return df_r * K * _norm_cdf(-d2) - df_q * S * _norm_cdf(-d1)

def bs_greeks(S: float, K: float, T: float, r: float, q: float, sigma: float, kind: str):
    """Returns (delta, gamma, theta_per_day, vega_per_1vol). Theta is per calendar day."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        # Degenerate: at/near expiry; finite-difference could be used, but return zeros safely.
        return 0.0, 0.0, 0.0, 0.0
    d1 = _d1(S, K, T, r, q, sigma)
    d2 = _d2(d1, sigma, T)
    df_r = math.exp(-r * T)
    df_q = math.exp(-q * T)
    Nd1 = _norm_cdf(d1)
    Nmd1 = _norm_cdf(-d1)
    nd1 = _norm_pdf(d1)

    if kind == "CE":
        delta = df_q * Nd1
        theta = (-df_q * S * nd1 * sigma / (2.0 * math.sqrt(T))
                 - r * df_r * K * _norm_cdf(d2)
                 + q * df_q * S * Nd1)
    else:
        delta = -df_q * Nmd1
        theta = (-df_q * S * nd1 * sigma / (2.0 * math.sqrt(T))
                 + r * df_r * K * _norm_cdf(-d2)
                 - q * df_q * S * Nmd1)

    gamma = df_q * nd1 / (S * sigma * math.sqrt(T))
    vega = df_q * S * nd1 * math.sqrt(T)  # per 1.0 change in vol (i.e., 100% = 1.0)

    theta_per_day = theta / 365.0  # report per calendar day
    return float(delta), float(gamma), float(theta_per_day), float(vega)

def implied_vol(price: float, S: float, K: float, T: float, r: float, q: float, kind: str,
                lo: float = 1e-4, hi: float = 5.0, tol: float = 1e-6, max_iter: int = 100) -> float | None:
    """Brent bracket search for IV in [lo, hi]. Returns None if not solvable."""
    if price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None

    def f(sig: float) -> float:
        return bs_price(S, K, T, r, q, sig, kind) - price

    flo, fhi = f(lo), f(hi)
    if math.isnan(flo) or math.isnan(fhi):
        return None
    # If price outside no-arbitrage bounds, bail
    if flo * fhi > 0:
        # Try expanding the bracket once
        hi2 = min(10.0, hi * 2.0)
        if f(hi2) * flo > 0:
            return None
        hi = hi2
        fhi = f(hi)

    a, b = lo, hi
    fa, fb = flo, fhi
    for _ in range(max_iter):
        m = 0.5 * (a + b)
        fm = f(m)
        if abs(fm) < tol:
            return m
        # bisection step
        if fa * fm <= 0:
            b, fb = m, fm
        else:
            a, fa = m, fm
    return None

def year_fraction(expiry: datetime, now: datetime | None = None) -> float:
    """Continuous time fraction (calendar) between now and expiry, in years."""
    now = now or datetime.now(timezone.utc)
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    dt = max(0.0, (expiry - now).total_seconds())
    return dt / SECONDS_PER_YEAR
