import numpy as np
from scipy.stats import norm
from utils.logger import logger

def black_scholes_calculator(S, K, T, r, sigma, option_type="call"):
    """
    Calculates the Black-Scholes option price.

    Args:
        S (float): Spot price of the underlying asset.
        K (float): Strike price of the option.
        T (float): Time to expiration in years.
        r (float): Risk-free interest rate.
        sigma (float): Volatility of the underlying asset (implied volatility).
        option_type (str): Type of the option, 'call' or 'put'.

    Returns:
        float: The theoretical price of the option, or None if inputs are invalid.
    """
    if not all(isinstance(i, (int, float)) and i > 0 for i in [S, K, T, sigma]):
        logger.warning(f"Invalid input for Black-Scholes: S={S}, K={K}, T={T}, sigma={sigma}. All must be positive numbers.")
        return None
    if not isinstance(r, (int, float)) or r < 0:
        logger.warning(f"Invalid risk-free rate for Black-Scholes: r={r}. Must be a non-negative number.")
        return None
        
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    try:
        if option_type == "call":
            price = (S * norm.cdf(d1, 0.0, 1.0) - K * np.exp(-r * T) * norm.cdf(d2, 0.0, 1.0))
        elif option_type == "put":
            price = (K * np.exp(-r * T) * norm.cdf(-d2, 0.0, 1.0) - S * norm.cdf(-d1, 0.0, 1.0))
        else:
            logger.error(f"Invalid option type '{option_type}' for Black-Scholes calculation.")
            return None

        # Calculate Vega
        # Vega is the same for both calls and puts
        vega = S * norm.pdf(d1, 0.0, 1.0) * np.sqrt(T)

        return {
            "price": price,
            "vega": vega
        }
    except Exception as e:
        logger.error(f"Error in Black-Scholes calculation: {e}", exc_info=True)
        return None
