"""
Currency standardization module for Hotel Batch Data Pipeline.
Standardizes monetary amounts to Indian Rupee (INR) based on effective exchange rates.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, Optional, Tuple
import pandas as pd

from src.utils.database import DatabaseManager, db
from src.monitoring.logger import get_logger

logger = get_logger("currency_transformer")


class CurrencyTransformer:
    """Manages currency conversion using database exchange rate reference table."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or db
        self.exchange_rates: Dict[str, float] = self._load_exchange_rates()

    def _load_exchange_rates(self) -> Dict[str, float]:
        """Fetch current exchange rates from dim_currency table."""
        tbl = self.db.get_table_name("analytics_dw", "dim_currency")
        try:
            df = self.db.fetch_dataframe(f"SELECT currency_code, exchange_rate_to_inr FROM {tbl}")
            rates = dict(zip(df["currency_code"].str.upper(), df["exchange_rate_to_inr"].astype(float)))
            logger.info(f"Loaded exchange rates from {tbl}: {rates}")
            return rates
        except Exception as exc:
            logger.warning(f"Could not load rates from database ({exc}). Using static fallback.")
            return {
                "INR": 1.0,
                "USD": 86.5,
                "EUR": 92.3,
                "GBP": 108.5
            }

    def convert_to_inr(self, amount: Any, currency: str) -> Tuple[Optional[float], Optional[float], bool]:
        """
        Convert an amount in specified currency to INR.

        Returns:
            Tuple of (converted_amount_inr, exchange_rate_used, is_valid)
        """
        if amount is None or pd.isna(amount) or currency is None or pd.isna(currency):
            return None, None, False

        curr = str(currency).strip().upper()
        if curr not in self.exchange_rates:
            return None, None, False

        try:
            amt_dec = Decimal(str(amount).strip())
            rate_dec = Decimal(str(self.exchange_rates[curr]))
            converted = (amt_dec * rate_dec).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            return float(converted), float(rate_dec), True
        except Exception:
            return None, None, False

    def standardize_dataframe(
        self,
        df: pd.DataFrame,
        amount_col: str,
        currency_col: str,
        out_amount_col: str = "room_rate_inr",
        out_rate_col: str = "exchange_rate_used"
    ) -> pd.DataFrame:
        """
        Vectorized/batch currency standardization for DataFrames.
        """
        res_df = df.copy()
        rates = []
        converted_amts = []
        is_valid_curr = []

        for _, row in res_df.iterrows():
            amt = row.get(amount_col)
            curr = row.get(currency_col)
            c_amt, rate, ok = self.convert_to_inr(amt, curr)
            converted_amts.append(c_amt)
            rates.append(rate)
            is_valid_curr.append(ok)

        res_df[out_amount_col] = converted_amts
        res_df[out_rate_col] = rates
        res_df["is_currency_valid"] = is_valid_curr
        return res_df
