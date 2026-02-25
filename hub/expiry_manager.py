import datetime
from utils.logger import logger

class ExpiryManager:
    def __init__(self, all_contracts, near_expiry_date=None, monthly_expiries=None):
        self.all_contracts = all_contracts
        self.near_expiry_date = near_expiry_date
        self.monthly_expiries = monthly_expiries or []

    def get_trade_expiry_date(self, today=None, mode='WEEKLY'):
        """Determines the target trade expiry date based on mode (WEEKLY, MONTHLY, etc.)"""
        if today is None: today = datetime.date.today()
        if hasattr(today, 'date'): today = today.date()

        all_future_expiries = sorted(list({
            c.expiry.date() for c in self.all_contracts
            if hasattr(c, 'expiry') and c.expiry and c.expiry.date() >= today
        }))

        if not all_future_expiries:
            logger.error("No future expiries found in contract master.")
            return None

        if mode.upper() == 'MONTHLY':
            first_month = all_future_expiries[0].month
            first_year = all_future_expiries[0].year
            month_expiries = [exp for exp in all_future_expiries if exp.month == first_month and exp.year == first_year]
            return month_expiries[-1] if month_expiries else None

        expiry_map = {'WEEKLY': 0, 'NEXT_WEEK': 1, 'NEXT_TO_NEXT_WEEK': 2}
        idx = expiry_map.get(mode.upper(), 0)
        return all_future_expiries[idx] if idx < len(all_future_expiries) else all_future_expiries[-1]

    def calculate_expiry_date(self, mode, today=None):
        """Helper to calculate expiry date for a given mode string."""
        if today is None: today = datetime.date.today()
        if hasattr(today, 'date'): today = today.date()

        unique_weekly = sorted(list({
            c.expiry.date() for c in self.all_contracts
            if c.instrument_type in ['CE', 'PE'] and c.expiry.date() >= today
        }))
        if not unique_weekly: return None

        mode = mode.upper()
        if mode in ['CURRENT_WEEK', 'WEEKLY']: return unique_weekly[0]
        if mode == 'NEXT_WEEK': return unique_weekly[1] if len(unique_weekly) > 1 else unique_weekly[0]
        if mode == 'MONTHLY':
            curr_month, curr_year = unique_weekly[0].month, unique_weekly[0].year
            monthly_expiry = next((e for e in self.monthly_expiries if e.month == curr_month and e.year == curr_year), None)
            return monthly_expiry or self.near_expiry_date
        return self.near_expiry_date
