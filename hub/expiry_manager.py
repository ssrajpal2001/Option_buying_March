import datetime
import calendar
from utils.logger import logger

class ExpiryManager:
    def __init__(self, all_contracts, near_expiry_date=None, monthly_expiries=None):
        self.all_contracts = all_contracts
        self.near_expiry_date = near_expiry_date
        self.monthly_expiries = monthly_expiries or []

    @staticmethod
    def get_last_thursday(year, month):
        """Returns the last Thursday (NIFTY monthly expiry) of the given month."""
        last_day = calendar.monthrange(year, month)[1]
        for day in range(last_day, last_day - 7, -1):
            candidate = datetime.date(year, month, day)
            if candidate.weekday() == 3:  # Thursday
                return candidate
        return None

    @staticmethod
    def is_monthly_expiry_date(d):
        """Returns True if date d is the last Thursday of its month (i.e., a monthly expiry)."""
        return d == ExpiryManager.get_last_thursday(d.year, d.month)

    def get_trade_expiry_date(self, today=None, mode='WEEKLY'):
        """Determines the target trade expiry date based on mode (WEEKLY, MONTHLY, EXPIRY, etc.)"""
        if today is None: today = datetime.date.today()
        if hasattr(today, 'date'): today = today.date()

        all_future_expiries = sorted(list({
            c.expiry.date() for c in self.all_contracts
            if hasattr(c, 'expiry') and c.expiry and c.expiry.date() >= today
        }))

        if not all_future_expiries:
            logger.error("No future expiries found in contract master.")
            return None

        mode_upper = mode.upper()

        if mode_upper == 'MONTHLY':
            # On expiry day, the broker API may exclude today's expiring contracts.
            # If today IS the last Thursday of the month (monthly expiry day), treat today as
            # the monthly expiry even if it's missing from all_future_expiries.
            if self.is_monthly_expiry_date(today) and today not in all_future_expiries:
                logger.warning(f"ExpiryManager: MONTHLY mode on expiry day {today} — contracts not in master list. Using today as monthly expiry.")
                return today

            first_month = all_future_expiries[0].month
            first_year = all_future_expiries[0].year
            month_expiries = [exp for exp in all_future_expiries if exp.month == first_month and exp.year == first_year]

            # The monthly expiry is the last Thursday of the month (last entry in sorted month list)
            monthly = month_expiries[-1] if month_expiries else None
            if monthly and self.is_monthly_expiry_date(monthly):
                return monthly

            # Fallback: scan month_expiries for any entry that is a last-Thursday
            for exp in reversed(month_expiries):
                if self.is_monthly_expiry_date(exp):
                    return exp

            return monthly

        # 'EXPIRY' is treated as 'WEEKLY' (nearest available expiry)
        expiry_map = {'WEEKLY': 0, 'EXPIRY': 0, 'NEXT_WEEK': 1, 'NEXT_TO_NEXT_WEEK': 2}
        idx = expiry_map.get(mode_upper, 0)

        # Edge case: on expiry day, today might not be in all_future_expiries (broker excluded it).
        # Detect by checking if the first future expiry is strictly after today (meaning today
        # was excluded). This works regardless of which weekday NSE sets as expiry day.
        if idx == 0 and all_future_expiries[0] > today:
            logger.warning(f"ExpiryManager: WEEKLY/EXPIRY mode — today {today} not in future expiries list. Using today as expiry.")
            return today

        return all_future_expiries[idx] if idx < len(all_future_expiries) else all_future_expiries[-1]

    def calculate_expiry_date(self, mode, today=None):
        """Helper to calculate expiry date for a given mode string."""
        if today is None: today = datetime.date.today()
        if hasattr(today, 'date'): today = today.date()

        unique_weekly = sorted(list({
            c.expiry.date() for c in self.all_contracts
            if c.instrument_type in ['CE', 'PE'] and c.expiry.date() >= today
        }))

        mode = mode.upper()

        # Edge case: on expiry day, today may be excluded from unique_weekly (broker API omits
        # expiring contracts). Detect by checking if the list is empty or starts after today.
        # Works regardless of which weekday NSE uses as expiry day.
        if not unique_weekly or unique_weekly[0] > today:
            logger.warning(f"ExpiryManager: calculate_expiry_date — today {today} excluded from contract list. Injecting as expiry.")
            unique_weekly = [today] + unique_weekly

        if not unique_weekly: return None

        if mode in ['CURRENT_WEEK', 'WEEKLY', 'EXPIRY']: return unique_weekly[0]
        if mode == 'NEXT_WEEK': return unique_weekly[1] if len(unique_weekly) > 1 else unique_weekly[0]
        if mode == 'MONTHLY':
            curr_month, curr_year = unique_weekly[0].month, unique_weekly[0].year
            monthly_expiry = next((e for e in self.monthly_expiries if e.month == curr_month and e.year == curr_year), None)
            return monthly_expiry or self.near_expiry_date
        return self.near_expiry_date
