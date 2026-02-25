from utils.logger import logger

class StrikeManager:
    def __init__(self, state_manager, atm_manager, config, instrument_name="NIFTY"):
        self.state_manager = state_manager
        self.atm_manager = atm_manager
        self.config = config
        self.instrument_name = instrument_name



    def get_strike_watchlist(self, primary_target_strike):
        """
        Generates a watchlist of strikes around the primary target strike.
        Includes 1 strike above and 1 strike below (Total 3: ATM, ATM+1, ATM-1).
        """
        if not primary_target_strike:
            return []

        strike_interval = self.config.get_int(self.instrument_name, 'strike_interval', fallback=50)
        num_strikes = self.config.get_int('settings', 'strikes_to_monitor', 1)
        watchlist = [primary_target_strike]

        for i in range(1, num_strikes + 1):
            watchlist.append(primary_target_strike + (i * strike_interval))
            watchlist.append(primary_target_strike - (i * strike_interval))
            
        return sorted(list(set(watchlist)))

    def get_recording_watchlist(self, atm_strike):
        """
        Generates a wide watchlist for recording purposes (ATM +/- 10).
        """
        if not atm_strike:
            return []

        strike_interval = self.config.get_int(self.instrument_name, 'strike_interval', fallback=50)
        num_strikes = 10
        watchlist = [atm_strike]

        for i in range(1, num_strikes + 1):
            watchlist.append(atm_strike + (i * strike_interval))
            watchlist.append(atm_strike - (i * strike_interval))

        return sorted(list(set(watchlist)))

    def find_and_get_target_strike_pair(self, expiry):
        """
        Finds the best target strike pair for a given expiry.
        """
        min_diff = float('inf')
        best_pair = None

        atm_strike = self.atm_manager.strikes.get('atm')
        if not atm_strike: return None

        watchlist = self.get_strike_watchlist(atm_strike)

        for strike in watchlist:
            ce_key = self.atm_manager.find_instrument_key_by_strike(strike, 'CALL', expiry)
            pe_key = self.atm_manager.find_instrument_key_by_strike(strike, 'PUT', expiry)

            ce_ltp = self.state_manager.option_prices.get(ce_key)
            pe_ltp = self.state_manager.option_prices.get(pe_key)

            if ce_ltp is not None and pe_ltp is not None:
                diff = abs(float(ce_ltp) - float(pe_ltp))
                if diff < min_diff:
                    min_diff = diff
                    best_pair = {'strike': strike, 'ce_ltp': ce_ltp, 'pe_ltp': pe_ltp}

        if best_pair:
             logger.debug(f"V2 Selection: Best strike for {expiry} identified: {best_pair['strike']} (Diff: {min_diff:.2f})")

        return best_pair
