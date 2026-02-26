pt (asyncio.TimeoutError, aiohttp.ClientError) as e:
            status = getattr(e, 'status', None)
            error_msg = str(e)
            if isinstance(e, asyncio.TimeoutError):
                status = 408  # Request Timeout
                error_msg = f"Request timeout after {timeout}s"

            if not silence_error:
                logger.warning(f"API call to {endpoint} failed (Status: {status}). Reason: {error_msg}")

            # Retry on 429 (Rate Limit), 408 (Timeout), 5xx (Server Error) or network-level errors (status is None)
            should_retry = retry and (status is None or status == 429 or status == 408 or status >= 500)

            if should_retry:
                logger.info(f"Attempting to switch client and retry API call to {endpoint}...")
                switched = self.auth_handler.switch_client()
                if switched:
                    await asyncio.sleep(1)
                    return await self._request(method, endpoint, params, retry=False, silence_error=silence_error, timeout=timeout)
                else:
                    logger.error("Failover to secondary client failed or no more clients available.")

            raise e

    async def get_option_contracts(self, instrument_key):
        endpoint = "/option/contract"
        params = {'instrument_key': instrument_key}
        try:
            response = await self._request('get', endpoint, params=params)
            return response.get('data', [])
        except aiohttp.ClientResponseError as e:
            logger.error(f"Error fetching option contracts: {e}")
            return []

    async def get_historical_candle_data(self, instrument_key, interval, to_date, from_date):
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        today_str = datetime.datetime.now(ist).strftime('%Y-%m-%d')
        to_date_str = str(to_date)
        from_date_str = str(from_date)

        if self.backtest_mode:
            cache_key = (instrument_key, interval, to_date_str, from_date_str)
            if cache_key in self.ohlc_cache:
                return self.ohlc_cache[cache_key]

        # LOGIC: Upstox has separate endpoints for historical (past) and intraday (today).
        # today_str uses IST timezone to match Indian market dates correctly.

        df = pd.DataFrame()

        if to_date_str == today_str:
            # Range includes today
            if from_date_str == today_str:
                # Today only: Use intraday endpoint
                logger.info(f"DATA_FETCH: Intraday-only request for '{instrument_key}' ({interval}).")
                df = await self.get_intraday_candle_data(instrument_key, interval)
            else:
                # Range from past to today: Merge historical and intraday
                yesterday = (datetime.datetime.now(ist) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

                logger.info(f"DATA_FETCH: Merging historical (up to {yesterday}) + intraday for '{instrument_key}' ({interval}).")

                # 1. Fetch past data up to yesterday
                hist_endpoint = f"/historical-candle/{instrument_key}/{interval}/{yesterday}/{from_date_str}"
                try:
                    hist_response = await self._request('get', hist_endpoint)
                    hist_candles = hist_response.get('data', {}).get('candles', [])
                    hist_df = self._format_historical_data(hist_candles)
                    logger.info(f"DATA_FETCH: Historical part returned {len(hist_candles)} candles for '{instrument_key}'.")
                except Exception as e:
                    logger.warning(f"Failed to fetch historical part of merged range: {e}")
                    hist_df = pd.DataFrame()

                # 2. Fetch today's intraday data
                intra_df = await self.get_intraday_candle_data(instrument_key, interval)

                # 3. Merge
                df = pd.concat([hist_df, intra_df]).sort_index()
                df = df[~df.index.duplicated(keep='last')]
        else:
            # Past range only: Use standard historical endpoint
            logger.info(f"DATA_FETCH: Requesting historical data for '{instrument_key}' from {from_date_str} to {to_date_str} ({interval}).")
            endpoint = f"/historical-candle/{instrument_key}/{interval}/{to_date_str}/{from_date_str}"
            try:
                response = await self._request('get', endpoint)
                candles = response.get('data', {}).get('candles', [])
                df = self._format_historical_data(candles)
            except aiohttp.ClientResponseError as e:
                logger.error(f"Error fetching historical candle data: {e}")
                df = pd.DataFrame()

        if self.backtest_mode and not df.empty:
            self.ohlc_cache[cache_key] = df

        return df

    async def get_intraday_candle_data(self, instrument_key, interval):
        today_str = datetime.date.today().strftime('%Y-%m-%d')
        if self.backtest_mode:
            cache_key = (instrument_key, interval, today_str)
            if cache_key in self.ohlc_cache:
                # logger.info(f"CACHE HIT: Returning cached intraday OHLC data for {instrument_key} on {today_str}.")
                return self.ohlc_cache[cache_key]

        # Upstox API only accepts '1minute' or '30minute' for intraday interval
        valid_intervals = ['1minute', '30minute']
        if interval not in valid_intervals:
            logger.warning(f"Invalid interval '{interval}' for Upstox intraday candle API. Defaulting to '1minute'.")
            interval = '1minute'
        endpoint = f"/historical-candle/intraday/{instrument_key}/{interval}"
        try:
            response = await self._request('get', endpoint)
            candles = response.get('data', {}).get('candles', [])
            logger.info(f"DATA_FETCH: Intraday endpoint returned {len(candles)} candles for '{instrument_key}' ({interval}).")
            df = self._format_historical_data(candles)

            if self.backtest_mode:
                # logger.info(f"CACHE MISS: Caching intraday OHLC data for {instrument_key} on {today_str}.")
                self.ohlc_cache[cache_key] = df

            return df
        except aiohttp.ClientResponseError as e:
            logger.error(f"Error fetching intraday candle data: {e}")
            return pd.DataFrame()

    async def get_ltp(self, instrument_key, silence_error=False):
        endpoint = f"/market-quote/ltp"
        params = {'instrument_key': instrument_key}
        try:
            response = await self._request('get', endpoint, params=params, silence_error=silence_error)
            return response.get('data', {}).get(instrument_key, {}).get('last_price', 0.0)
        except aiohttp.ClientResponseError as e:
            if not silence_error:
                logger.error(f"Error fetching LTP: {e}")
            return 0.0

    async def get_ltps(self, instrument_keys, silence_error=False):
        """
        Fetches LTP for multiple instrument keys in a single call.
        instrument_keys: list of strings
        """
        if not instrument_keys:
            return {}

        all_results = {}
        # Upstox API allows max 50 keys per request
        chunk_size = 50
        for i in range(0, len(instrument_keys), chunk_size):
            chunk = instrument_keys[i:i + chunk_size]
            endpoint = "/market-quote/ltp"
            params = {'instrument_key': ','.join(chunk)}
            try:
                response = await self._request('get', endpoint, params=params, silence_error=silence_error)
                data = response.get('data', {})
                all_results.update({key: info.get('last_price', 0.0) for key, info in data.items()})
            except aiohttp.ClientResponseError as e:
                if not silence_error:
                    logger.error(f"Error fetching batch LTPs for chunk {i//chunk_size}: {e}")

        return all_results

    async def get_full_market_quote(self, instrument_keys):
        """Fetches full market quote including OHLC and volume."""
        if not instrument_keys:
            return {}

        all_results = {}
        # Upstox API allows max 50 keys per request
        chunk_size = 50
        for i in range(0, len(instrument_keys), chunk_size):
            chunk = instrument_keys[i:i + chunk_size]
            endpoint = "/market-quote/quotes"
            params = {'instrument_key': ','.join(chunk)}
            try:
                response = await self._request('get', endpoint, params=params)
                all_results.update(response.get('data', {}))
            except Exception as e:
                logger.error(f"Error fetching full market quote for chunk {i//chunk_size}: {e}")

        return all_results

    async def verify_authentication(self):
        """
        Makes a simple, low-cost API call to verify that the access token is valid.
        Returns True if successful, False otherwise.
        """
        try:
            await self._request('get', '/user/profile')
            return True
        except aiohttp.ClientResponseError as e:
            if e.status == 401:
                return False
            # For other errors, we might want to log them but not necessarily
            # treat it as a critical auth failure.
            logger.warning(f"An unexpected error occurred during auth verification: {e}")
            return False # Treat other errors as failures for safety

    def _format_historical_data(self, candles):
        if not candles:
            return pd.DataFrame()

        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True) # Ensure data is sorted chronologically
        return df

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
