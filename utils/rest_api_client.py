import aiohttp
from .logger import logger
import datetime
import pandas as pd
import asyncio

class RestApiClient:
    BASE_URL = "https://api.upstox.com/v2"

    def __init__(self, auth_handler):
        self.auth_handler = auth_handler
        self.session = None
        self.ohlc_cache = {}
        self.backtest_mode = self.auth_handler.config_manager.get_boolean('backtest', 'enabled', fallback=False)

    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=await self._get_headers())
        return self.session

    async def _get_headers(self):
        return {
            'Accept': 'application/json',
            'Api-Version': '2.0',
            'Authorization': f'Bearer {self.auth_handler.get_access_token()}'
        }

    async def _request(self, method, endpoint, params=None, retry=True, silence_error=False, timeout=15):
        try:
            session = await self._get_session()
            url = f"{self.BASE_URL}{endpoint}"

            atimeout = aiohttp.ClientTimeout(total=timeout)

            async with session.request(method, url, params=params, timeout=atimeout) as response:
                if response.status == 400:
                    body = await response.text()
                    if silence_error:
                        logger.debug(f"Upstox API 400 (Expected/Silenced) at {endpoint}. Body: {body}")
                    else:
                        logger.error(f"Upstox API 400 Bad Request at {endpoint}. Body: {body}")

                response.raise_for_status()
                return await response.json()
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            status = getattr(e, 'status', None)
            error_msg = str(e)
            if isinstance(e, asyncio.TimeoutError):
                status = 408
                error_msg = f"Request timeout after {timeout}s"

            if not silence_error:
                logger.warning(f"API call to {endpoint} failed (Status: {status}). Reason: {error_msg}")

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

    async def get_expiring_option_contracts(self, instrument_key, expiry_date):
        """Fetch contracts expiring today from expired-instruments endpoint.
        Tries Upstox v2 first, then v3 as fallback if v2 returns empty.
        Logs full response details to diagnose API issues.
        expiry_date: datetime.date or string 'YYYY-MM-DD'
        """
        params = {
            'instrument_key': instrument_key,
            'expiry_date': str(expiry_date)
        }
        endpoint = "/expired-instruments/option/contract"

        for base_url in [self.BASE_URL, "https://api.upstox.com/v3"]:
            full_url = f"{base_url}{endpoint}"
            logger.info(f"ContractManager: Trying expired-instruments: {full_url} params={params}")
            try:
                session = await self._get_session()
                atimeout = aiohttp.ClientTimeout(total=15)
                async with session.request('get', full_url, params=params, timeout=atimeout) as response:
                    body = await response.json()
                    data = body.get('data', []) or []
                    logger.info(f"ContractManager: expired-instruments response — status={response.status}, data_count={len(data)}, body_keys={list(body.keys()) if isinstance(body, dict) else type(body)}")
                    if response.status == 200 and data:
                        logger.info(f"ContractManager: Got {len(data)} expiry-day contracts from {base_url}")
                        return data
                    if response.status != 200:
                        logger.warning(f"ContractManager: {base_url} returned HTTP {response.status}")
                    else:
                        logger.warning(f"ContractManager: {base_url} returned 200 but data=[] for expiry={expiry_date}")
            except Exception as e:
                logger.warning(f"ContractManager: expired-instruments call failed for {base_url}: {e}")

        logger.error(f"ContractManager: All endpoints returned empty — no expiry-day contracts found for {expiry_date} (key={instrument_key})")
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

        df = pd.DataFrame()

        if to_date_str == today_str:
            if from_date_str == today_str:
                logger.info(f"DATA_FETCH: Intraday-only request for '{instrument_key}' ({interval}).")
                df = await self.get_intraday_candle_data(instrument_key, interval)
            else:
                yesterday = (datetime.datetime.now(ist) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

                logger.info(f"DATA_FETCH: Merging historical (up to {yesterday}) + intraday for '{instrument_key}' ({interval}).")

                hist_endpoint = f"/historical-candle/{instrument_key}/{interval}/{yesterday}/{from_date_str}"
                try:
                    hist_response = await self._request('get', hist_endpoint)
                    hist_candles = hist_response.get('data', {}).get('candles', [])
                    hist_df = self._format_historical_data(hist_candles)
                    logger.info(f"DATA_FETCH: Historical part returned {len(hist_candles)} candles for '{instrument_key}'.")
                except Exception as e:
                    logger.warning(f"Failed to fetch historical part of merged range: {e}")
                    hist_df = pd.DataFrame()

                intra_df = await self.get_intraday_candle_data(instrument_key, interval)

                df = pd.concat([hist_df, intra_df]).sort_index()
                df = df[~df.index.duplicated(keep='last')]
        else:
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
                return self.ohlc_cache[cache_key]

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
        if not instrument_keys:
            return {}

        all_results = {}
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
        if not instrument_keys:
            return {}

        all_results = {}
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
        try:
            await self._request('get', '/user/profile')
            return True
        except aiohttp.ClientResponseError as e:
            if e.status == 401:
                return False
            logger.warning(f"An unexpected error occurred during auth verification: {e}")
            return False

    def _format_historical_data(self, candles):
        if not candles:
            return pd.DataFrame()

        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        return df

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
