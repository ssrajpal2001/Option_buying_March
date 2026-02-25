import asyncio
import websockets
import json
import ssl
import aiohttp
import inspect
from concurrent.futures import ThreadPoolExecutor
from .logger import logger
from . import MarketDataFeedV3_pb2 as pb
from hub.data_feed_base import DataFeed

class WebSocketManager(DataFeed):
    def __init__(self, api_client_manager):
        self.api_client_manager = api_client_manager
        self.message_handlers = []
        self.websocket = None
        self.is_connected = False
        self.subscriptions = {}  # To track symbols and modes
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 1000 # Increased for all-day reliability
        self.reconnect_delay = 5  # seconds
        self._running = False
        self._listener_task = None
        self._watchdog_task = None
        self._processor_task = None
        self._latency_task = None
        self._message_queue = asyncio.Queue(maxsize=2000)
        self._handler_tasks = set() # Store handler tasks to prevent them from being garbage-collected
        # Thread pool for sync handlers
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ws_handler")
        self._last_message_time = asyncio.get_event_loop().time()

    def register_message_handler(self, handler):
        """Adds a new message handler to the list of handlers."""
        if handler not in self.message_handlers:
            self.message_handlers.append(handler)
            handler_name = getattr(handler, '__name__', type(handler).__name__)
            logger.info(f"Message handler {handler_name} registered.")

    async def _get_auth_uri(self):
        logger.info("Authorizing WebSocket feed...")
        try:
            active_client = self.api_client_manager.get_active_client()
            if not active_client:
                raise ValueError("No active API client available for WebSocket authorization.")

            url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
            headers = {
                "Authorization": f"Bearer {active_client.auth_handler.get_access_token()}",
                "Accept": "application/json"
            }
            
            logger.info(f"Sending GET request to {url}...")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            
            uri = data.get("data", {}).get("authorized_redirect_uri")
            
            if not uri:
                raise ValueError("Could not extract authorized_redirect_uri from auth response.")

            logger.info("WebSocket authorization successful.")
            return uri
            
        except Exception as e:
            logger.error(f"Failed to authorize WebSocket feed: {e}", exc_info=True)
            raise

    async def connect_and_listen(self):
        """
        Main loop to connect and reconnect to the WebSocket.
        """
        self._running = True
        logger.info("WebSocketManager starting connection loop.")
        while self._running and self.reconnect_attempts < self.max_reconnect_attempts:
            try:
                logger.info(f"Attempting to authorize and connect... (Attempt {self.reconnect_attempts + 1})")
                uri = await self._get_auth_uri()
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                active_client = self.api_client_manager.get_active_client()
                if not active_client:
                    logger.error("Cannot connect WebSocket: No active API client.")
                    await asyncio.sleep(self.reconnect_delay)
                    continue

                headers = {
                    'Authorization': f'Bearer {active_client.auth_handler.get_access_token()}'
                }

                logger.info(f"Opening WebSocket connection to {uri[:50]}...")
                async with websockets.connect(uri, ssl=ssl_context, additional_headers=headers, ping_interval=30, ping_timeout=20) as websocket:
                    self.websocket = websocket
                    self.is_connected = True
                    self.reconnect_attempts = 0
                    logger.info("WebSocket connection established successfully.")

                    if self.subscriptions:
                        logger.info(f"V2 WebSocket: Sending re-subscription request for {len(self.subscriptions)} instruments after reconnect.")
                        # Ensure we clear any stale state in handler tasks
                        self._handler_tasks.clear()
                        await self._send_subscription_request()
                        logger.info("V2 WebSocket: Re-subscription request SENT.")
                    else:
                        logger.warning("V2 WebSocket: No existing instruments to re-subscribe to.")

                    # Start proactive watchdog, message processor and latency monitor
                    self._last_message_time = asyncio.get_event_loop().time()
                    self._watchdog_task = asyncio.create_task(self._run_watchdog())
                    self._processor_task = asyncio.create_task(self._message_processor())
                    self._latency_task = asyncio.create_task(self._run_latency_monitor())

                    try:
                        await self._listen_messages(websocket)
                    finally:
                        if self._watchdog_task:
                            self._watchdog_task.cancel()
                        if self._processor_task:
                            self._processor_task.cancel()
                        if self._latency_task:
                            self._latency_task.cancel()

            except (websockets.ConnectionClosed, AttributeError) as e:
                # Handle transient AttributeErrors that sometimes occur in websockets/aiohttp during disconnect
                if isinstance(e, AttributeError):
                    err_str = str(e)
                    if "resume_reading" in err_str or "NoneType" in err_str or "object has no attribute" in err_str:
                        logger.warning(f"Caught transient AttributeError during WebSocket closure: {err_str}")
                    else:
                        logger.error(f"An unexpected critical AttributeError occurred: {e}", exc_info=True)
                        self._running = False
                        break
                else:
                    logger.warning(f"WebSocket connection issue: {type(e).__name__} ({e}). Attempting to reconnect...")
            except Exception as e:
                logger.error(f"An unexpected error occurred in the connection loop: {e}", exc_info=True)
            finally:
                self.is_connected = False
                self.websocket = None
                if self._running:
                    self.reconnect_attempts += 1
                    logger.info(f"Reconnect attempt {self.reconnect_attempts}/{self.max_reconnect_attempts} in {self.reconnect_delay} seconds...")
                    await asyncio.sleep(self.reconnect_delay)

        if not self._running:
            logger.info("WebSocket manager stopped.")
        else:
            logger.error("Failed to reconnect to WebSocket after multiple attempts. Please restart.")

    async def _run_latency_monitor(self):
        """Monitors the event loop for stalls and blocks."""
        last_time = asyncio.get_event_loop().time()
        while True:
            try:
                await asyncio.sleep(1.0)
                now = asyncio.get_event_loop().time()
                # Expected time is 1.0s, anything significantly more is a stall
                drift = now - last_time - 1.0
                if drift > 2.0:
                    logger.warning(f"EVENT LOOP STALL DETECTED: Loop blocked for {drift:.2f}s! This can cause WebSocket disconnects.")
                last_time = now
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in latency monitor: {e}")

    async def _run_watchdog(self):
        """Proactively monitors silence and forces reconnection."""
        while True:
            try:
                await asyncio.sleep(10)
                silence_duration = asyncio.get_event_loop().time() - self._last_message_time

                # HEARTBEAT & MONITORING
                qsize = self._message_queue.qsize()
                if qsize > 100 or silence_duration > 30:
                    logger.info(f"WS WATCHDOG: Silence: {silence_duration:.1f}s | Queue: {qsize} | Handlers: {len(self.message_handlers)}")

                if silence_duration > 90:
                    logger.error(f"WATCHDOG: Proactive detection of WebSocket SILENCE for {silence_duration:.0f}s. Forcing reconnect.")
                    if self.websocket:
                        # Closing the websocket will trigger an exception in the listener loop
                        # Use a timeout to avoid hanging the watchdog itself
                        try:
                            # Note: We don't break the loop here. We want the watchdog to keep trying
                            # if the connection stays silent and doesn't close successfully.
                            await asyncio.wait_for(self.websocket.close(code=1001, reason="Watchdog silence timeout"), timeout=5.0)
                        except Exception as e:
                            logger.warning(f"Watchdog: Forced close failed or timed out: {e}")
                            # Final fallback: attempt transport-level closure
                            try:
                                if hasattr(self.websocket, 'transport') and self.websocket.transport:
                                    self.websocket.transport.close()
                            except: pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in WebSocket watchdog: {e}")

    async def _listen_messages(self, websocket):
        """
        Inner loop to listen for messages on an active connection.
        Optimized to ONLY receive and queue messages, minimizing loop blockages.
        """
        while self.is_connected and websocket is not None:
            try:
                # 30s timeout to detect stale connections
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                except AttributeError as ae:
                    # Specific hardening for 'NoneType' object has no attribute 'resume_reading'
                    # which can occur in some websockets/aiohttp versions during concurrent close.
                    if "resume_reading" in str(ae) or "NoneType" in str(ae):
                        logger.warning("Caught transient AttributeError in WebSocket recv loop. Connection likely closing.")
                        break
                    raise

                self._last_message_time = asyncio.get_event_loop().time()

                try:
                    # Put into queue for background processing
                    self._message_queue.put_nowait(message)
                except asyncio.QueueFull:
                    # Drop oldest if queue is full to prevent total hang
                    try:
                        self._message_queue.get_nowait()
                        self._message_queue.task_done()
                        self._message_queue.put_nowait(message)
                        logger.warning("WebSocket queue FULL. Dropped oldest packet.")
                    except Exception: pass

            except asyncio.TimeoutError:
                if not self.is_connected or websocket is None: break

                # WATCHDOG: If no message for 90 seconds, force a reconnect
                # Market data should be flowing at least every few seconds.
                silence_duration = asyncio.get_event_loop().time() - self._last_message_time
                if silence_duration > 90:
                    logger.error(f"WATCHDOG: WebSocket SILENCE for {silence_duration:.0f}s. Forcing reconnect.")
                    break

                logger.warning("No WebSocket data for 30s, sending ping...")
                try:
                    pong = await websocket.ping()
                    await asyncio.wait_for(pong, timeout=10.0)
                except Exception:
                    logger.error("WebSocket ping failed, reconnecting...")
                    break
            except (websockets.ConnectionClosed, AttributeError):
                # Re-raise to be handled by the outer connection loop
                raise
            except Exception as e:
                logger.error(f"Error in listener loop: {e}", exc_info=True)
                break

    async def _message_processor(self):
        """Background task to parse and route messages from the queue."""
        logger.info("WebSocket message processor started.")
        first_message_received = False
        while self._running:
            try:
                q_size = self._message_queue.qsize()
                if q_size > 500:
                    logger.warning(f"WS PROCESSOR: Falling behind! Queue size: {q_size}")

                message = await self._message_queue.get()

                if not first_message_received:
                    logger.info("First market data packet processed from queue. Data feed is active.")
                    first_message_received = True

                try:
                    # logger.debug("Parsing WebSocket message...")
                    feed_response = pb.FeedResponse()
                    feed_response.ParseFromString(message)

                    if self.message_handlers:
                        # Process handlers concurrently across orchestrators.
                        await asyncio.gather(*(self._run_handler_safe(h, feed_response) for h in self.message_handlers))

                except Exception as e:
                    logger.error(f"WebSocket processing error: {e}", exc_info=True)
                finally:
                    self._message_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in message processor loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def _run_handler_safe(self, handler, data):
        """Run handler with automatic sync/async detection and timeout"""
        try:
            start_time = asyncio.get_event_loop().time()

            # Get callable
            if callable(handler) and not hasattr(handler, 'handle_message'):
                func = handler
                name = getattr(handler, '__name__', 'handler')
            elif hasattr(handler, 'handle_message'):
                func = handler.handle_message
                name = f"{type(handler).__name__}.handle_message"
            else:
                return

            # Detect async vs sync
            if inspect.iscoroutinefunction(func):
                # ASYNC: Run directly
                await asyncio.wait_for(func(data), timeout=10.0)
            else:
                # SYNC: Run in thread pool to avoid blocking
                loop = asyncio.get_running_loop()
                await asyncio.wait_for(
                    loop.run_in_executor(self._executor, func, data),
                    timeout=10.0
                )

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > 2.0:
                logger.warning(f"Slow WebSocket handler {name}: {elapsed:.2f}s")

        except asyncio.TimeoutError:
            logger.error(f"TIMEOUT: WebSocket handler {name} blocked >10s")
        except Exception as e:
            logger.error(f"Handler error in {name}: {e}", exc_info=True)


    async def _send_subscription_request(self):
        if not self.is_connected or not self.websocket:
            logger.warning("Cannot subscribe, WebSocket is not connected.")
            return

        subs_by_mode = {}
        for symbol, mode in self.subscriptions.items():
            if mode not in subs_by_mode:
                subs_by_mode[mode] = []
            subs_by_mode[mode].append(symbol)

        for mode, symbols in subs_by_mode.items():
            data = {
                "guid": "guid-1",
                "method": "sub",
                "data": {
                    "mode": mode,
                    "instrumentKeys": symbols
                }
            }
            await self.websocket.send(json.dumps(data).encode('utf-8'))

    async def _send_unsubscription_request(self, symbols):
        if not self.is_connected or not self.websocket:
            logger.warning("Cannot unsubscribe, WebSocket is not connected.")
            return

        data = {
            "guid": "guid-1",
            "method": "unsub",
            "data": {
                "instrumentKeys": symbols
            }
        }
        await self.websocket.send(json.dumps(data).encode('utf-8'))

    def subscribe(self, symbols, mode='full'):
        new_subscriptions_by_mode = {}
        for symbol in symbols:
            if symbol not in self.subscriptions:
                self.subscriptions[symbol] = mode
                if mode not in new_subscriptions_by_mode:
                    new_subscriptions_by_mode[mode] = []
                new_subscriptions_by_mode[mode].append(symbol)

        if new_subscriptions_by_mode and self.is_connected:
            for sub_mode, sub_symbols in new_subscriptions_by_mode.items():
                asyncio.create_task(self._send_specific_subscription_request(sub_symbols, sub_mode))

    async def _send_specific_subscription_request(self, symbols, mode):
        if not self.is_connected or not self.websocket:
            logger.warning("Cannot subscribe, WebSocket is not connected.")
            return

        data = {
            "guid": "guid-2",
            "method": "sub",
            "data": {
                "mode": mode,
                "instrumentKeys": symbols
            }
        }
        await self.websocket.send(json.dumps(data).encode('utf-8'))

    def unsubscribe(self, symbols):
        unsubscribed = False
        for symbol in symbols:
            if symbol in self.subscriptions:
                del self.subscriptions[symbol]
                unsubscribed = True

        if unsubscribed and self.is_connected:
            asyncio.create_task(self._send_unsubscription_request(symbols))

    def start(self):
        """
        Starts the WebSocket connection and listening loop in a background task.
        Returns the task object.
        """
        if not self._listener_task:
            self._listener_task = asyncio.create_task(self.connect_and_listen())
            logger.info("WebSocket listener task created.")
        return self._listener_task

    async def close(self):
        self._running = False
        self.is_connected = False
        self._executor.shutdown(wait=False)
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket connection closed by client.")
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                logger.info("WebSocket listen task cancelled.")