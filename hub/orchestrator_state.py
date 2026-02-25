from collections import defaultdict, deque

class OrchestratorState:
    def __init__(self):
        self.previous_atm_strike = None
        self.target_strike = None
        self.tick_history = defaultdict(lambda: deque(maxlen=100))
        self.v2_target_strike_pair = None
        self.current_target_strike = None
