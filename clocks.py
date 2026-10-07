import sys
import threading


class LamportClock:
    """Lamport logical clock of one process (a client or a replica).

    Every event is written as one log line. Updating the clock and writing the
    line happen under the same lock, so the lines in one process's log are
    always in increasing L order, even when 8 server threads log at once.
    """

    def __init__(self, name, log_path=None):
        self.name = name
        self._time = 0
        self._lock = threading.Lock()
        self._out = open(log_path, "a", buffering=1) if log_path else sys.stderr

    @property
    def time(self):
        with self._lock:
            return self._time

    def tick(self, event, description):
        """Local event (SEND, APPLY, ...): increment first, then log.
        Returns the new value, which a SEND attaches to the outgoing message."""
        with self._lock:
            self._time += 1
            self._log(event, description, self._time)
            return self._time

    def receive(self, description, received_time):
        """RECV: own = max(own, received) + 1, before the message is processed."""
        with self._lock:
            self._time = max(self._time, received_time) + 1
            self._log("RECV", description, self._time, received_time)
            return self._time

    def close(self):
        if self._out is not sys.stderr:
            self._out.close()

    def _log(self, event, description, time, received=None):
        line = f"[{self.name}] {event:<6} {description:<45} L={time}"
        if received is not None:
            line += f"  (received L={received})"
        print(line, file=self._out, flush=True)
