"""Event store: append-only event log. State is never stored directly — it is
reconstructed by replaying events for an aggregate id."""
import json
import os
import tempfile
import time
import contextlib


class EventStore:
    def __init__(self, path: str):
        self.path = path
        self.lock_path = path + ".lock"
        if not os.path.exists(path):
            self._write({"streams": {}})  # stream_id -> [event, ...]

    def _read(self):
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write(self, data):
        d = os.path.dirname(self.path) or "."
        fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    @contextlib.contextmanager
    def lock(self):
        while True:
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                break
            except FileExistsError:
                time.sleep(0.01)
        try:
            yield
        finally:
            if os.path.exists(self.lock_path):
                os.remove(self.lock_path)

    def append(self, stream_id: str, event_type: str, payload: dict):
        with self.lock():
            db = self._read()
            stream = db["streams"].setdefault(stream_id, [])
            event = {"seq": len(stream) + 1, "type": event_type, "payload": payload}
            stream.append(event)
            self._write(db)
            return event

    def load(self, stream_id: str):
        db = self._read()
        return db["streams"].get(stream_id, [])

    def all_streams(self):
        return self._read()["streams"]
