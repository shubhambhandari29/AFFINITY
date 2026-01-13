import logging

from core import logging_config


def test_configure_logging_sets_handlers(monkeypatch):
    created = {}

    def fake_makedirs(path, exist_ok):
        created["path"] = path
        created["exist_ok"] = exist_ok

    class DummyHandler:
        def __init__(self, filename, when, interval, backupCount, encoding):
            self.filename = filename
            self.formatter = None

        def setFormatter(self, formatter):
            self.formatter = formatter

        def close(self):
            pass

    monkeypatch.setattr(logging_config.os, "makedirs", fake_makedirs)
    monkeypatch.setattr(logging_config.logging.handlers, "TimedRotatingFileHandler", DummyHandler)

    root = logging.getLogger()
    original_level = root.level
    original_handlers = root.handlers[:]
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    try:
        logging_config.configure_logging()

        assert created["path"] == "logs"
        assert created["exist_ok"] is True
        assert root.level == logging.INFO
        assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)
        assert any(isinstance(h, DummyHandler) for h in root.handlers)
    finally:
        for handler in root.handlers[:]:
            root.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        for handler in original_handlers:
            root.addHandler(handler)
        root.setLevel(original_level)
