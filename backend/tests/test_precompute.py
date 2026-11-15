import threading
import time
from datetime import date

from app.models.price import CoinDaily
from app.services import precompute
from app.services.precompute import get_or_compute


def _add_day(db, d, close=100.0):
    db.add(CoinDaily(symbol="BTC", date=d, close=close, volume=1.0, fng=50))
    db.commit()


class TestPrecompute:
    def test_same_data_version_reuses_result(self, db):
        _add_day(db, date(2026, 11, 1))
        calls = []

        def compute():
            calls.append(1)
            return len(calls)

        assert get_or_compute("k", db, "BTC", compute) == 1
        assert get_or_compute("k", db, "BTC", compute) == 1
        assert len(calls) == 1

    def test_new_day_invalidates_result(self, db):
        _add_day(db, date(2026, 11, 1))
        calls = []

        def compute():
            calls.append(1)
            return len(calls)

        get_or_compute("k", db, "BTC", compute)
        _add_day(db, date(2026, 11, 2))
        assert get_or_compute("k", db, "BTC", compute) == 2

    def test_result_survives_restart_via_db(self, db):
        # 서버가 잠들었다 깨어나 메모리가 비어도 DB 사본으로 재계산 없이 응답한다
        _add_day(db, date(2026, 11, 1))
        calls = []

        def compute():
            calls.append(1)
            return {"daily": [1, 2, 3]}

        get_or_compute("k", db, "BTC", compute)
        precompute.clear()  # 메모리만 비움 (재기동 흉내)
        assert get_or_compute("k", db, "BTC", compute) == {"daily": [1, 2, 3]}
        assert len(calls) == 1

    def test_concurrent_requests_compute_once(self, monkeypatch):
        # 동시 요청이 같은 무거운 계산을 중복 실행하지 않는지 본다 (DB 조회는 고정값으로 대체)
        monkeypatch.setattr(precompute, "latest_date", lambda _db, _coin: date(2026, 11, 1))
        calls = []

        def slow():
            calls.append(1)
            time.sleep(0.3)
            return "done"

        results = []
        threads = [
            threading.Thread(target=lambda: results.append(get_or_compute("k", None, "BTC", slow)))
            for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results == ["done"] * 4
        assert len(calls) == 1
