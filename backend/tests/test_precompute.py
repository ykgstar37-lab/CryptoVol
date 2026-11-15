import threading
import time
from datetime import date

from app.models.price import CoinDaily
from app.services import precompute
from app.services.precompute import get_or_compute


def _add_day(db, d, close=100.0):
    db.add(CoinDaily(symbol="BTC", date=d, close=close, volume=1.0, fng=50))
    db.commit()


def _counter():
    calls = []

    def compute(_session):
        calls.append(1)
        return len(calls)

    return calls, compute


class TestPrecompute:
    def test_same_data_version_reuses_result(self, db):
        _add_day(db, date(2026, 11, 1))
        calls, compute = _counter()

        assert get_or_compute("k", db, "BTC", compute) == 1
        assert get_or_compute("k", db, "BTC", compute) == 1
        assert len(calls) == 1

    def test_new_day_serves_previous_result_then_refreshes(self, db):
        # 새 날짜가 들어와도 사용자는 기다리지 않고 직전 결과를 받는다
        _add_day(db, date(2026, 11, 1))
        calls, compute = _counter()
        get_or_compute("k", db, "BTC", compute)

        _add_day(db, date(2026, 11, 2))
        assert get_or_compute("k", db, "BTC", compute) == 1  # 직전 결과 즉시 반환

        precompute.wait_idle()  # 백그라운드 갱신 완료
        assert get_or_compute("k", db, "BTC", compute) == 2
        assert len(calls) == 2

    def test_result_survives_restart_via_db(self, db):
        # 서버가 잠들었다 깨어나 메모리가 비어도 DB 사본으로 재계산 없이 응답한다
        _add_day(db, date(2026, 11, 1))
        calls = []

        def compute(_session):
            calls.append(1)
            return {"daily": [1, 2, 3]}

        get_or_compute("k", db, "BTC", compute)
        precompute.clear()  # 메모리만 비움 (재기동 흉내)
        assert get_or_compute("k", db, "BTC", compute) == {"daily": [1, 2, 3]}
        assert len(calls) == 1

    def test_restart_on_new_day_serves_persisted_previous_result(self, db):
        # 잠든 사이 날짜가 바뀌어도 DB의 직전 결과를 바로 준다
        _add_day(db, date(2026, 11, 1))
        calls, compute = _counter()
        get_or_compute("k", db, "BTC", compute)
        precompute.clear()
        _add_day(db, date(2026, 11, 2))

        assert get_or_compute("k", db, "BTC", compute) == 1
        precompute.wait_idle()
        assert get_or_compute("k", db, "BTC", compute) == 2

    def test_first_ever_requests_compute_once(self, monkeypatch):
        # 결과가 하나도 없을 때 동시 요청 4개가 와도 계산은 한 번만 돈다
        monkeypatch.setattr(precompute, "latest_date", lambda _db, _coin: date(2026, 11, 1))
        calls = []

        def slow(_session):
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
