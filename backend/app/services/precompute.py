"""무거운 롤링 적합 결과를 미리 계산해 두는 저장소.

compare · accuracy · leaderboard는 날짜마다 GARCH 계열을 다시 적합해 첫 계산에
무료 플랜 CPU로 1~3분이 걸린다. 결과는 DB의 일별 데이터가 바뀔 때만 달라지므로
5분 TTL 대신 "코인별 마지막 날짜"를 버전으로 삼아 저장하고,
서버 기동 직후와 일일 수집 직후에 백그라운드에서 미리 채운다.
무료 플랜은 잠들면 메모리가 비므로 결과를 DB(precomputed_result)에도 남긴다.

같은 키를 동시에 요청하면 한 번만 계산하도록 키별 락을 둔다.
첫 화면이 계산 3종을 동시에 부르면서 CPU를 나눠 쓰다 시간 초과가 나던 문제를 막는다.
"""
import json
import logging
import threading
import time
from datetime import date
from typing import Callable

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.precomputed import PrecomputedResult
from app.models.price import CoinDaily

logger = logging.getLogger(__name__)

COINS = ["BTC", "ETH", "SOL"]
COMPARE_PERIODS = [30, 60, 90]  # 프론트 VolatilityChart의 기간 버튼
ACCURACY_DAYS = 60  # 프론트 AccuracyTracker 기본값

_store: dict[str, tuple[date | None, object]] = {}
_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(key: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(key, threading.Lock())


def latest_date(db: Session, coin: str) -> date | None:
    return db.query(func.max(CoinDaily.date)).filter(CoinDaily.symbol == coin).scalar()


def _load_persisted(key: str, version: date | None):
    from app.database import SessionLocal

    s = SessionLocal()
    try:
        row = s.get(PrecomputedResult, key)
        if row is not None and row.version == version:
            return json.loads(row.payload)
    except Exception as e:
        logger.warning(f"precompute load failed for {key}: {e}")
    finally:
        s.close()
    return None


def _persist(key: str, version: date | None, result) -> None:
    from app.database import SessionLocal

    s = SessionLocal()
    try:
        s.merge(PrecomputedResult(key=key, version=version, payload=json.dumps(result)))
        s.commit()
    except Exception as e:
        # 저장 실패는 응답을 막지 않는다 — 메모리 사본으로 계속 서빙
        s.rollback()
        logger.warning(f"precompute persist failed for {key}: {e}")
    finally:
        s.close()


def get_or_compute(key: str, db: Session, coin: str, compute: Callable[[], object]):
    """데이터 버전(마지막 날짜)이 같으면 저장된 결과를, 다르면 새로 계산해 돌려준다.

    조회 순서: 메모리 → DB(재기동 후에도 남아 있음) → 계산.
    """
    version = latest_date(db, coin)
    hit = _store.get(key)
    if hit is not None and hit[0] == version:
        return hit[1]

    with _lock_for(key):
        # 락을 기다리는 동안 다른 요청이 이미 계산했을 수 있다
        hit = _store.get(key)
        if hit is not None and hit[0] == version:
            return hit[1]
        result = _load_persisted(key, version)
        if result is None:
            result = compute()
            _persist(key, version, result)
        _store[key] = (version, result)
        return result


def clear():
    _store.clear()


def warm_all():
    """모든 코인 · 기간의 무거운 결과를 순서대로 채운다 (백그라운드 스레드에서 호출)."""
    from app.database import SessionLocal
    from app.routers.signal import compute_leaderboard
    from app.routers.volatility import compute_accuracy, compute_compare

    started = time.time()
    for coin in COINS:
        db = SessionLocal()
        try:
            for days in COMPARE_PERIODS:
                get_or_compute(f"compare:{coin}:{days}", db, coin,
                               lambda: compute_compare(db, coin, days))
            get_or_compute(f"accuracy:{coin}:{ACCURACY_DAYS}", db, coin,
                           lambda: compute_accuracy(db, coin, ACCURACY_DAYS))
            get_or_compute(f"leaderboard:{coin}", db, coin,
                           lambda: compute_leaderboard(db, coin))
        except Exception as e:
            logger.error(f"{coin} precompute error: {e}")
        finally:
            db.close()
    logger.info(f"Precompute complete in {time.time() - started:.1f}s")
