"""무거운 롤링 적합 결과를 미리 계산해 두는 저장소.

compare · accuracy · leaderboard는 날짜마다 GARCH 계열을 다시 적합해 첫 계산에
무료 플랜 CPU로 1~3분(Render에서 15개 조합 전체는 20분 이상)이 걸린다.
결과는 DB의 일별 데이터가 바뀔 때만 달라지므로 5분 TTL 대신
"코인별 마지막 날짜"를 버전으로 삼아 저장한다.

- 무료 플랜은 잠들면 메모리가 비므로 결과를 DB(precomputed_result)에도 남긴다.
- 새 날짜 데이터가 들어와 버전이 바뀌어도 사용자를 기다리게 하지 않는다.
  직전 결과를 바로 돌려주고, 새 계산은 백그라운드 워커 1개에서 순서대로 한다
  (stale-while-revalidate). 아무 결과도 없을 때(최초 배포 직후)만 계산을 기다린다.
- 같은 키는 키별 락으로 한 번만 계산한다.
"""
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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

# compute는 세션을 받아 결과를 돌려준다. 백그라운드 갱신은 요청 세션이 닫힌 뒤에도
# 돌아야 하므로 요청 세션을 붙잡지 않고 자기 세션을 연다.
Compute = Callable[[Session], object]

_store: dict[str, tuple[date | None, object]] = {}
_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()
_refreshing: set[str] = set()
# 워커를 1개로 두어 갱신 계산끼리 작은 CPU를 나눠 쓰지 않게 한다
_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="precompute")


def _lock_for(key: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(key, threading.Lock())


def latest_date(db: Session, coin: str) -> date | None:
    return db.query(func.max(CoinDaily.date)).filter(CoinDaily.symbol == coin).scalar()


def _load_persisted(key: str):
    """DB에 남은 결과를 버전과 함께 돌려준다 (버전이 오래돼도 돌려준다)."""
    from app.database import SessionLocal

    s = SessionLocal()
    try:
        row = s.get(PrecomputedResult, key)
        if row is not None:
            return row.version, json.loads(row.payload)
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


def refresh(key: str, coin: str, compute: Compute):
    """현재 데이터 버전의 결과를 보장한다. 필요하면 계산하고 메모리 · DB에 저장한다."""
    from app.database import SessionLocal

    s = SessionLocal()
    try:
        version = latest_date(s, coin)
        with _lock_for(key):
            hit = _store.get(key)
            if hit is not None and hit[0] == version:
                return hit[1]
            persisted = _load_persisted(key)
            if persisted is not None and persisted[0] == version:
                _store[key] = persisted
                return persisted[1]
            result = compute(s)
            _persist(key, version, result)
            _store[key] = (version, result)
            return result
    finally:
        s.close()


def _schedule_refresh(key: str, coin: str, compute: Compute) -> None:
    with _locks_guard:
        if key in _refreshing:
            return
        _refreshing.add(key)

    def run():
        try:
            refresh(key, coin, compute)
            logger.info(f"precompute refreshed {key}")
        except Exception as e:
            logger.error(f"precompute refresh failed for {key}: {e}")
        finally:
            with _locks_guard:
                _refreshing.discard(key)

    _executor.submit(run)


def get_or_compute(key: str, db: Session, coin: str, compute: Compute):
    """요청 경로용. 결과가 하나라도 있으면 기다리지 않는다.

    - 현재 버전 결과가 있으면 그대로 돌려준다.
    - 이전 버전 결과만 있으면 그것을 돌려주고 갱신은 백그라운드에 맡긴다.
    - 아무 결과도 없으면 계산을 기다린다 (최초 배포 직후에만 생긴다).
    """
    version = latest_date(db, coin)
    hit = _store.get(key)
    if hit is None:
        persisted = _load_persisted(key)
        if persisted is not None:
            _store[key] = persisted
            hit = persisted

    if hit is not None:
        if hit[0] != version:
            _schedule_refresh(key, coin, compute)
        return hit[1]

    return refresh(key, coin, compute)


def clear():
    """메모리 사본만 비운다 (테스트 · 재기동 흉내용)."""
    _store.clear()


def wait_idle():
    """백그라운드 갱신이 모두 끝날 때까지 기다린다 (테스트용)."""
    _executor.submit(lambda: None).result()


def warm_all():
    """모든 코인 · 기간의 결과를 현재 데이터 버전으로 채운다 (백그라운드 스레드에서 호출)."""
    from app.routers.signal import compute_leaderboard
    from app.routers.volatility import compute_accuracy, compute_compare

    started = time.time()
    for coin in COINS:
        jobs: list[tuple[str, Compute]] = [
            (f"compare:{coin}:{d}", lambda s, c=coin, d=d: compute_compare(s, c, d))
            for d in COMPARE_PERIODS
        ]
        jobs.append((f"accuracy:{coin}:{ACCURACY_DAYS}",
                     lambda s, c=coin: compute_accuracy(s, c, ACCURACY_DAYS)))
        jobs.append((f"leaderboard:{coin}", lambda s, c=coin: compute_leaderboard(s, c)))
        for key, compute in jobs:
            try:
                refresh(key, coin, compute)
            except Exception as e:
                logger.error(f"{key} precompute error: {e}")
    logger.info(f"Precompute complete in {time.time() - started:.1f}s")
