from sqlalchemy import Column, Date, DateTime, String, Text, func

from app.database import Base


class PrecomputedResult(Base):
    """무거운 롤링 적합 결과의 영속 사본.

    무료 플랜은 15분 무접속이면 잠들어 메모리가 비므로,
    깨어난 직후 첫 요청이 다시 몇 분씩 계산하지 않도록 DB에 둔다.
    version은 해당 코인의 마지막 데이터 날짜다.
    """

    __tablename__ = "precomputed_result"

    key = Column(String(64), primary_key=True)
    version = Column(Date, nullable=True)
    payload = Column(Text, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
