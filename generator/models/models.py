from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class AdEvent(Base):
    __tablename__ = "ad_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ad_id = Column(Integer, nullable=False)
    ad_event = Column(String, nullable=False)
    user_id = Column(Integer, nullable=False)

    @classmethod
    def from_event(cls, event: tuple) -> "AdEvent":
        return cls(
            ad_id=event[0],
            ad_event=event[1],
            user_id=event[2],
        )