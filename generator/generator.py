import asyncio
import random

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from generator.consts import DATABASE_URL
from generator.models.models import Base, AdEvent
from generator.registry.ad_type_registry import AdTypeRegistry
from generator.registry.ad_types_enum import AdTypesEnum


def generate_event() -> "AdEvent":
    ad_types = AdTypesEnum.values() # ['AD_WAS_SHOWN', 'AD_WAS_CLICKED']
    rand_index = random.randint(0, 4)
    # В 1/5 случаев будет клик
    rand_index = 1 if rand_index == 0 else 0

    rand_ad_type = ad_types[rand_index]
    generator_cls = AdTypeRegistry.get_generator_by_name(generator_name=rand_ad_type)

    event = generator_cls().execute()
    return AdEvent.from_event(event)


async def main():
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        while True:
            event = generate_event()
            if event.ad_event == AdTypesEnum.AD_WAS_CLICKED.name:
                # Если был клик, значит был и показ
                view_event = AdEvent.from_event((event.ad_id, AdTypesEnum.AD_WAS_SHOWN.name, event.user_id))
                session.add(view_event)
            session.add(event)
            await session.commit()
            print(f"Inserted event: {event.ad_event} for ad_id={event.ad_id}")
            await asyncio.sleep(0.05)


if __name__ == "__main__":
    asyncio.run(main())
