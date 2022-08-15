import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import (Column, String, Integer, DateTime, func)
from sqlalchemy.orm import declarative_base, sessionmaker
from data_retrieve import get_hero_data
from pg_db_settings import PG_USER, PG_PASSWORD, DB_NAME, DB_HOST

PG_ASYNC_CONN_URI = f'postgresql+asyncpg://{PG_USER}:{PG_PASSWORD}@{DB_HOST}/{DB_NAME}'

engine = create_async_engine(PG_ASYNC_CONN_URI, echo=False)

Base = declarative_base()

Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def create_db_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


async def save_hero_in_db(hero_data):
    async with Session() as session:
        async with session.begin():
            for hero in hero_data[0]:
                name = hero['name']
                birth_year = hero['birth_year']
                eye_color = hero['eye_color']
                films = ','.join(hero['films'])
                gender = hero['gender']
                hair_color = hero['hair_color']
                height = hero['height']
                homeworld = hero['homeworld']
                mass = hero['mass']
                skin_color = hero['skin_color']
                species = ','.join(hero['species'])
                starships = ','.join(hero['starships'])
                vehicles = ','.join(hero['vehicles'])
                hero = Hero(name=name, birth_year=birth_year, eye_color=eye_color, films=films,
                            gender=gender, hair_color=hair_color, height=height, homeworld=homeworld,
                            mass=mass, skin_color=skin_color, species=species, starships=starships,
                            vehicles=vehicles)
                session.add(hero)


class Hero(Base):
    __tablename__ = 'sw_heroes'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, index=True)
    birth_year = Column(String, nullable=False)
    eye_color = Column(String, nullable=False)
    films = Column(String, nullable=False)
    gender = Column(String, nullable=False)
    hair_color = Column(String, nullable=False)
    height = Column(String, nullable=False)
    homeworld = Column(String, nullable=False)
    mass = Column(String, nullable=False)
    skin_color = Column(String, nullable=False)
    species = Column(String, nullable=False)
    starships = Column(String, nullable=False)
    vehicles = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


async def async_main():
    await create_db_tables()
    hero_data = await asyncio.gather(get_hero_data())
    await save_hero_in_db(hero_data)


def main():
    asyncio.get_event_loop().run_until_complete(async_main())


if __name__ == '__main__':
    main()


