import asyncio
import aiohttp
from more_itertools import chunked

MAX_CHUNK = 30


async def get_ppl(session, ppl_id):
    async with session.get(f'https://swapi.dev/api/people/{ppl_id}') as response:
        json_data = await response.json()
        return json_data


async def get_hero_data():
    heroes_list = []
    async with aiohttp.ClientSession() as session:
        coroutines = (get_ppl(session, i) for i in range(1, 120))
        for coroutines_chunk in chunked(coroutines, MAX_CHUNK):
            result = await asyncio.gather(*coroutines_chunk)
            for hero_data in result:
                if 'name' in hero_data:
                    heroes_list.append(hero_data)
        return heroes_list

