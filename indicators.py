#!/usr/bin/env python
"""Módulo importación indicadores"""

import asyncio
import aiohttp
import istac


async def main():
    """Sample main function"""
    async with aiohttp.ClientSession() as session:
        #async for indicator in istac.indicators(session):
        #    print(indicator)
        #data = await istac.indicator_data(
        #    session, 'TURISTAS', {
        #        'granularity': 'TIME[MONTHLY]',
        #        'representation': 'MEASURE[ABSOLUTE]',
        #        'fields': '-observationsMetadata',
        #    })
        #print(data.head(10))
        dims = await istac.dimension_data(session, 'TURISTAS')
        for code, dim in dims.items():
            print(f'{code}:')
            print(dim.points.head(10))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
