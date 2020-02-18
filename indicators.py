#!/usr/bin/env python
"""Módulo importación indicadores"""

import asyncio
import aiohttp
import istac


async def main():
    """Sample main function"""
    async with aiohttp.ClientSession() as session:

        # Get indicators
        async for indicator in istac.indicators(session):
            print(f'Indicator {indicator.code} ({indicator.title})')

        # Get indicator data
        code = 'TURISTAS'
        data = await istac.indicator_df(
            session, code, {
                'granularity': 'TIME[MONTHLY]',
                'representation': 'MEASURE[ABSOLUTE]',
                'fields': '-observationsMetadata',
            })
        print(f'Indicator {code}')
        print(data.head(10))

        # Get dimensions
        dims = await istac.dimensions(session, code)
        for dim_name, dim in dims.items():
            print(f'Dimension {dim_name} (code {dim.code})')
            print(dim.df.head(5))

        # Join df with dimensions
        join = istac.Indicator.join(data, dims, dropna=True)
        print(f'Joined indicator {code} with its dims')
        print(join.head(10))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
