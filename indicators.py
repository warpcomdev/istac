#!/usr/bin/env python
"""Módulo importación indicadores"""

import asyncio
import aiohttp
import istac


async def main():
    """Sample main function"""
    async with aiohttp.ClientSession() as session:
        data = await istac.indicator_data(
            session, 'TURISTAS', {
                'granularity': 'TIME[MONTHLY]',
                'representation': 'MEASURE[ABSOLUTE]',
                'fields': '-observationsMetadata',
            })
        print(data.head(10))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
