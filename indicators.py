#!/usr/bin/env python
"""Módulo importación indicadores"""

import asyncio
from typing import Mapping, Any

import aiohttp
import pandas as pd
import istac


class FetchError(Exception):
    """Exception raised when Fetch fails"""

    # pylint: disable=super-init-not-called
    def __init__(self, url: str, headers: Mapping[str, str],
                 resp: aiohttp.ClientResponse):
        """Build error info from Fetch request"""
        self.url = url
        self.status = resp.status
        self.headers = headers

    def __str__(self) -> str:
        """Format exception"""
        return f'URL[{self.url}]: {self.status}'


async def fetch(session: aiohttp.ClientSession, url: str) -> Mapping[str, Any]:
    """Fetch json body from URL"""
    headers = {'Accept': 'application/json'}
    async with session.get(url, headers=headers) as response:
        if response.status != 200:
            raise FetchError(url, headers, response)
        return await response.json()


async def indicator_data(code: str) -> pd.DataFrame:
    """
    Build dataframe from ISTAC API data.
    See https://www3.gobiernodecanarias.org/istac/api/indicators/v1.0
    """
    url = f'https://www3.gobiernodecanarias.org/istac/api/indicators/v1.0/indicators/{code}/data'
    async with aiohttp.ClientSession() as session:
        indicator = pd.DataFrame(istac.parse(await fetch(session, url)))
        return indicator.set_index('_offset')


if __name__ == '__main__':
    LOOP = asyncio.get_event_loop()
    DATA = LOOP.run_until_complete(indicator_data('TURISTAS'))
    print(DATA.head(20))
