#!/usr/bin/env python
# pylint fights with yapf over line breaks
# pylint: disable=bad-continuation
"""
ISTAC module holds some helpers to decode ISTAC API data
"""

import unittest
from typing import (cast, Iterable, Generator, AsyncGenerator, Mapping, Dict,
                    Sequence, Optional, Any, Callable)

import itertools
import aiohttp
import pandas as pd


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


# pylint: disable=too-few-public-methods
class MultiStr(dict):
    """Dictionary containing one str per language"""
    def __str__(self):
        """Return default representation"""
        return self.get('__default__', '')

    def __repr__(self):
        """Return complete object"""
        return f'MultiStr({repr(dict(self))})'


# pylint: disable=too-few-public-methods
class _Path:
    """_Path is a collection of nested attributes in a json object"""
    def __init__(self, cast_with: Callable[[Any], Any], *args: str):
        """Collect all arguments as a path"""
        self.cast = cast_with
        self.path = args

    def __call__(self, item: Any) -> Any:
        """Gets an attribute nested inside a json object, at the given path"""
        for step in self.path:
            item = item.get(step, None)
            if item is None:
                break
        return self.cast(item) if item is not None else None


# pylint: disable=too-few-public-methods
class _Schema(dict):
    """_Schema encapsulates a mapping from attribute names to _Path"""
    def __init__(self, items: Optional[Mapping[str, _Path]] = None):
        """Initialize schema from dict"""
        if items is not None:
            super().__init__(items)

    def apply(self, item: Any) -> Dict[str, Any]:
        """Apply this schema to an object, return dict"""
        return dict((key, path(item)) for key, path in self.items())


class Dimension:
    """Dimension class"""

    _SCHEMAS: Mapping[str, _Schema] = {
        'GEOGRAPHICAL':
        _Schema({
            'latitude': _Path(float, 'latitude'),
            'longitude': _Path(float, 'longitude'),
        }),
        'TIME':
        _Schema(),
        'MEASURE':
        _Schema({
            'baseValue':
            _Path(str, 'quantity', 'baseValue'),
            'decimalPlaces':
            _Path(int, 'quantity', 'decimalPlaces'),
            'isPercentage':
            _Path(str, 'quantity', 'isPercentage'),
            'max':
            _Path(float, 'quantity', 'max'),
            'min':
            _Path(float, 'quantity', 'min'),
            'percentageOf':
            _Path(MultiStr, 'quantity', 'percentageOf'),
            'significantDigits':
            _Path(int, 'quantity', 'significantDigits'),
            'type':
            _Path(str, 'quantity', 'type'),
            'unit':
            _Path(MultiStr, 'quantity', 'unit'),
            'unitMultiplier':
            _Path(MultiStr, 'quantity', 'unitMultiplier'),
            'unitSymbol':
            _Path(str, 'quantity', 'unitSymbol'),
            'unitSymbolPosition':
            _Path(str, 'quantity', 'unitSymbolPosition'),
        }),
    }

    @staticmethod
    def _granularity(
            data: Sequence[Mapping[str, Any]]) -> Mapping[str, MultiStr]:
        """Formats granularity as a mapping of codes to titles"""
        return dict(
            (str(item['code']), MultiStr(item['title'])) for item in data)

    def _schema(self) -> _Schema:
        """List of columns and corresponding paths within representation"""
        if self.code not in Dimension._SCHEMAS:
            raise ValueError(f'Undefined dimension code {self.code}')
        columns = _Schema({
            'code': _Path(str, 'code'),
            'title': _Path(MultiStr, 'title'),
        })
        if self.granularity is not None and len(self.granularity) > 0:
            columns['granularityCode'] = _Path(str, 'granularityCode')
        columns.update(Dimension._SCHEMAS[self.code])
        return columns

    # pylint: disable=invalid-name
    def __init__(self, body: Mapping[str, Any]):
        """Build dimension with body received from ISTAC"""
        self.code = str(body['code'])
        self.granularity = Dimension._granularity(
            body.get('granularity', tuple()))
        schema = self._schema()
        data = (schema.apply(rep)
                for rep in body.get('representation', tuple()))
        self.df = pd.DataFrame(data, columns=schema.keys()).set_index('code')


class Indicator:
    """
    Indicator class collects information published by ISTAC API
    for a particular indicator.
    """
    _SCHEMA = _Schema({
        'id':
        _Path(str, 'id'),
        'kind':
        _Path(str, 'kind'),
        'selfLink':
        _Path(str, 'selfLink'),
        'code':
        _Path(str, 'code'),
        'version':
        _Path(str, 'version'),
        'title':
        _Path(MultiStr, 'title'),
        'systemSurveyLinks':
        _Path(tuple, 'systemSurveyLinks'),
        'subjectCode':
        _Path(str, 'subjectCode'),
        'subjectTitle':
        _Path(MultiStr, 'subjectTitle'),
        'conceptDescription':
        _Path(MultiStr, 'conceptDescription'),
        'notes':
        _Path(MultiStr, 'notes'),
    })

    def __init__(self, istac_data: Mapping[str, Any]):
        """Build indicator from istac data item"""
        self.attribs = Indicator._SCHEMA.apply(istac_data)
        self.code = cast(str, self.attribs['code'])
        self.title = cast(MultiStr, self.attribs['title'])

    def __str__(self) -> str:
        """Returns attribute title"""
        return f'{self.code}: {self.title}'

    def __repr__(self) -> str:
        """Return detailed representation"""
        return f'Indicator({repr(self.attribs)})'

    # pylint: disable=invalid-name
    async def df(self,
                 session: aiohttp.ClientSession,
                 params: Optional[Mapping[str, str]] = None) -> pd.DataFrame:
        """Collect indicator data"""
        return await indicator_df(session, self.code, params)

    async def dims(self,
                   session: aiohttp.ClientSession) -> Mapping[str, Dimension]:
        """Collect indicator data"""
        return await dimensions(session, self.code)

    @staticmethod
    def join(df: pd.DataFrame,
             dims: Mapping[str, Dimension],
             dropna: Optional[bool] = False) -> pd.DataFrame:
        """Joins the data and dimensions into a single dataframe"""
        for dim_name, dim_data in dims.items():
            df = df.join(dim_data.df, on=dim_name, rsuffix=f'_{dim_name}')
        # Set the index of the df to be the combination of dimensions
        df = df.set_index(list(dims.keys()))
        if not dropna:
            return df
        return df.dropna(axis=1, how='all')


async def indicators(
    session: aiohttp.ClientSession,
    params: Optional[Mapping[str, str]] = None
) -> AsyncGenerator[Indicator, None]:
    """
    Enumerate indicators available from ISTAC API data. See:
    https://www3.gobiernodecanarias.org/istac/api/indicators/v1.0
    """
    next_url = f'https://www3.gobiernodecanarias.org/istac/api/indicators/v1.0/indicators'
    while next_url != '':
        data = await _fetch(session, next_url, params)
        for item in data['items']:
            yield Indicator(item)
        next_url = data.get('nextLink', '')
        self_url = data.get('selfLink', '')
        if next_url == self_url:
            next_url = ''
        # Params for following pages are in the url
        params = None


async def indicator_df(
        session: aiohttp.ClientSession,
        code: str,
        params: Optional[Mapping[str, str]] = None) -> pd.DataFrame:
    """
    Build dataframe from ISTAC API data. See:
    https://www3.gobiernodecanarias.org/istac/api/indicators/v1.0

    For example: request monthly data for 2019, absolute figures
    for indicator 'TURISTAS':

    data = await indicator_data(
        session, 'TURISTAS', {
            'granularity': 'TIME[MONTHLY]',
            'representation': 'MEASURE[ABSOLUTE],TIME[2019]',
            'fields': '-observationsMetadata',
        })
    """
    url = f'https://www3.gobiernodecanarias.org/istac/api/indicators/v1.0/indicators/{code}/data'
    return _parse_data(await _fetch(session, url, params))


async def dimensions(session: aiohttp.ClientSession,
                     code: str) -> Mapping[str, Dimension]:
    """
    Build dataframe from ISTAC API dimension data. See:
    https://www3.gobiernodecanarias.org/istac/api/indicators/v1.0

    For example: request dimensional data for indicator 'TURISTAS':
    data = await dimension_data(session, 'TURISTAS')
    """
    url = f'https://www3.gobiernodecanarias.org/istac/api/indicators/v1.0/indicators/{code}'
    body = await _fetch(session, url)
    return dict(
        (code, Dimension(data)) for code, data in body['dimension'].items())


async def _fetch(
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[Mapping[str, str]] = None) -> Mapping[str, Any]:
    """Fetch json body from URL. Raises FetchError on error"""
    headers = {'Accept': 'application/json'}
    async with session.get(url, headers=headers, params=params) as response:
        if response.status != 200:
            raise FetchError(url, headers, response)
        return await response.json()


def _parse_data(body: Mapping[str, Any]) -> pd.DataFrame:
    """
    Turns the data from ISTAC API into a sequence of objects.

    Data comes in the form:
    {
        "format": [ "DIM1", "DIM2", ... ]
        "dimension": {
            "DIM1": {
                "representation": {
                    "size": N,
                    "index": {
                        "VAL1": 0,
                        "VAL2": 1,
                        ...
                        "VALN": N-1
                    }
                }
            }, // as many DIMs as needed
        },
        "observation": [ array of observations ],
        "attribute": [array of attributes, optional ]
    }

    From swagger documentation:
        > observation (Array[string], optional): Array de observaciones. Las
        > observaciones se encuentran ordenadas por la combinación de las
        > categorías manteniendo fijada siempre la primera categoría de la
        > primera dimensión e iterando sobre las categorías de la última
        > dimensión del array. Por ejemplo, dadas las dimensiones A, B y C
        > con 3, 2, y 4 categorías respectivamente, los valores estarán
        > ordenados de tal manera que primero se itere sobre las 4 categorías
        > de C, posteriormente sobre las dos de B y por último sobre las 3 de
        > A. En dicho ejemplo, el resultado sería el siguiente:
        > A1B1C1, A1B1C2, A1B1C3, A1B1C4, A1B2C1, A1B2C2, A1B2C3, A1B2C4,
        > A2B1C1, A2B1C2, A2B1C3, A1B1C4, A2B2C1, A2B2C2, A2B2C3, A2B2C4,
        > A3B1C1, A3B1C2, A3B1C3, A3B1C4, A3B2C1, A3B2C2, A3B2C3, A3B2C4

    This data is inserted in a DataFrame with columns
    [ '_meta', '_attr', 'F' ], and then one column per dim.
    """
    dnames = tuple(body['format'])

    def values():
        # Build inverse index from representations
        repres = tuple(body['dimension'][name]['representation']
                       for name in dnames)
        revidx = tuple(tuple(_reverse(rep['index'])) for rep in repres)
        # Build dimension counter from sizes
        dsizes = (rep['size'] for rep in repres)
        dcount = _count(dsizes)
        # Get observations and attributes
        observ = body['observation']
        attrib = body.get('attribute', itertools.repeat(None))
        offset = 0
        for count, obs, attr in zip(dcount, observ, attrib):
            current = {
                '_offset': offset,
                '_meta': attr,
                'F': obs,
            }
            current.update((dnames[idx], revidx[idx][pos])
                           for idx, pos in enumerate(count))
            offset += 1
            yield current

    # Make sure the dataframe has the proper format, even
    # when data is empty.
    columns = ['_offset', '_meta', 'F']
    columns.extend(dnames)
    return pd.DataFrame(values(), columns=columns).set_index('_offset')


def _reverse(indexes: Mapping[str, int]) -> Sequence[str]:
    """Reverses an index of strings to offsets

    I.e. given a Mapping such as:
    { "KEY1": 0, "KEY2": 1, "KEY3": 2, ... }

    Returns a list of keys in index order:
    [ "KEY1", "KEY2", "KEY3", ... ]
    """
    limit = len(indexes)
    sorts = [""] * limit
    for key, idx in indexes.items():
        sorts[idx] = key
    return sorts


def _count(sizes: Iterable[int]) -> Generator[Iterable[int], None, None]:
    """
    Count is a generator that holds a set of indexes that
    rotate withing a given set of sizes.

    E.g. Say that we buid a counter with sizes (2, 3, 2).
    The iteration would yield the following triplets:

    [0..2] [0..3] [0..2]
    0,     0,      0
    0,     0,      1
    0,     1,      0
    0,     1,      1
    0,     2,      0
    0,     2,      1
    1,     0,      0
    1,     0,      1
    1,     1,      0
    1,     1,      1
    1,     2,      0
    1,     2,      1
    """
    sizes = tuple(sizes)
    total = 1
    for size in sizes:
        total *= size
    if total <= 0:
        return
    shift = [0] * len(sizes)
    while total > 0:
        yield shift
        for index in range(len(sizes) - 1, -1, -1):
            current = shift[index] + 1
            if current < sizes[index]:
                shift[index] = current
                break
            shift[index] = 0
        total -= 1


class CountTest(unittest.TestCase):
    """Unittesting count"""
    def test_empty(self):
        """Empty sizes should result in no iteration"""
        for _ in _count([2, 5, 0]):
            self.fail('Should not iterate when some size is 0')

    def test_regular(self):
        """Regular iteration should work"""
        result = tuple(tuple(x) for x in _count([2, 3, 2]))
        expect = (
            (0, 0, 0),
            (0, 0, 1),
            (0, 1, 0),
            (0, 1, 1),
            (0, 2, 0),
            (0, 2, 1),
            (1, 0, 0),
            (1, 0, 1),
            (1, 1, 0),
            (1, 1, 1),
            (1, 2, 0),
            (1, 2, 1),
        )
        self.assertEqual(result, expect)


class ReverseTest(unittest.TestCase):
    """Unittesting reverse"""
    def test_regular(self):
        """Regular input should be reversed"""
        result = _reverse({
            'val3': 3,
            'val0': 0,
            'val1': 1,
            'val2': 2,
        })
        self.assertEqual(tuple(result), ('val0', 'val1', 'val2', 'val3'))


class ParseDataTest(unittest.TestCase):
    """Unittesting parse"""

    # pylint: disable=no-self-use
    def test_empty(self):
        """Empty DataFrame should have proper columns"""
        actual = _parse_data({
            'format': ['DIM1', 'DIM2', 'DIM3'],
            'dimension': {
                'DIM1': {
                    'representation': {
                        'size': 0,
                        'index': {}
                    }
                },
                'DIM2': {
                    'representation': {
                        'size': 0,
                        'index': {}
                    }
                },
                'DIM3': {
                    'representation': {
                        'size': 0,
                        'index': {}
                    }
                },
            },
            'observation': [],
        })
        columns = ['_offset', '_meta', 'F', 'DIM1', 'DIM2', 'DIM3']
        expected = pd.DataFrame(tuple(), columns=columns).set_index('_offset')
        pd.testing.assert_frame_equal(actual, expected)

    # pylint: disable=no-self-use
    def test_regular(self):
        """Regular input should be packaged"""
        actual = _parse_data({
            'format': ['DIM1', 'DIM2', 'DIM3'],
            'dimension': {
                'DIM1': {
                    'representation': {
                        'size': 2,
                        'index': {
                            'D1V1': 0,
                            'D1V2': 1
                        },
                    },
                },
                'DIM2': {
                    'representation': {
                        'size': 3,
                        'index': {
                            'D2V1': 0,
                            'D2V2': 1,
                            'D2V3': 2
                        },
                    },
                },
                'DIM3': {
                    'representation': {
                        'size': 2,
                        'index': {
                            'D3V1': 0,
                            'D3V2': 1
                        },
                    },
                },
            },
            'observation': [
                'D1V1_D2V1_D3V1',
                'D1V1_D2V1_D3V2',
                'D1V1_D2V2_D3V1',
                'D1V1_D2V2_D3V2',
                'D1V1_D2V3_D3V1',
                'D1V1_D2V3_D3V2',
                'D1V2_D2V1_D3V1',
                'D1V2_D2V1_D3V2',
                'D1V2_D2V2_D3V1',
                'D1V2_D2V2_D3V2',
                'D1V2_D2V3_D3V1',
                'D1V2_D2V3_D3V2',
            ],
            'attribute': [
                'ATT_D1V1_D2V1_D3V1',
                'ATT_D1V1_D2V1_D3V2',
                'ATT_D1V1_D2V2_D3V1',
                'ATT_D1V1_D2V2_D3V2',
                'ATT_D1V1_D2V3_D3V1',
                'ATT_D1V1_D2V3_D3V2',
                'ATT_D1V2_D2V1_D3V1',
                'ATT_D1V2_D2V1_D3V2',
                'ATT_D1V2_D2V2_D3V1',
                'ATT_D1V2_D2V2_D3V2',
                'ATT_D1V2_D2V3_D3V1',
                'ATT_D1V2_D2V3_D3V2',
            ],
        })
        columns = ['_offset', '_meta', 'F', 'DIM1', 'DIM2', 'DIM3']
        expected = pd.DataFrame((
            {
                'DIM1': 'D1V1',
                'DIM2': 'D2V1',
                'DIM3': 'D3V1',
                '_offset': 0,
                'F': 'D1V1_D2V1_D3V1',
                '_meta': 'ATT_D1V1_D2V1_D3V1'
            },
            {
                'DIM1': 'D1V1',
                'DIM2': 'D2V1',
                'DIM3': 'D3V2',
                '_offset': 1,
                'F': 'D1V1_D2V1_D3V2',
                '_meta': 'ATT_D1V1_D2V1_D3V2'
            },
            {
                'DIM1': 'D1V1',
                'DIM2': 'D2V2',
                'DIM3': 'D3V1',
                '_offset': 2,
                'F': 'D1V1_D2V2_D3V1',
                '_meta': 'ATT_D1V1_D2V2_D3V1'
            },
            {
                'DIM1': 'D1V1',
                'DIM2': 'D2V2',
                'DIM3': 'D3V2',
                '_offset': 3,
                'F': 'D1V1_D2V2_D3V2',
                '_meta': 'ATT_D1V1_D2V2_D3V2'
            },
            {
                'DIM1': 'D1V1',
                'DIM2': 'D2V3',
                'DIM3': 'D3V1',
                '_offset': 4,
                'F': 'D1V1_D2V3_D3V1',
                '_meta': 'ATT_D1V1_D2V3_D3V1'
            },
            {
                'DIM1': 'D1V1',
                'DIM2': 'D2V3',
                'DIM3': 'D3V2',
                '_offset': 5,
                'F': 'D1V1_D2V3_D3V2',
                '_meta': 'ATT_D1V1_D2V3_D3V2'
            },
            {
                'DIM1': 'D1V2',
                'DIM2': 'D2V1',
                'DIM3': 'D3V1',
                '_offset': 6,
                'F': 'D1V2_D2V1_D3V1',
                '_meta': 'ATT_D1V2_D2V1_D3V1'
            },
            {
                'DIM1': 'D1V2',
                'DIM2': 'D2V1',
                'DIM3': 'D3V2',
                '_offset': 7,
                'F': 'D1V2_D2V1_D3V2',
                '_meta': 'ATT_D1V2_D2V1_D3V2'
            },
            {
                'DIM1': 'D1V2',
                'DIM2': 'D2V2',
                'DIM3': 'D3V1',
                '_offset': 8,
                'F': 'D1V2_D2V2_D3V1',
                '_meta': 'ATT_D1V2_D2V2_D3V1'
            },
            {
                'DIM1': 'D1V2',
                'DIM2': 'D2V2',
                'DIM3': 'D3V2',
                '_offset': 9,
                'F': 'D1V2_D2V2_D3V2',
                '_meta': 'ATT_D1V2_D2V2_D3V2'
            },
            {
                'DIM1': 'D1V2',
                'DIM2': 'D2V3',
                'DIM3': 'D3V1',
                '_offset': 10,
                'F': 'D1V2_D2V3_D3V1',
                '_meta': 'ATT_D1V2_D2V3_D3V1'
            },
            {
                'DIM1': 'D1V2',
                'DIM2': 'D2V3',
                'DIM3': 'D3V2',
                '_offset': 11,
                'F': 'D1V2_D2V3_D3V2',
                '_meta': 'ATT_D1V2_D2V3_D3V2'
            },
        ),
                                columns=columns).set_index('_offset')
        pd.testing.assert_frame_equal(actual, expected)


if __name__ == "__main__":
    unittest.main()
