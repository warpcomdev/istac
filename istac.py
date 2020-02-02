#!/usr/bin/env python
"""
ISTAC module holds some helpers to decode ISTAC API data
"""

import unittest

from typing import Iterable, Generator, Mapping, Sequence, Any


def parse(body: Mapping[str, Any]) -> Generator[Mapping[str, Any], None, None]:
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
        "attribute": [array of attributes ]
    }

    And is yielded in the form:
    [
        {
            "_offset": offset,
            "_meta": attribute,
            "F": observation,
            "DIM1": "VAL1", ... one per dimension
        }, ... one per observation
    ]
    """
    dim_names = body['format']
    observation = body['observation']
    attribute = body['attribute']
    dimension = tuple(body['dimension'][name]['representation']
                      for name in dim_names)
    represent = tuple(tuple(_reverse(dim['index'])) for dim in dimension)
    offset = 0
    for position in _count(dim['size'] for dim in dimension):
        current = {
            '_offset': offset,
            '_meta': attribute[offset],
            'F': observation[offset],
        }
        current.update((dim_names[idx], represent[idx][pos])
                       for idx, pos in enumerate(position))
        offset += 1
        yield current


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
        self.assertEqual(result, ('val0', 'val1', 'val2', 'val3'))


class ParseTest(unittest.TestCase):
    """Unittesting parse"""
    def test_regular(self):
        """Regular input should be packaged"""
        data = {
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
        }
        expected = (
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
        )
        self.assertEqual(tuple(parse(data)), expected)


if __name__ == "__main__":
    unittest.main()
