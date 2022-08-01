#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copyright 2022 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
PROJECT: Urbo2

This software and / or computer program has been developed by Telefónica Soluciones
de Informática y Comunicaciones de España, S.A.U (hereinafter TSOL) and is protected
as copyright by the applicable legislation on intellectual property.

It belongs to TSOL, and / or its licensors, the exclusive rights of reproduction,
distribution, public communication and transformation, and any economic right on it,
all without prejudice of the moral rights of the authors mentioned above. It is expressly
forbidden to decompile, disassemble, reverse engineer, sublicense or otherwise transmit
by any means, translate or create derivative works of the software and / or computer
programs, and perform with respect to all or part of such programs, any type of exploitation.

Any use of all or part of the software and / or computer program will require the
express written consent of TSOL. In all cases, it will be necessary to make
an express reference to TSOL ownership in the software and / or computer
program.

Non-fulfillment of the provisions set forth herein and, in general, any violation of
the peaceful possession and ownership of these rights will be prosecuted by the means
provided in both Spanish and international law. TSOL reserves any civil or
criminal actions it may exercise to protect its rights.
"""
#pylint: disable=invalid-name,missing-function-docstring,redefined-outer-name,line-too-long

import os
import configparser
import logging
import json
import argparse
import tempfile
import itertools
import time
import re
from io import TextIOWrapper

from typing import List, Dict, Any, Sequence, Protocol, Generator, Optional
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter, Retry

import psycopg2
import tc_etl_lib as tc
import pandas as pd
from pyaxis import pyaxis

requests.packages.urllib3.disable_warnings()


# Sets the logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="time=%(asctime)s | lvl=%(levelname)s | comp=ETL-ISTAC-CUBES | op=%(name)s:%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Gets this module's path
module_path = Path(__file__)

def read_config(config: configparser.ConfigParser, section: str, option: str, fallback: Optional[str] = None):
    """Read config value from environment or file"""
    env_name = f"ETL_ISTAC_{section.upper()}_{option.upper()}"
    result = os.getenv(env_name, config.get(section, option, fallback=fallback))
    if result is None:
        # Checks if the config has a environment section
        raise ValueError(f'Missing config {section}:{option} ({env_name})')
    return result

def read_config_int(config: configparser.ConfigParser, section: str, option: str, fallback: Optional[str] = None):
    """Read config value as integer, from environment or file"""
    return int(read_config(config, section, option, fallback))

def iter_chunk(iterable, chunk_size: int):
    """Chunks an iterable into smaller iterables"""
    # See https://stackoverflow.com/questions/8991506/iterate-an-iterator-by-chunks-of-n-in-python
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, chunk_size)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


class Store(Protocol):
    """Store represents any persistence backend"""
    def open(self):
        """Ready the store for saving data"""

    def send_batch(self, subservice: str, entities: Sequence[Any]) -> None:
        """Saves a batch of entities to the backend"""

    def get_entity(self, subservice: str, entityid: str, entitytype: str) -> Optional[Dict[str, Any]]:
        """Gets an entity by id and type"""

    def close(self):
        """Closes the backend connection"""


@dataclass
class OrionStore:
    """Orion-based store"""
    endpoint_keystone: str
    endpoint_cb: str
    user: str
    password: str
    service: str
    subservice: str
    timeout: int
    post_retry_connect: int
    post_retry_backoff_factor: int
    sleep_send_batch: int
    batch_size: int

    auth_manager: Optional[tc.authManager] = None
    cb_manager: Optional[tc.cbManager] = None


    def open(self):
        self.auth_manager = tc.authManager(
            endpoint=self.endpoint_keystone,
            service=self.service,
            user=self.user,
            password=self.password,
            subservice=self.subservice
        )
        self.cb_manager = tc.cbManager(
            endpoint=self.endpoint_cb,
            timeout=self.timeout,
            post_retry_connect=post_retry_connect,
            post_retry_backoff_factor=post_retry_backoff_factor,
            sleep_send_batch=sleep_send_batch
        )

    def send_batch(self, subservice: str, entities: Sequence[Any]):
        """Send a POST /v2/op/update batch"""
        if self.auth_manager is None or self.cb_manager is None:
            raise ValueError("store is not open yet")
        # Practical experience shows that many entities can stress
        # the context broker and make it fail. Better make sure chunks are
        # small enough, and separated by some seconds
        must_sleep = False
        for chunk in iter_chunk(entities, self.batch_size):
            if must_sleep:
                time.sleep(self.sleep_send_batch)
            else:
                must_sleep = True
            self.cb_manager.send_batch(subservice=subservice, auth=self.auth_manager, entities=chunk)

    def get_entity(self, subservice: str, entityid: str, entitytype: str) -> Optional[Dict[str, Any]]:
        try:
            matches = self.cb_manager.get_entities(subservice=subservice, auth=self.auth_manager, id=entityid, type=entitytype, limit=1)
        except tc.FetchError as err:
            if err.response.status_code == 404: # Not found
                return None
            else:
                raise
        if len(matches) < 1:
            return None
        return matches[0]

    def close(self):
        pass


@dataclass
class SQLFileStore:
    """SQL-File based store"""
    sql_file: Path
    service: str
    entity_table: Dict[str, str] # Mapping from entity type to table name
    handler: Optional[TextIOWrapper] = None

    @staticmethod
    def escape(obj: Any) -> str:
        """Escapes a value to be used in a SQL string"""
        # See https://github.com/psycopg/psycopg2/issues/331
        adaptor = psycopg2.extensions.adapt(obj)
        if hasattr(adaptor, 'encoding'):
            adaptor.encoding = 'utf-8'
        return adaptor.getquoted().decode('utf-8')

    @staticmethod
    def sql_values(subservice: str, entity: Dict[str, Any], fields: Sequence[str]) -> str:
        """Generates a string suitable for SQL insert, with all values of the entity"""
        sql = [
            SQLFileStore.escape(entity['id']),
            SQLFileStore.escape(entity['type']),
            SQLFileStore.escape(subservice),
            "NOW()"
        ]
        for field in fields:
            entry = entity.get(field, {'type':'Text', 'value': None})
            value: str
            if entry['value'] is None:
                value = "NULL"
            elif 'json' in entry['type']:
                value = SQLFileStore.escape(json.dumps(entry['value']))
                if 'geo' in entry['type']:
                    value = f"ST_GeomFromGeoJSON({value})"
            else:
                value = SQLFileStore.escape(entry['value'])
            sql.append(value)
        return f"({','.join(sql)})"

    @staticmethod
    def sql_insert(subservice: str, table_name: str, fields: Sequence[str], entities: Sequence[Any]) -> str:
        return "\n".join((
            f"INSERT INTO {table_name} (entityid,entitytype,fiwareservicepath,recvtime,{','.join(fields)}) VALUES",
            ",\n".join(
                SQLFileStore.sql_values(subservice, entity, fields)
                for entity in entities
            ),
            ";"
        ))

    def table_name(self, subservice: str, entity_type: str) -> str:
        # Table name is generated from service, subservice and entity type
        default_name = f"{subservice.strip('/')}_{entity_type.lower()}"
        mapped_name = self.entity_table.get(entity_type, default_name)
        if not mapped_name: # unmapped tables are not saved to SQL
            return ""
        return f"{service}.{mapped_name}"

    def open(self):
        self.handler = open(self.sql_file, "w+", encoding="utf-8")

    def send_batch(self, subservice: str, entities: Sequence[Any]) -> None:
        # truncate huge batches to avoid stressing psql client too
        for sub_batch in iter_chunk(entities, 10000):
            self.send_single_batch(subservice, sub_batch)

    def send_single_batch(self, subservice: str, entities: Sequence[Any]) -> None:

        # Group entities by type
        entities_by_type = defaultdict(list)
        for entity in entities:
            entities_by_type[entity['type']].append(entity)

        # For each type, get the fieldset
        fields_by_type = defaultdict(set)
        for entity_type, typed_entities in entities_by_type.items():
            for entity in typed_entities:
                for field in entity:
                    if field not in ('id', 'type'):
                        fields_by_type[entity_type].add(field)

        # Generate all rows for each type
        sql = []
        for entity_type, typed_entities in entities_by_type.items():
            table_name = self.table_name(subservice, entity_type)
            if table_name:
                table_cols = sorted(fields_by_type[entity_type])
                sql.append(SQLFileStore.sql_insert(subservice, table_name, table_cols, typed_entities))

        self.handler.write("\n".join(sql))
        self.handler.write("\n")

    def get_entity(self, subservice: str, entityid: str, entitytype: str) -> Optional[Dict[str, Any]]:
        # In SQL mode, always download full resources
        return None

    def close(self):
        self.handler.close()


def read_csv(csv_file: str) -> pd.DataFrame:
    """Read CSV file with list of download URLs"""
    data = pd.read_csv(csv_file, encoding="utf-8", sep=",", low_memory=False)
    return data[['id', 'download']]


@dataclass
class Bookmark:
    """Manages cube bookmarks"""

    # memorized values
    subservice: str
    entityid: str

    # Current bookmarks read from Context Broker
    yearly_year:   Optional[int]
    monthly_year:  Optional[int]
    monthly_month: Optional[int]

    # Keep a different set of values for max seen. Do not
    # assume the entries are sorted.
    yearly_year_max:   Optional[int] = None
    monthly_year_max:  Optional[int] = None
    monthly_month_max: Optional[int] = None

    def __init__(self, store: Store, subservice: str, entityid: str):
        """Read current year and month bookmarks"""
        self.subservice    = subservice
        self.entityid      = entityid
        self.yearly_year   = None
        self.monthly_year  = None
        self.monthly_month = None

        bookmark = store.get_entity(subservice=subservice, entityid=f"{entityid}", entitytype='IstacCubeBookmark')
        if bookmark is not None:
            def int_attrib(entity, attr):
                value = entity.get(attr, None)
                if value:
                    return int(value)
                return None
            # Take only data for years newer than the bookmark
            self.yearly_year   = int_attrib(bookmark, 'yearly_year')
            self.monthly_year  = int_attrib(bookmark, 'monthly_year')
            self.monthly_month = int_attrib(bookmark, 'monthly_month')

    def updates(self, entities: Sequence[Any]) -> Generator[Any, None, None]:
        """Filter entities, yields only newer ones"""
        for entity in entities:
            year  = entity['year']['value']
            month = entity.get('month', {}).get('value', None)
            if month:
                if (self.monthly_year is None) or (year > self.monthly_year) or (year == self.monthly_year and month > self.monthly_month):
                    yield entity
                    # Update bookmarks to save
                    if (self.monthly_year_max is None) or (year > self.monthly_year_max) or (year == self.monthly_year_max and month > self.monthly_month_max):
                        self.monthly_year_max = year
                        self.monthly_month_max = month
            else:
                if (self.yearly_year is None) or (year > self.yearly_year):
                    yield entity
                    # Update bookmarks to save
                    if (self.yearly_year_max is None) or (year > self.yearly_year_max):
                        self.yearly_year_max = year
        # Once all entities are processed, yield bookmark
        bookmark = self.bookmark()
        if bookmark is not None:
            yield bookmark

    def bookmark(self):
        """Save new bookmarks"""
        bookmark = {}
        if self.yearly_year_max is not None:
            bookmark['yearly_year'] = {
                'type': 'Number',
                'value': self.yearly_year_max
            }
        if self.monthly_year_max is not None:
            bookmark['monthly_year'] = {
                'type': 'Number',
                'value': self.monthly_year_max
            }
            bookmark['monthly_month'] = {
                'type': 'Number',
                'value': self.monthly_month_max
            }
        if len(bookmark) > 0:
            bookmark.update({
                'id': f"{self.entityid}",
                'type': 'IstacCubeBookmark',
            })
            return bookmark
        return None


def read_cube(session: requests.Session, store: Store, subservice: str, entityid: str, download: str):
    """Downloads a pc-axis cube and writes it to the store"""
    # pyaxis.parse needs either an URL, or a file.
    with tempfile.NamedTemporaryFile(mode='w+b', delete=True) as pxfile:
        pxfile.write(session.get(download).content)
        pxfile.flush() # in case the file is smaller than 4Kb, it might be buffered
        px = pyaxis_parse(pxfile.name, encoding="iso-8859-1")
    # Sort data columns alphabetically, so that it is always
    # printed in the same order
    meta = px['METADATA']
    rows = px['DATA']['ROWS']
    cols = sorted(px['DATA']['COLS'])

    # Meta-entity describes the cube
    meta_entity = {
        'id': entityid,
        'type': 'IstacCubeMetadata',
        'TimeInstant': {
            'type': 'DateTime',
            'value': datetime.utcnow().isoformat(timespec='milliseconds')
        }
    }
    for idx, col in enumerate(cols, start=1):
        meta_entity[f'column{idx}'] = {
            'type': 'TextUnrestricted',
            'value': col
        }
    meta_vars = {
        'AXIS-VERSION': ['axisVersion', 'Text'],
        'LANGUAGE': ['language', 'Text'],
        'CREATION-DATE': ['creationDate', 'TextUnrestricted'],
        'UPDATE-FREQUENCY': ['updateFrequency', 'TextUnrestricted'],
        'TABLEID': ['tableId', 'TextUnrestricted'],
        'MATRIX': ['matrix', 'TextUnrestricted'],
        'SUBJECT-CODE': ['subjectCode', 'Text'],
        'SUBJECT-AREA': ['subjectArea', 'TextUnrestricted'],
        'DESCRIPTION': ['description', 'TextUnrestricted'],
        'TITLE': ['title', 'TextUnrestricted'],
        'CONTENTS': ['contents', 'TextUnrestricted'],
        'UNITS': ['units', 'TextUnrestricted'],
        'STUB': ['stub', 'TextUnrestricted'],
        'HEADING': ['heading', 'TextUnrestricted'],
        'LAST-UPDATED': ['lastUpdated', 'Text'],
        'CONTACT': ['contact', 'TextUnrestricted'],
        'REFPERIOD': ['refPeriod', 'Text'],
        'SOURCE': ['source', 'TextUnrestricted'],
        'SURVEY': ['survey', 'TextUnrestricted'],
        'INFO': ['info', 'TextUnrestricted'],
    }
    for idx, attr in meta_vars.items():
        meta_entity[attr[0]] = {
            'type': attr[1],
            'value': ", ".join(meta.get(idx, []))
        }
    logger.info("Sending IstacCubeMetadata for %s", meta_entity['id'])
    store.send_batch(subservice, [meta_entity])

    monthlist = {
        'enero': 1,
        'febrero': 2,
        'marzo': 3,
        'abril': 4,
        'mayo': 5,
        'junio': 6,
        'julio': 7,
        'agosto': 8,
        'septiembre': 9,
        'octubre': 10,
        'noviembre': 11,
        'diciembre': 12,
    }
    def row_timestamp(row) -> Dict[str, Optional[int]]:
        """Computes year and month from row info"""
        if 'Años' in cols:
            return {'year': int(row['Años']), 'month': None}
        
        if 'Periodos' in cols:
            year, month = None, None
            parts = row['Periodos'].split()
            for part in parts:
                if part.isdigit() and len(part) == 4:
                    year = int(part)
                # overwrite old month if there is a match later
                month = monthlist.get(part.lower(), month)
            # If we recognized at least a year, use it
            if year is not None:
                return {'year': year, 'month': month}

        # If it does not have a temporal dimension, usually last 4 digits of description are the year
        desc_year = meta['DESCRIPTION'][-1].strip().strip(".").strip()[-4:]
        if desc_year.isdigit():
            return {'year': int(desc_year), 'month': None}

        # As a last resource, use LASTUPDATED
        lu = meta.get("LAST-UPDATED(Absoluto)", [])
        if len(lu) == 1:
            year  = int(lu[0][:4])
            month = int(lu[0][4:6])
            return {'year': year, 'month': month}

        raise ValueError(f"Cube {id} ({download}) DOES NOT HAVE TEMPORAL DIMENSION: {meta['DESCRIPTION']}")

    # Turn a row into an entity
    def row_entity(entityid: str, row: Dict[str, Any], cols: List[str]):
        # Normalize year, month and value columns
        timestamp    = row_timestamp(row)
        row['year']  = timestamp['year']
        row['month'] = timestamp['month']
        value = row['DATA']
        if value == '':
            value = None
        elif value is not None:
            value = float(value)
        # Build entity
        entity = {
            'id': entityid,
            'type': 'IstacCube',
            'data': {
                'type': 'None' if value is None else 'Number',
                'value': value,
            },
            'year': {
                'type': 'Number',
                'value': int(row['year']),
            }
        }
        if row.get('month', None) is None:
            entity.update({
                'month': {
                    'type': 'None',
                    'value': None,
                },
                'TimeInstant': {
                    'type': 'DateTime',
                    'value': f"{int(row['year']):d}-01-01T12:00:00Z"
                }
            })
        else:
            entity.update({
                'month': {
                    'type': 'Number',
                    'value': int(row['month']),
                },
                'TimeInstant': {
                    'type': 'DateTime',
                    'value': f"{int(row['year']):d}-{int(row['month']):02d}-01T12:00:00Z"
                }
            })
        for idx, col in enumerate(cols, start=1):
            entity[f'column{idx}'] = {
                'type': 'TextUnrestricted',
                'value': row[col]
            }
        return entity

    bookmark = Bookmark(store, subservice, entityid)
    store.send_batch(subservice, bookmark.updates(row_entity(entityid, row, cols) for row in rows))


# I have copied the implementation of these functions from pyaxis library,
# to customize it a bit, because some pc-axis files are so large that
# pandas exhausts its memory.
# So I turned the result int a generator that yields the items one by one
def build_modin_dataframe(dimension_names, dimension_members, data_values,
                    null_values, sd_values):
    """Build a dataframe from dimensions and data.
       Adds the cartesian product of dimension members plus the series of data.
    Args:
        dimension_names (list of string)
        dimension_members (list of string)
        data_values(Series): pandas series with the data values column.
        null_values(str): regex with the pattern for the null values in the px
                          file. Defaults to '.'.
        sd_values(str): regex with the pattern for the statistical disclosured
                        values in the px file. Defaults to '..'.
    Returns:
        df (pandas dataframe)
    """
    # cartesian product of dimension members
    dim_exploded = list(itertools.product(*dimension_members))

    # column of data values
    nv_replace = re.compile(null_values)
    sd_replace = re.compile(sd_values)
    def new_row(dim_values, value, dim_names=dimension_names, nv_replace=nv_replace, sd_replace=sd_replace):
        result = dict(zip(dim_names, dim_values))
        # null values and statistical disclosure treatment
        if nv_replace.match(value):
            value = None
        elif sd_replace.match(value):
            value = None
        result['DATA'] = value
        return result

    return {
        'COLS': dimension_names,
        'ROWS': (new_row(dim_values, value) for (dim_values, value) in zip(dim_exploded, data_values))
    }


def pyaxis_parse(uri, encoding, timeout=10,
          null_values=r'^"\."$', sd_values=r'"\.\."'):
    """Extract metadata and data sections from pc-axis.
    Args:
        uri (str): file name or URL
        encoding (str): charset encoding
        timeout (int): request timeout in seconds; optional
        null_values(str): regex with the pattern for the null values in the px
                          file. Defaults to '.'.
        sd_values(str): regex with the pattern for the statistical disclosured
                        values in the px file. Defaults to '..'.
    Returns:
         pc_axis_dict (dictionary): dictionary of metadata and pandas df.
                                    METADATA: dictionary of metadata
                                    DATA: pandas dataframe
    """
    # get file content or URL stream
    try:
        pc_axis = pyaxis.read(uri, encoding, timeout)
    except ValueError:
        import traceback
        logger.error('Generic exception: %s', traceback.format_exc())
        raise

    # metadata and data extraction and cleaning
    metadata_elements, raw_data = pyaxis.metadata_extract(pc_axis)

    # stores raw metadata into a dictionary
    metadata = pyaxis.metadata_split_to_dict(metadata_elements)

    # explode raw data into a Series of values, which can contain nullos or sd
    # (statistical disclosure)
    data_values = raw_data.split()

    # extract dimension names and members from
    # 'meta_dict' STUB and HEADING keys
    dimension_names, dimension_members = pyaxis.get_dimensions(metadata)

    # build a dataframe
    df = build_modin_dataframe(
        dimension_names,
        dimension_members,
        data_values,
        null_values=null_values,
        sd_values=sd_values)

    # dictionary of metadata and data (pandas dataframe)
    parsed_pc_axis = {
        'METADATA': metadata,
        'DATA': df
    }
    return parsed_pc_axis


### Main program starts here ###
if __name__ == "__main__":

    # Gets the config
    parser = argparse.ArgumentParser(description='Load Istac cubes')
    parser.add_argument('-c', '--config', dest='config', help='path to config file')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    if args.config:
        logging.info('Reading config from file %s', {args.config})
        config.read(args.config)

    endpoint_cb = read_config(config, 'environment', 'endpoint_cb')
    endpoint_keystone = read_config(config, 'environment', 'endpoint_keystone')
    service = read_config(config, 'environment', 'service')
    subservice = read_config(config, 'environment', 'subservice')
    user = read_config(config, 'environment', 'user')
    password = read_config(config, 'environment', 'password')

    sleep_send_batch = read_config_int(config, 'settings', 'sleep_send_batch', '5')
    timeout = read_config_int(config, 'settings', 'timeout', '10')
    post_retry_connect = read_config_int(config, 'settings', 'post_retry_connect', '3')
    post_retry_backoff_factor = read_config_int(config, 'settings', 'post_retry_backoff_factor', '20')
    batch_size = read_config_int(config, 'settings', 'batch_size', '50')

    sql_file = read_config(config, 'settings', 'sql_file', '')
    csv_file = read_config(config, 'settings', 'csv_file')

    store: Store
    if sql_file:
        # Use SQL file store to speed up initial load
        # See https://github.com/psycopg/psycopg2/issues/331
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        store = SQLFileStore(sql_file=Path(sql_file), service=service, entity_table={
            "IstacCube": "cx_istaccube",
            "IstacCubeMetadata": "cx_istaccubemetadata_lastdata",
            "IstacCubeBookmark": "" # Do not save tis one to DB
        })
    else:
        # Use context broker store
        store = OrionStore(
            endpoint_keystone=endpoint_keystone,
            endpoint_cb=endpoint_cb,
            user=user,
            password=password,
            service=service,
            subservice=subservice,
            timeout=timeout,
            post_retry_connect=post_retry_connect,
            post_retry_backoff_factor=post_retry_backoff_factor,
            sleep_send_batch=sleep_send_batch,
            batch_size=batch_size
        )

    store.open()
    try:
        logger.info('*** SEND ISTAC CUBES: Starting ***')
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=Retry(
            total=post_retry_connect,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504]
        ))
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        for _, row in read_csv(csv_file).iterrows():
            logger.info('*** SEND ISTAC CUBE %s ***', row["id"])
            read_cube(session, store, subservice, row['id'], row['download'])

    finally:
        store.close()
