# -*- coding: utf-8 -*-
import random
import json
import os
from datetime import date
from pygeos import to_wkb, set_srid, points
import psycopg2

from copy_writer import CopyWriter


def random_point():
    point = points(random.uniform(-90, 90), random.uniform(-180, 180))
    return set_srid(point, 4326)


def get_generator():
    for i in range(0, 11):
        yield {
            'id': i,
            'str': 'test',
            'dict': {'a': 1, 'b': 2},
            'date': date.today(),
            'geom': random_point(),
            'maybe': None
        }


def encode_geom(geom):
    return to_wkb(geom, hex=True, include_srid=True)


def encode_json(value):
    def escape(value):
        return value.replace('\\', '\\\\')
    return escape(json.dumps(
        value,
        ensure_ascii=False
        ))

column_data = [
    {'name': 'id', 'key': 'id'},
    {'name': 'a_string', 'key': 'str'},
    {'name': 'a_dict', 'key': 'dict', 'encoder': encode_json},
    {'name': 'a_date', 'key': 'date'},
    {'name': 'maybe', 'key': 'maybe'},
    {'name': 'geom', 'key': 'geom', 'encoder': encode_geom}
]


if __name__ == '__main__':
    connection = psycopg2.connect(os.environ['CONN_STR'])
    cw = CopyWriter(connection, column_data, partition=2)
    cw.write(get_generator(), 'test')
