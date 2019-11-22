# -*- coding: utf-8 -*-
import sys


from split_generator import split_generator as split
from IteratorFile import IteratorFile


class CopyWriter:
    def __init__(self, connection, column_data, partition=100000):
        self.partition = partition
        self.connection = connection
        self.column_data = column_data
        self.columns = [column['key'] for column in column_data]

    def write(self, feature_generator, table_name):
        print(f'save to {table_name}')
        c = 1
        with self.connection.cursor() as cur:
            for generator in split(feature_generator, self.partition):
                file = IteratorFile(generator, self.column_data)
                cur.copy_from(file, table_name, columns=self.columns)
                self.connection.commit()
                sys.stdout.write(f'\rWritten: {c * self.partition:,}')
                sys.stdout.flush()
                c += 1
