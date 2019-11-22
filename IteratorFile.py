# -*- coding: utf-8 -*-
import io
import sys


def format_line(record, column_data):
    line_template = '\t'.join(['%s'] * len(column_data))
    data = []
    for column in column_data:
        key = column['key']
        value = None

        # get the value
        if key in record:
            value = record[key]

        if value is None:
            data.append('\\N')
        elif 'encoder' in column:
            data.append(column['encoder'](value))
        else:
            data.append(value)

    return line_template % tuple(data)



class IteratorFile(io.TextIOBase):
    """
        Use this class to support writing geometries to PostGIS using COPY
        based on https://gist.github.com/jsheedy/ed81cdf18190183b3b7d
    """

    def __init__(self, records, column_data):
        self._records = records
        self._column_data = column_data
        self._f = io.StringIO()

    def read(self, length=sys.maxsize):
        try:
            while self._f.tell() < length:
                self._f.write(self._get_next() + '\n')
        except StopIteration:
            # soak up StopIteration. this block is not necessary because
            # of finally, but just to be explicit
            pass
        except Exception as e:
            print('uncaught exception: {}'.format(e))
            raise e
        finally:
            self._f.seek(0)
            data = self._f.read(length)
            # save the remainder for next read
            remainder = self._f.read()
            self._f.seek(0)
            self._f.truncate(0)
            self._f.write(remainder)
            return data

    def readline(self):
        return self._get_next()

    def _get_next(self):
        record = next(self._records)
        return format_line(record, self._column_data)
