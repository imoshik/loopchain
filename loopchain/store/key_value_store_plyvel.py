# Copyright 2019 ICON Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import functools
import urllib.parse
import plyvel

from loopchain.store.key_value_store import KeyValueStoreError, KeyValueStoreIOError
from loopchain.store.key_value_store import KeyValueStoreWriteBatch, KeyValueStore


def _error_convert(func):

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except plyvel.IOError as e:
            raise KeyValueStoreIOError(e)
        except plyvel.Error as e:
            raise KeyValueStoreError(e)

    return _wrapper


def _check_bytes(value):
    if not isinstance(value, bytes):
        raise ValueError(f"Argument type is not bytes. Argument type is {type(value)}")


class KeyValueStoreWriteBatchPlyvel(KeyValueStoreWriteBatch):

    def __init__(self, db: plyvel.DB, **kwargs):
        self.__write_batch = self.__new_write_batch(db, **kwargs)

    @_error_convert
    def __new_write_batch(self, db: plyvel.DB, **kwargs):
        return db.write_batch(**kwargs)

    @_error_convert
    def put(self, key, value):
        _check_bytes(key)
        _check_bytes(value)

        self.__write_batch.put(key, value)

    @_error_convert
    def delete(self, key):
        _check_bytes(key)

        self.__write_batch.delete(key)

    @_error_convert
    def clear(self):
        self.__write_batch.clear()

    @_error_convert
    def write(self):
        self.__write_batch.write()


class KeyValueStorePlyvel(KeyValueStore):
    def __init__(self, uri: str, **kwargs):
        uri_obj = urllib.parse.urlparse(uri)
        if uri_obj.scheme != 'file':
            raise ValueError(f"Support file path URI only (ex. file:///xxx/xxx). uri={uri}")
        self.__db = self.__new_db(uri_obj.path, **kwargs)

    @_error_convert
    def __new_db(self, path, **kwargs) ->plyvel.DB:
        return plyvel.DB(path, **kwargs)

    @_error_convert
    def get(self, key, default=None, **kwargs):
        _check_bytes(key)

        result = self.__db.get(key, default=default, **kwargs)
        if result is None:
            raise KeyError(f"Has no value of key({key}")

    @_error_convert
    def put(self, key, value, **kwargs):
        _check_bytes(key)
        _check_bytes(value)

        self.__db.put(key, value, **kwargs)

    @_error_convert
    def close(self):
        self.__db.close()

    @_error_convert
    def delete(self, key, **kwargs):
        _check_bytes(key)

        self.__db.delete(key, **kwargs)

    @_error_convert
    def WriteBatch(self, **kwargs) -> KeyValueStoreWriteBatch:
        return self.__db.write_batch(**kwargs)
