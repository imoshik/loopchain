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

from loopchain.store.key_value_store import KeyValueStoreError, KeyValueStoreIOError
from loopchain.store.key_value_store import KeyValueStoreWriteBatch, KeyValueStore


def _error_convert(func):

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise KeyValueStoreError(e)

    return _wrapper


def _check_bytes(value):
    if not isinstance(value, bytes):
        raise ValueError(f"Argument type is not bytes. Argument type is {type(value)}")


class KeyValueStoreWriteBatchDict(KeyValueStoreWriteBatch):

    def __init__(self, store_dict):
        self.__store_dict = store_dict
        self.__batch_dict = dict()

    @_error_convert
    def put(self, key, value):
        _check_bytes(key)
        _check_bytes(value)

        self.__batch_dict[key] = value

    @_error_convert
    def delete(self, key):
        _check_bytes(key)

        try:
            del self.__batch_dict[key]
        except KeyError:
            pass

    @_error_convert
    def clear(self):
        self.__batch_dict = dict()

    @_error_convert
    def write(self):
        for key, value in self.__batch_dict.items():
            self.__store_dict[key] = value


class KeyValueStoreDict(KeyValueStore):
    def __init__(self):
        self.__store_dict = dict()

    @_error_convert
    def get(self, key, default=None, **kwargs):
        _check_bytes(key)

        try:
            return self.__store_dict[key]
        except KeyError as e:
            if default is None:
                raise KeyError(f"Has no value of key({key}")
            return default

    @_error_convert
    def put(self, key, value, **kwargs):
        _check_bytes(key)
        _check_bytes(value)

        self.__store_dict[key] = value

    @_error_convert
    def close(self):
        self.__store_dict = None

    @_error_convert
    def delete(self, key, **kwargs):
        _check_bytes(key)

        try:
            del self.__batch_dict[key]
        except KeyError:
            pass

    @_error_convert
    def WriteBatch(self, **kwargs) -> KeyValueStoreWriteBatch:
        return KeyValueStoreWriteBatchDict(self.__store_dict)
