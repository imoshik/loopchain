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


import abc


class KeyValueStoreError(Exception):
    pass


class KeyValueStoreIOError(IOError):
    pass


class KeyValueStoreWriteBatch(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def put(self, key, value, **kwargs):
        raise NotImplementedError("put() function is interface method")

    @abc.abstractmethod
    def delete(self, key, **kwargs):
        raise NotImplementedError("delete() function is interface method")

    @abc.abstractmethod
    def clear(self):
        raise NotImplementedError("clear() function is interface method")

    @abc.abstractmethod
    def write(self):
        raise NotImplementedError("write() function is interface method")


class KeyValueStore(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get(self, key, default=None, **kwargs):
        raise NotImplementedError("get() function is interface method")

    @abc.abstractmethod
    def put(self, key, value, **kwargs):
        raise NotImplementedError("put() function is interface method")

    @abc.abstractmethod
    def delete(self, key, **kwargs):
        raise NotImplementedError("delete() function is interface method")

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError("close() function is interface method")

    @abc.abstractmethod
    def WriteBatch(self) -> KeyValueStoreWriteBatch:
        raise NotImplementedError("Batch constructor is not implemented in KeyValueStore class")

    def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc_value, traceback):
        self.close()
