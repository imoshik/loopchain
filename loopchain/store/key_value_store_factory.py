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

import enum

from loopchain.store.key_value_store import KeyValueStore
from loopchain.store.key_value_store_plyvel import KeyValueStorePlyvel


class KeyValueStoreType(enum.Enum):
    PLYVEL = 'plyvel'


class KeyValueStoreFactory:

    @staticmethod
    def new(store_type: KeyValueStoreType, uri: str, **kwargs) -> KeyValueStore:
        if store_type == KeyValueStoreType.PLYVEL:
            return KeyValueStorePlyvel(uri, **kwargs)
        else:
            raise ValueError(f"store_name is invalid. store_type={store_type}")
