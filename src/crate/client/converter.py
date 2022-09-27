# -*- coding: utf-8; -*-
#
# Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.
"""
Machinery for converting CrateDB database types to native Python data types.

https://crate.io/docs/crate/reference/en/latest/interfaces/http.html#column-types
"""
import ipaddress
from copy import deepcopy
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Optional, Union, List

InputVal = Any


def _to_ipaddress(value: Optional[str]) -> Optional[Union[ipaddress.IPv4Address, ipaddress.IPv6Address]]:
    """
    https://docs.python.org/3/library/ipaddress.html
    """
    if value is None:
        return None
    return ipaddress.ip_address(value)


def _to_datetime(value: Optional[float]) -> Optional[datetime]:
    """
    https://docs.python.org/3/library/datetime.html
    """
    if value is None:
        return None
    return datetime.utcfromtimestamp(value / 1e3)


def _to_default(value: Optional[InputVal]) -> Optional[Any]:
    return value


# Symbolic aliases for the numeric data type identifiers defined by the CrateDB HTTP interface.
# https://crate.io/docs/crate/reference/en/latest/interfaces/http.html#column-types
class CrateDatatypeIdentifier(Enum):
    NULL = 0
    NOT_SUPPORTED = 1
    CHAR = 2
    BOOLEAN = 3
    TEXT = 4
    IP = 5
    DOUBLE = 6
    REAL = 7
    SMALLINT = 8
    INTEGER = 9
    BIGINT = 10
    TIMESTAMP = 11
    OBJECT = 12
    GEOPOINT = 13
    GEOSHAPE = 14
    UNCHECKED_OBJECT = 15
    REGPROC = 19
    TIME = 20
    OIDVECTOR = 21
    NUMERIC = 22
    REGCLASS = 23
    DATE = 24
    BIT = 25
    JSON = 26
    CHARACTER = 27
    ARRAY = 100


# Map data type identifier to converter function.
_DEFAULT_CONVERTERS: Dict[int, Callable[[Optional[InputVal]], Optional[Any]]] = {
    CrateDatatypeIdentifier.IP.value: _to_ipaddress,
    CrateDatatypeIdentifier.TIMESTAMP.value: _to_datetime,
}


class Converter:
    def __init__(
        self,
        mappings: Dict[int, Callable[[Optional[InputVal]], Optional[Any]]] = None,
        default: Callable[[Optional[InputVal]], Optional[Any]] = _to_default,
    ) -> None:
        self._mappings = mappings or {}
        self._default = default

    @property
    def mappings(self) -> Dict[int, Callable[[Optional[InputVal]], Optional[Any]]]:
        return self._mappings

    def get(self, type_: int) -> Callable[[Optional[InputVal]], Optional[Any]]:
        return self.mappings.get(type_, self._default)

    def set(self, type_: Union[CrateDatatypeIdentifier, int], converter: Callable[[Optional[InputVal]], Optional[Any]]) -> None:
        type_int = self.get_mapping_key(type_)
        self.mappings[type_int] = converter

    @staticmethod
    def get_mapping_key(type_: Union[CrateDatatypeIdentifier, int]) -> int:
        if isinstance(type_, Enum):
            return type_.value
        else:
            return type_

    def convert(self, type_: int, value: Optional[Any]) -> Optional[Any]:
        if isinstance(type_, List):
            type_, inner_type = type_
            assert type_ == 100, f"Type {type_} not implemented as collection type"
            if value is None:
                result = self.convert(inner_type, None)
            else:
                result = [self.convert(inner_type, item) for item in value]
        else:
            converter = self.get(type_)
            result = converter(value)
        return result


class DefaultTypeConverter(Converter):
    def __init__(self) -> None:
        super(DefaultTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_CONVERTERS), default=_to_default
        )
