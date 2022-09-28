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
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

InputVal = Any
ConverterFunction = Callable[[Optional[InputVal]], Optional[Any]]
ConverterDefinition = Union[ConverterFunction, Tuple[ConverterFunction, "ConverterDefinition"]]
ColTypesDefinition = Union[int, List[Union[int, "ColTypesDefinition"]]]


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
class DataType(Enum):
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
    TIMESTAMP_WITH_TZ = 11
    OBJECT = 12
    GEOPOINT = 13
    GEOSHAPE = 14
    TIMESTAMP_WITHOUT_TZ = 15
    UNCHECKED_OBJECT = 16
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
_DEFAULT_CONVERTERS: Dict[DataType, ConverterFunction] = {
    DataType.IP: _to_ipaddress,
    DataType.TIMESTAMP_WITH_TZ: _to_datetime,
    DataType.TIMESTAMP_WITHOUT_TZ: _to_datetime,
}


class Converter:
    def __init__(
        self,
        mappings: Optional[Dict[DataType, ConverterFunction]] = None,
        default: ConverterFunction = _to_default,
    ) -> None:
        self._mappings = mappings or {}
        self._default = default

    @property
    def mappings(self) -> Dict[DataType, ConverterFunction]:
        return self._mappings

    def get(self, type_: DataType) -> ConverterFunction:
        return self.mappings.get(type_, self._default)

    def set(self, type_: DataType, converter: ConverterFunction) -> None:
        self.mappings[type_] = converter

    def convert(self, converter_definition: ConverterDefinition, value: Optional[Any]) -> Optional[Any]:
        """
        Convert a single row cell value using given converter definition.
        Also works recursively on nested values like `ARRAY` collections.
        Invoked from `Cursor._convert_rows`.
        """
        if isinstance(converter_definition, tuple):
            type_, inner_type = converter_definition
            if value is None:
                result = self.convert(inner_type, None)
            else:
                result = [self.convert(inner_type, item) for item in value]
        else:
            result = converter_definition(value)
        return result

    def col_type_to_converter(self, type_: ColTypesDefinition) -> ConverterDefinition:
        """
        Resolve integer data type identifier to its corresponding converter function.
        Also handles nested definitions with a *list* of data type identifiers on the
        right hand side, describing the inner type of `ARRAY` values.

        It is important to resolve the converter functions first, in order not to
        hog the row loop with redundant lookups to the `mappings` dictionary.
        """
        result: ConverterDefinition
        if isinstance(type_, list):
            type_, inner_type = type_
            if DataType(type_) is not DataType.ARRAY:
                raise ValueError(f"Data type {type_} is not implemented as collection type")
            result = (self.get(DataType(type_)), self.col_type_to_converter(inner_type))
        else:
            result = self.get(DataType(type_))
        return result


class DefaultTypeConverter(Converter):
    def __init__(self) -> None:
        super(DefaultTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_CONVERTERS), default=_to_default
        )
