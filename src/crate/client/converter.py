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
from typing import Any, Callable, Dict, Optional, Union


def _to_ipaddress(value: Optional[str]) -> Optional[Union[ipaddress.IPv4Address, ipaddress.IPv6Address]]:
    """
    https://docs.python.org/3/library/ipaddress.html
    """
    if value is None:
        return None
    return ipaddress.ip_address(value)


def _to_datetime(value: Optional[str]) -> Optional[datetime]:
    """
    https://docs.python.org/3/library/datetime.html
    """
    if value is None:
        return None
    return datetime.utcfromtimestamp(value / 1e3)


def _to_default(value: Optional[str]) -> Optional[str]:
    return value


# Mapping for numeric data type identifiers defined by the CrateDB HTTP interface.
# https://crate.io/docs/crate/reference/en/latest/interfaces/http.html#column-types
_DEFAULT_CONVERTERS: Dict[int, Callable[[Optional[int]], Optional[Any]]] = {
    5: _to_ipaddress,
    11: _to_datetime,
}


class Converter:
    def __init__(
        self,
        mappings: Dict[int, Callable[[Optional[int]], Optional[Any]]] = None,
        default: Callable[[Optional[int]], Optional[Any]] = _to_default,
    ) -> None:
        self._mappings = mappings or {}
        self._default = default

    @property
    def mappings(self) -> Dict[int, Callable[[Optional[int]], Optional[Any]]]:
        return self._mappings

    def get(self, type_: int) -> Callable[[Optional[int]], Optional[Any]]:
        return self.mappings.get(type_, self._default)

    def set(self, type_: int, converter: Callable[[Optional[int]], Optional[Any]]) -> None:
        self.mappings[type_] = converter

    def remove(self, type_: int) -> None:
        self.mappings.pop(type_, None)

    def update(self, mappings: Dict[int, Callable[[Optional[int]], Optional[Any]]]) -> None:
        self.mappings.update(mappings)

    def convert(self, type_: int, value: Optional[Any]) -> Optional[Any]:
        converter = self.get(type_)
        return converter(value)


class DefaultTypeConverter(Converter):
    def __init__(self) -> None:
        super(DefaultTypeConverter, self).__init__(
            mappings=deepcopy(_DEFAULT_CONVERTERS), default=_to_default
        )