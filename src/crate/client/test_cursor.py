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

from datetime import datetime
from ipaddress import IPv4Address
from unittest import TestCase
from unittest.mock import MagicMock

from crate.client import connect
from crate.client.converter import DataType
from crate.client.cursor import Cursor
from crate.client.http import Client
from crate.client.test_util import ClientMocked


class CursorTest(TestCase):

    def test_execute_with_args(self):
        client = MagicMock(spec=Client)
        conn = connect(client=client)
        c = conn.cursor()
        statement = 'select * from locations where position = ?'
        c.execute(statement, 1)
        client.sql.assert_called_once_with(statement, 1, None)
        conn.close()

    def test_execute_with_bulk_args(self):
        client = MagicMock(spec=Client)
        conn = connect(client=client)
        c = conn.cursor()
        statement = 'select * from locations where position = ?'
        c.execute(statement, bulk_parameters=[[1]])
        client.sql.assert_called_once_with(statement, None, [[1]])
        conn.close()

    def test_execute_with_converter(self):
        client = ClientMocked()
        conn = connect(client=client)

        # Use the set of data type converters from `DefaultTypeConverter`
        # and add another custom converter.
        converter = Cursor.get_default_converter()
        converter.set(DataType.BIT, lambda value: value is not None and int(value[2:-1], 2) or None)

        # Create a `Cursor` object with converter.
        c = conn.cursor(converter=converter)

        # Make up a response using CrateDB data types `TEXT`, `IP`,
        # `TIMESTAMP`, `BIT`.
        conn.client.set_next_response({
            "col_types": [4, 5, 11, 25],
            "cols": ["name", "address", "timestamp", "bitmask"],
            "rows": [
                ["foo", "10.10.10.1", 1658167836758, "B'0110'"],
                [None, None, None, None],
            ],
            "rowcount": 1,
            "duration": 123
        })

        c.execute("")
        result = c.fetchall()
        self.assertEqual(result, [
            ['foo', IPv4Address('10.10.10.1'), datetime(2022, 7, 18, 18, 10, 36, 758000), 6],
            [None, None, None, None],
        ])

        conn.close()

    def test_execute_array_with_converter(self):
        client = ClientMocked()
        conn = connect(client=client)
        converter = Cursor.get_default_converter()
        cursor = conn.cursor(converter=converter)

        conn.client.set_next_response({
            "col_types": [4, [100, 5]],
            "cols": ["name", "address"],
            "rows": [["foo", ["10.10.10.1", "10.10.10.2"]]],
            "rowcount": 1,
            "duration": 123
        })

        cursor.execute("")
        result = cursor.fetchone()
        self.assertEqual(result, [
            'foo',
            [IPv4Address('10.10.10.1'), IPv4Address('10.10.10.2')],
        ])

    def test_execute_nested_array_with_converter(self):
        client = ClientMocked()
        conn = connect(client=client)
        converter = Cursor.get_default_converter()
        cursor = conn.cursor(converter=converter)

        conn.client.set_next_response({
            "col_types": [4, [100, [100, 5]]],
            "cols": ["name", "address_buckets"],
            "rows": [["foo", [["10.10.10.1", "10.10.10.2"], ["10.10.10.3"], [], None]]],
            "rowcount": 1,
            "duration": 123
        })

        cursor.execute("")
        result = cursor.fetchone()
        self.assertEqual(result, [
            'foo',
            [[IPv4Address('10.10.10.1'), IPv4Address('10.10.10.2')], [IPv4Address('10.10.10.3')], [], None],
        ])
