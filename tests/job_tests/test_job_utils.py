"""
Test job utils.
"""


from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import unittest

from pygenie.jobs.utils import arg_list


class ArgList(object):
    def __init__(self):
        self.my_list = list()

    @arg_list
    def add_to_list(self, my_list):
        """Adds item to self.my_list."""


class TestArgList(unittest.TestCase):
    """Test pygenie.jobs.utils.arg_list decorator."""

    def test_strings(self):
        """Test pygenie.jobs.utils.arg_list with string arguments."""

        arglist = ArgList()
        arglist.add_to_list('a')
        arglist.add_to_list('b')
        arglist.add_to_list('c')

        assert (
            ['a', 'b', 'c'] ==
            arglist.my_list)

    def test_lists(self):
        """Test pygenie.jobs.utils.arg_list with list of string arguments."""

        arglist = ArgList()
        arglist.add_to_list(['1'])
        arglist.add_to_list(['2'])
        arglist.add_to_list(['3'])

        assert (
            ['1', '2', '3'] ==
            arglist.my_list)

    def test_duplicates_strs(self):
        """Test pygenie.jobs.utils.arg_list with duplicate arguments (strings)."""

        arglist = ArgList()
        arglist.add_to_list('f')
        arglist.add_to_list('e')
        arglist.add_to_list('f')

        assert (
            ['f', 'e'] ==
            arglist.my_list)

    def test_mixed_types(self):
        """Test pygenie.jobs.utils.arg_list with mixed type arguments."""

        arglist = ArgList()
        arglist.add_to_list('g')
        arglist.add_to_list([{'h': 'h'}])
        arglist.add_to_list('f')

        assert (
            ["g", {"h": "h"}, "f"] ==
            arglist.my_list)
