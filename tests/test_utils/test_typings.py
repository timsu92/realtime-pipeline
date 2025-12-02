import unittest
from typing import TYPE_CHECKING

from realtime_pipeline import Node
from realtime_pipeline.utils.typings import (
    node_downstream_from_class,
    node_downstream_from_instance,
    node_upstream_from_class,
    node_upstream_from_instance,
)


class TestTyping(unittest.TestCase):
    """Test typing-related functionalities of Node class"""

    class Inherited(Node[int, float, str]):
        pass

    Direct = Node[int, float, str]

    def setUp(self) -> None:
        super().setUp()
        self.node_direct = TestTyping.Direct()

        self.node_inherited = TestTyping.Inherited()

    ##### Tests for upstream type detection #####

    def test_upstream_when_supplied_inherit_class(self):
        """Test upstream arity detection when concrete types are supplied"""
        upstreams = node_upstream_from_class(TestTyping.Inherited)
        self.assertIsNotNone(upstreams)
        if TYPE_CHECKING:
            assert isinstance(upstreams, tuple)
        self.assertTupleEqual(upstreams, (int, float))

    def test_upstream_when_supplied_inherit_instance(self):
        """Test upstream arity detection when concrete types are supplied"""
        upstreams = node_upstream_from_instance(self.node_inherited)
        self.assertIsNotNone(upstreams)
        if TYPE_CHECKING:
            assert isinstance(upstreams, tuple)
        self.assertTupleEqual(upstreams, (int, float))

    def test_upstream_when_supplied_direct_class(self):
        """Test upstream arity detection when concrete types are supplied"""
        upstreams = node_upstream_from_class(TestTyping.Direct)
        self.assertIsNotNone(upstreams)
        if TYPE_CHECKING:
            assert isinstance(upstreams, tuple)
        self.assertTupleEqual(upstreams, (int, float))

    def test_upstream_when_supplied_direct_instance(self):
        """Test upstream arity detection when concrete types are supplied"""
        upstreams = node_upstream_from_instance(self.node_direct)
        self.assertIsNotNone(upstreams)
        if TYPE_CHECKING:
            assert isinstance(upstreams, tuple)
        self.assertTupleEqual(upstreams, (int, float))

    def test_upstream_when_generic_instance(self):
        """Test upstream arity detection when generic types are used"""
        generic_node = Node()
        upstreams = node_upstream_from_instance(generic_node)
        self.assertIsNone(upstreams)

    def test_upstream_when_generic_class(self):
        """Test upstream arity detection when generic types are used"""
        upstreams = node_upstream_from_class(Node)
        self.assertIsNone(upstreams)

    ##### Tests for downstream type detection #####

    def test_downstream_when_supplied_inherit_class(self):
        """Test downstream type detection when concrete types are supplied"""
        downstream = node_downstream_from_class(TestTyping.Inherited)
        self.assertEqual(downstream, str)

    def test_downstream_when_supplied_inherit_instance(self):
        """Test downstream type detection when concrete types are supplied"""
        downstream = node_downstream_from_instance(self.node_inherited)
        self.assertEqual(downstream, str)

    def test_downstream_when_supplied_direct_class(self):
        """Test downstream type detection when concrete types are supplied"""
        downstream = node_downstream_from_class(TestTyping.Direct)
        self.assertEqual(downstream, str)

    def test_downstream_when_supplied_direct_instance(self):
        """Test downstream type detection when concrete types are supplied"""
        downstream = node_downstream_from_instance(self.node_direct)
        self.assertEqual(downstream, str)

    def test_downstream_when_generic_class(self):
        """Test downstream type detection when generic types are used"""
        downstream = node_downstream_from_class(Node)
        self.assertIsNone(downstream)

    def test_downstream_when_generic_instance(self):
        """Test downstream type detection when generic types are used"""
        generic_node = Node()
        downstream = node_downstream_from_instance(generic_node)
        self.assertIsNone(downstream)
