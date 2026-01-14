import itertools
import threading
import time
import unittest
from random import randint
from typing import Optional

from realtime_pipeline import Node


def simple_processor(*data):
    """Simple processor that converts string to uppercase"""
    return [str(d).upper() for d in data]


def sum_processor(*data):
    """Simple processor that sums numeric data"""
    return sum(float(d) for d in data)


class BasicNodeFunctions(unittest.TestCase):
    """Test basic functionalities of Node class"""

    def setUp(self):
        # Simple one-to-one relationship setup
        self.upstream = Node(target=lambda _: "dummy")
        self.downstream = Node(target=simple_processor)
        self.downstream.subscribe_to(self.upstream)

        # Manually provide some test data for the upstream node
        self.upstream._data[1.0] = "test_data_1"
        self.upstream._data[2.0] = "test_data_2"
        self.upstream._data[3.0] = "test_data_3"

    def test_subscribe(self):
        self.assertIn(self.downstream, self.upstream._last_downstream_gots.keys())
        self.assertIn(self.upstream, self.downstream._upstreams)
        self.assertEqual(self.upstream._last_downstream_gots[self.downstream], -1)

    def test_unsubscribe(self):
        self.downstream.unsubscribe_from(self.upstream)
        self.assertNotIn(self.downstream, self.upstream._last_downstream_gots)
        self.assertNotIn(self.upstream, self.downstream._upstreams)

    def test_query_availables_when_data_available(self):
        availables = self.upstream._query_availables(self.downstream, block=False)
        self.assertListEqual(availables, [1.0, 2.0, 3.0])
        availables = self.upstream._query_availables(self.downstream, block=True)
        self.assertListEqual(availables, [1.0, 2.0, 3.0])

    def test_query_availables_when_no_data_with_block(self):
        self.upstream._data.clear()
        t = threading.Thread(
            target=self.upstream._query_availables,
            args=(self.downstream, True),
            daemon=True,
        )
        t.start()
        t.join(timeout=0.3)
        self.assertTrue(t.is_alive(), "Thread should be waiting for new data")

    def test_query_availables_when_no_data_and_no_block(self):
        self.upstream._data.clear()
        availables = self.upstream._query_availables(self.downstream, block=False)
        self.assertListEqual(availables, [])

    def test_give_data(self):
        data = self.upstream._give_data(2.0, self.downstream)
        self.assertEqual(data, "test_data_2")
        self.assertEqual(self.upstream._last_downstream_gots[self.downstream], 2.0)

    def test_cleanup_old_data_manual(self):
        self.upstream._last_downstream_gots[self.downstream] = 2.0
        self.upstream._cleanup_old_data()
        self.assertEqual(len(self.upstream._data), 1)
        self.assertIn(3.0, self.upstream._data)
        self.assertNotIn(1.0, self.upstream._data)
        self.assertNotIn(2.0, self.upstream._data)

    def test_cleanup_old_data_through_api(self):
        # Add test data through normal methods
        self.upstream._data[1.0] = "data_1"
        self.upstream._data[2.0] = "data_2"
        self.upstream._data[3.0] = "data_3"
        self.upstream._data[4.0] = "data_4"
        self.upstream._new_data_available.set()

        # Let downstream consume the first two pieces of data through the normal API
        self.upstream._give_data(1.0, self.downstream)
        self.upstream._give_data(2.0, self.downstream)

        # Verify consumption records are correct
        self.assertEqual(self.upstream._last_downstream_gots[self.downstream], 2.0)

        # Now perform cleanup, data <= 2.0 should be cleared
        self.upstream._cleanup_old_data()

        # Verify cleanup results: 1.0 and 2.0 should be cleared, 3.0 and 4.0 should remain
        self.assertEqual(len(self.upstream._data), 2)
        self.assertNotIn(1.0, self.upstream._data)
        self.assertNotIn(2.0, self.upstream._data)
        self.assertIn(3.0, self.upstream._data)
        self.assertIn(4.0, self.upstream._data)

        # Continue sending data
        self.upstream._give_data(4.0, self.downstream)

        # Verify consumption records are correct
        self.assertEqual(self.upstream._last_downstream_gots[self.downstream], 4.0)

        # Perform cleanup, all data should be cleared
        self.upstream._cleanup_old_data()
        self.assertEqual(len(self.upstream._data), 0)

    def test_wait_for_any_upstream_data(self):
        self.upstream._data.clear()
        t = threading.Thread(
            target=self.downstream._wait_for_any_upstream_data, daemon=True
        )
        t.start()
        t.join(timeout=0.3)
        self.assertTrue(t.is_alive(), "Thread should be waiting for new data")

        # Simulate new data arrival
        self.upstream._data[1.0] = "new_data"
        self.upstream._new_data_available.set()
        t.join()  # Should be able to get new data and finish


class TestNodeOneToOne(unittest.TestCase):
    """Test one-to-one pub/sub relationship"""

    def setUp(self):
        # Create a simple one-to-one relationship
        self.upstream = Node(target=lambda _: "dummy")
        self.downstream = Node(target=simple_processor)
        self.downstream.subscribe_to(self.upstream)

        # Manually provide some test data for the upstream node
        self.upstream._data[1.0] = "test_data_1"
        self.upstream._data[2.0] = "test_data_2"
        self.upstream._data[3.0] = "test_data_3"

    def test_thread_safety_basic(self):
        results = []

        def reader():
            availables = self.upstream._query_availables(self.downstream)
            results.append(len(availables))

        def writer():
            # Simulate new data being added
            current_time = time.time()
            self.upstream._data[current_time] = f"new_data_{current_time}"
            self.upstream._new_data_available.set()

        threads = [threading.Thread(target=reader) for _ in range(5)] + [
            threading.Thread(target=writer) for _ in range(2)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Assert that we have read the available data correctly
        self.assertEqual(len(results), 5)
        # Assert that new data has been added
        self.assertEqual(len(self.upstream._data), 5)

    def test_basic_data_flow(self):
        """Test basic data flow from upstream to downstream"""
        self.upstream.daemon = True
        self.downstream.daemon = True
        self.upstream._data.clear()
        self.downstream.start()
        for i in range(1, 4):
            self.upstream._data[i] = f"test_data_{i}"
            self.upstream._new_data_available.set()
            time.sleep(0.05)
        self.assertEqual(len(self.downstream._data), 3)
        self.assertSequenceEqual(self.downstream._data.keys(), [1.0, 2.0, 3.0])
        self.assertSequenceEqual(
            self.downstream._data.values(),
            [["TEST_DATA_1"], ["TEST_DATA_2"], ["TEST_DATA_3"]],
        )


class TestNodeMultipleUpstreams(unittest.TestCase):
    """Test multiple upstreams to a single downstream node"""

    def setUp(self):
        self.up1 = Node(target=lambda _: "up1_data")
        self.up2 = Node(target=lambda _: "up2_data")
        self.up3 = Node(target=lambda _: "up3_data")
        self.downstream = Node(
            target=lambda datas: tuple(datas), acceptable_time_bias=1.5
        )

        # Subscribe downstream to all upstreams
        self.downstream.subscribe_to(self.up1)
        self.downstream.subscribe_to(self.up2)
        self.downstream.subscribe_to(self.up3)

        # Provide test data for all upstreams
        self.up1._data.update({1.0: "a1", 2.0: "a2", 3.0: "a3"})
        self.up2._data.update({1.1: "b1", 2.1: "b2", 3.1: "b3"})
        self.up3._data.update({1.2: "c1", 2.2: "c2", 3.2: "c3"})

    def test_time_alignment(self):
        """Test time alignment of data from multiple sources"""
        datas, ts = self.downstream._get_from_upstream()
        # The algorithm will find the minimum value of the last timestamps, which is min(3.0, 3.1, 3.2) = 3.0
        # Then it will locate the data closest to 3.0 in each upstream.
        self.assertEqual(ts, 3.0)
        self.assertEqual(datas, ("a3", "b3", "c3"))

    def test_acceptable_time_bias_strict(self):
        """Test strict time tolerance range causing waiting"""
        self.downstream.acceptable_time_bias = 0.05
        t = threading.Thread(target=self.downstream._get_from_upstream, daemon=True)
        t.start()
        t.join(timeout=0.5)

        # If the thread is still running, it means it is waiting (this is the expected behavior)
        self.assertTrue(
            t.is_alive(), "Thread should still be running due to waiting for new data"
        )

    def test_acceptable_time_bias_loose(self):
        """Test loose time tolerance range"""
        self.downstream.acceptable_time_bias = 0.5
        datas, ts = self.downstream._get_from_upstream()
        self.assertEqual(ts, 3.0)
        self.assertEqual(datas, ("a3", "b3", "c3"))

    def test_data_order_consistency_after_new_data_arrives(self):
        # The first call will consume the latest data
        datas1, ts1 = self.downstream._get_from_upstream()
        self.assertEqual(ts1, 3.0)
        self.assertEqual(datas1, ("a3", "b3", "c3"))

        # During the second call, since the data has already been consumed, the upstream node's _last_downstream_gots will be updated
        # This means no new data is available unless we add new data
        self.up1._data[4.0] = "a4"
        self.up2._data[4.1] = "b4"
        self.up3._data[4.2] = "c4"

        # Notify that new data is available
        for up in [self.up1, self.up2, self.up3]:
            up._new_data_available.set()

        datas2, ts2 = self.downstream._get_from_upstream()
        self.assertEqual(ts2, 4.0)  # min(4.0, 4.1, 4.2) = 4.0
        self.assertEqual(datas2, ("a4", "b4", "c4"))
        self.assertGreater(ts2, ts1)
        self.assertNotEqual(datas1, datas2)


class TestNodeMultipleToMultiple(unittest.TestCase):
    """Test many-to-many pub/sub relationships and concurrent scenarios"""

    def setUp(self):
        self.upstreams = [Node(target=lambda _: f"up_{i}") for i in range(10)]
        self.downstreams = [Node(target=lambda x: f"down_{x}") for _ in range(10)]

    def test_concurrent_subscribe_unsubscribe(self):
        """Test the thread safety of concurrent subscribe/unsubscribe"""

        def subscribe_and_unsubscribe(downstream: Node):
            # Create randomness on number of concurrent subscribers
            chunk = randint(0, len(self.upstreams) - 1)
            for upstream in self.upstreams[:chunk]:
                downstream.subscribe_to(upstream)
                downstream.unsubscribe_from(upstream)
            for upstream in self.upstreams[chunk:]:
                downstream.subscribe_to(upstream)
                downstream.unsubscribe_from(upstream)

        threads = [
            threading.Thread(target=subscribe_and_unsubscribe, args=(downstream,))
            for downstream in self.downstreams
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify whether the final state is reasonable
        for i, downstream in enumerate(self.downstreams):
            with self.subTest(downstream=i):
                self.assertEqual(len(downstream._upstreams), 0)
        for i, upstream in enumerate(self.upstreams):
            with self.subTest(upstream=i):
                self.assertEqual(len(upstream._last_downstream_gots), 0)


class TestNodeEdgeCases(unittest.TestCase):
    """Test edge cases and error handling"""

    def test_no_upstreams(self):
        """Test behavior when there are no upstream nodes"""
        node = Node(target=lambda x: x)
        with self.assertRaises(ValueError):
            # An error should be raised when there are no upstream nodes
            node._get_from_upstream()

    def test_empty_upstream_data(self):
        """Test behavior when upstream nodes have no data"""
        upstream = Node(target=lambda _: "data")
        downstream = Node(target=lambda x: x)
        downstream.subscribe_to(upstream)

        # When upstream has no data, downstream queries should wait
        def add_data_later():
            time.sleep(0.1)
            upstream._data[1.0] = "test"
            upstream._new_data_available.set()

        t = threading.Thread(target=add_data_later)
        before_add_data_time = time.time()
        t.start()

        availables = upstream._query_availables(downstream, block=True)
        t.join()
        self.assertGreater(time.time() - before_add_data_time, 0.1)
        self.assertIn(1.0, availables)

    def test_unsubscribe_nonexistent(self):
        """Test unsubscribing from a nonexistent node"""
        node1 = Node(target=lambda x: x)
        node2 = Node(target=lambda x: x)

        # Attempting to unsubscribe from a node that was not subscribed should not be accepted
        with self.assertRaises((KeyError, ValueError)):
            node2.unsubscribe_from(node1)

    def test_duplicate_subscription(self):
        """Test that duplicate subscriptions raise an error"""
        upstream = Node(target=lambda _: "data")
        downstream = Node(target=lambda x: x)

        downstream.subscribe_to(upstream)

        # Duplicate subscriptions should raise ValueError
        with self.assertRaises(ValueError):
            downstream.subscribe_to(upstream)

        # Ensure there is only one subscription record
        self.assertEqual(len(upstream._last_downstream_gots), 1)
        self.assertEqual(len(downstream._upstreams), 1)


class TestNodeIncorrectConnection(unittest.TestCase):
    """Test behavior when nodes have incorrect upstream or downstream connections"""

    def test_no_upstream_no_type_hint_wait(self):
        """Test behavior when there are no upstream nodes and no type hint and wait for data"""
        strategy = [True, "warn_once", "warn_always"]
        for s in strategy:
            with self.subTest(strategy=s):
                node = Node(target=lambda x: x, wait_on_no_upstream=s)
                t = threading.Thread(
                    target=node._before_target,
                    name="test_no_upstream_no_type_hint_wait",
                    daemon=True,
                )
                t.start()
                t.join(timeout=0.5)
                self.assertTrue(
                    t.is_alive(), "Thread should be waiting for upstream data"
                )

    def test_no_upstream_no_type_hint_ignore(self):
        """Test behavior when there are no upstream nodes and no type hint and ignore"""
        node = Node(target=lambda x: x, wait_on_no_upstream="ignore")
        datas, ts = node._before_target()
        self.assertEqual(datas, tuple())
        self.assertIsInstance(ts, float)

    def test_no_upstream_no_type_hint_error(self):
        """Test behavior when there are no upstream nodes and no type hint and error"""
        node = Node(target=lambda x: x, wait_on_no_upstream="error")
        with self.assertRaises(ValueError):
            node._before_target()

    def test_no_upstream_with_type_hint_wait(self):
        """Test behavior when there are no upstream nodes but with type hint and wait for data"""
        strategy = [True, "warn_once", "warn_always"]
        for s in strategy:
            with self.subTest(strategy=s):
                node = Node[int, int](target=lambda x: x, wait_on_no_upstream=s)
                t = threading.Thread(
                    target=node._before_target,
                    name=f"test_no_upstream_with_type_hint_wait (strategy={s})",
                    daemon=True,
                )
                t.start()
                t.join(timeout=0.5)
                self.assertTrue(
                    t.is_alive(), "Thread should be waiting for upstream data"
                )

    def test_no_upstream_with_type_hint_ignore(self):
        """Test behavior when there are no upstream nodes but with type hint and ignore"""
        node = Node[int, int](target=lambda x: x, wait_on_no_upstream="ignore")
        datas, ts = node._before_target()
        self.assertEqual(datas, tuple())
        self.assertIsInstance(ts, float)

    def test_no_upstream_with_type_hint_error(self):
        """Test behavior when there are no upstream nodes but with type hint and error"""
        node = Node[int, int](target=lambda x: x, wait_on_no_upstream="error")
        with self.assertRaises(ValueError):
            node._before_target()

    def test_with_upstream_with_type_hint_wait(self):
        """Test behavior when there are insufficient upstream nodes and wait"""
        strategy = [True, "warn_once", "warn_always"]
        for s in strategy:
            with self.subTest(strategy=s):
                upstream = Node(target=lambda _: "data")
                downstream = Node[int, int, int](
                    target=lambda x, y: x, wait_on_no_upstream=s
                )
                downstream.subscribe_to(upstream)
                upstream._after_target("data", 0)
                t = threading.Thread(
                    target=downstream._before_target,
                    name=f"test_with_upstream_with_type_hint_wait (strategy={s})",
                    daemon=True,
                )
                t.start()
                t.join(timeout=0.5)
                self.assertTrue(
                    t.is_alive(), "Thread should be waiting for upstream data"
                )

    def test_with_upstream_with_type_hint_ignore(self):
        """Test behavior when there are insufficient upstream nodes and ignore"""
        upstream = Node(target=lambda _: 123)
        downstream = Node[int, int, int](
            target=lambda x, y: x, wait_on_no_upstream="ignore"
        )
        downstream.subscribe_to(upstream)
        upstream._after_target(123, 0)
        datas, ts = downstream._before_target()
        self.assertEqual(datas, (123,))
        self.assertEqual(ts, 0)

    def test_with_upstream_with_type_hint_error(self):
        """Test behavior when there are insufficient upstream nodes and error"""
        upstream = Node(target=lambda _: "data")
        downstream = Node[int, int, int](
            target=lambda x, y: x, wait_on_no_upstream="error"
        )
        downstream.subscribe_to(upstream)
        upstream._after_target("data", 0)
        with self.assertRaises(ValueError):
            downstream._before_target()


class TestSubscribeToWithTypeChecking(unittest.TestCase):
    """Test subscribe_to method with type checking validation"""

    # Define various node types for testing
    class SourceInt(Node[None, int]):
        """Outputs int type"""

        pass

    class SourceFloat(Node[None, float]):
        """Outputs float type"""

        pass

    class SourceStr(Node[None, str]):
        """Outputs str type"""

        pass

    class ProcessorIntFloat(Node[int, float, str]):
        """Expects int and float as upstream, outputs str"""

        pass

    class ProcessorIntInt(Node[int, int, str]):
        """Expects two ints as upstream, outputs str"""

        pass

    class ProcessorInt(Node[int, str]):
        """Expects only int as upstream, outputs str"""

        pass

    class ProcessorGeneric(Node):
        """Generic node without type specification"""

        pass

    class ProcessorOptionalInt(Node[Optional[int], str]):
        """Expects Optional[int] as upstream, outputs str"""

        pass

    class SourceOptionalInt(Node[None, Optional[int]]):
        """Outputs Optional[int] type"""

        pass

    def setUp(self) -> None:
        super().setUp()
        self.source_int = TestSubscribeToWithTypeChecking.SourceInt()
        self.source_float = TestSubscribeToWithTypeChecking.SourceFloat()
        self.source_str = TestSubscribeToWithTypeChecking.SourceStr()
        self.source_optional_int = TestSubscribeToWithTypeChecking.SourceOptionalInt()
        self.processor_int_float = TestSubscribeToWithTypeChecking.ProcessorIntFloat()
        self.processor_int_int = TestSubscribeToWithTypeChecking.ProcessorIntInt()
        self.processor_int = TestSubscribeToWithTypeChecking.ProcessorInt()
        self.processor_generic = TestSubscribeToWithTypeChecking.ProcessorGeneric()
        self.processor_optional_int = (
            TestSubscribeToWithTypeChecking.ProcessorOptionalInt()
        )

    def test_subscribe_matching_type_with_check_enabled(self):
        """Test subscribe_to succeeds when upstream output type matches expected input"""
        # ProcessorInt expects int from upstream
        # SourceInt outputs int
        # Should succeed
        self.processor_int.subscribe_to(self.source_int)
        self.assertIn(self.source_int, self.processor_int._upstreams)

    def test_subscribe_mismatched_type_with_check_enabled(self):
        """Test subscribe_to fails when upstream output type doesn't match expected input"""
        # ProcessorInt expects int from upstream
        # SourceFloat outputs float
        # Should raise ValueError due to type mismatch
        with self.assertRaises(ValueError):
            self.processor_int.subscribe_to(self.source_float)

    def test_subscribe_multiple_matching_types_with_check_enabled(self):
        """Test subscribe_to succeeds when multiple upstreams have matching types"""
        # ProcessorIntFloat expects (int, float) from upstreams
        # First subscribe to SourceInt (outputs int)
        self.processor_int_float.subscribe_to(self.source_int)
        self.assertIn(self.source_int, self.processor_int_float._upstreams)

        # Then subscribe to SourceFloat (outputs float)
        self.processor_int_float.subscribe_to(self.source_float)
        self.assertIn(self.source_float, self.processor_int_float._upstreams)

    def test_subscribe_wrong_type_in_multiple_upstreams_with_check_enabled(self):
        """Test subscribe_to fails when one of multiple upstreams has wrong type"""
        # ProcessorIntFloat expects (int, float) from upstreams
        # First subscribe to SourceInt (correct)
        self.processor_int_float.subscribe_to(self.source_int)

        # Then try to subscribe to SourceStr (wrong type, should fail)
        with self.assertRaises(ValueError):
            self.processor_int_float.subscribe_to(self.source_str)

    def test_subscribe_generic_upstream_node_always_allowed(self):
        """Test subscribe_to always allows generic upstream nodes (no type info available)"""
        # ProcessorInt expects specific types, but generic nodes should always be allowed
        # since we can't validate them
        self.processor_int.subscribe_to(self.processor_generic)
        self.assertIn(self.processor_generic, self.processor_int._upstreams)

    def test_subscribe_to_generic_processor_always_allowed(self):
        """Test subscribe_to on generic processor always allows any upstream"""
        # ProcessorGeneric has no type expectations
        # Should accept any upstream regardless of type
        self.processor_generic.subscribe_to(self.source_int)
        self.processor_generic.subscribe_to(self.source_float)
        self.processor_generic.subscribe_to(self.source_str)

        self.assertIn(self.source_int, self.processor_generic._upstreams)
        self.assertIn(self.source_float, self.processor_generic._upstreams)
        self.assertIn(self.source_str, self.processor_generic._upstreams)

    def test_subscribe_with_check_disabled_allows_mismatch(self):
        """Test subscribe_to with type checking disabled allows type mismatches"""
        # Create processor with type checking disabled
        processor_no_check = TestSubscribeToWithTypeChecking.ProcessorInt(
            wait_on_no_upstream="error", check_upstream_types=False
        )
        # Should succeed even though types don't match
        processor_no_check.subscribe_to(self.source_float)
        self.assertIn(self.source_float, processor_no_check._upstreams)

    def test_subscribe_multiple_upstreams_same_type(self):
        """Test subscribe_to allows multiple upstreams of the same type"""
        another_source_int = TestSubscribeToWithTypeChecking.SourceInt()
        self.processor_int_int.subscribe_to(self.source_int)
        self.processor_int_int.subscribe_to(another_source_int)

        self.assertIn(self.source_int, self.processor_int_int._upstreams)
        self.assertIn(another_source_int, self.processor_int_int._upstreams)

    def test_subscribe_optional_type_compatibility(self):
        """Test that type checking properly handles Optional types"""
        # Test 1: int upstream -> Optional[int] downstream
        # SHOULD PASS: int is compatible with Optional[int] (int is a subset of Union[int, None])
        self.processor_optional_int.subscribe_to(self.source_int)
        self.assertIn(self.source_int, self.processor_optional_int._upstreams)

        # Test 2: Optional[int] upstream -> int downstream
        # SHOULD FAIL: Optional[int] (Union[int, None]) should not match when only int is expected
        with self.assertRaises(ValueError):
            self.processor_int.subscribe_to(self.source_optional_int)


class TestDownstreamTypeDetection(unittest.TestCase):
    """Test detection of downstream type parameters in Node specializations"""

    class InheritedTyped(Node[int, str]):
        pass

    class InheritedGeneric(Node):
        pass

    DirectTyped = Node[int, str]
    DirectGeneric = Node

    def setUp(self) -> None:
        super().setUp()
        self.typed_nodes_check: list[Node] = [
            TestDownstreamTypeDetection.DirectTyped(check_downstream_type=True),
            TestDownstreamTypeDetection.InheritedTyped(check_downstream_type=True),
        ]
        self.typed_nodes_no_check: list[Node] = [
            TestDownstreamTypeDetection.DirectTyped(check_downstream_type=False),
            TestDownstreamTypeDetection.InheritedTyped(check_downstream_type=False),
        ]
        self.generic_nodes: list[Node] = [
            TestDownstreamTypeDetection.DirectGeneric(check_downstream_type=True),
            TestDownstreamTypeDetection.DirectGeneric(check_downstream_type=False),
            TestDownstreamTypeDetection.InheritedGeneric(check_downstream_type=True),
            TestDownstreamTypeDetection.InheritedGeneric(check_downstream_type=False),
        ]

    def test_downstream_type_detection(self):
        for node, match_type in itertools.product(
            self.typed_nodes_check, [True, False]
        ):
            with self.subTest(node=type(node), match_type=match_type):
                if match_type:
                    # Should put data in without error
                    node._after_target("test_string", 0.0)
                else:
                    # Should raise ValueError due to type mismatch
                    with self.assertRaises(ValueError):
                        node._after_target(12345, 0.0)
        for node, match_type in itertools.product(
            self.typed_nodes_no_check, [True, False]
        ):
            with self.subTest(node=type(node), match_type=match_type):
                # Type checking is disabled, so both should pass
                node._after_target("test_string" if match_type else 12345, 0.0)
        for node in self.generic_nodes:
            with self.subTest(node=type(node)):
                # Generic nodes should accept any data type
                node._after_target("test_string", 0.0)

    def test_optional_downstream_type_accepts_concrete_and_none(self):
        """Test that Optional[str] accepts both str and None values"""

        # Create a node that outputs Optional[str]
        class NodeWithOptionalDownstream(Node[int, Optional[str]]):
            pass

        node = NodeWithOptionalDownstream(check_downstream_type=True)

        # Should accept str value (str is part of Optional[str])
        node._after_target("test_string", 0.0)
        self.assertIn(0.0, node._data)

        # Should accept None value (None is part of Optional[str])
        node._after_target(None, 1.0)
        self.assertIn(1.0, node._data)

        # Should reject int value (int is not part of Optional[str])
        with self.assertRaises(ValueError):
            node._after_target(123, 2.0)


if __name__ == "__main__":
    unittest.main()
