import time
import unittest

from realtime_pipeline import Node
from realtime_pipeline.manager.progress import ProgressManager


class BasicProgressFunctions(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.pm = ProgressManager()

    def test_start_stop(self):
        self.pm.start()
        self.assertTrue(self.pm._progress.live.is_started)
        self.pm.stop()
        self.assertFalse(self.pm._progress.live.is_started)
        with self.pm:
            self.assertTrue(self.pm._progress.live.is_started)
        self.assertFalse(self.pm._progress.live.is_started)

    def test_ignore_on_non_registered_node(self):
        node = Node()
        with self.pm:
            self.pm.update_progress(node=node, process_timestamp=time.time())
        self.assertEqual(len(self.pm._nodes_info), 0)
        self.assertEqual(len(self.pm._progress.tasks), 0)

    def test_save_registered_node(self):
        node = Node()
        self.pm.add_node(node)
        self.assertEqual(len(self.pm._nodes_info), 1)
        self.assertEqual(len(self.pm._nodes_info[(node.name, node)].timestamp), 0)
        now = time.time()
        with self.pm:
            self.pm.update_progress(node=node, process_timestamp=now)
        self.assertEqual(len(self.pm._nodes_info[(node.name, node)].timestamp), 1)
        self.assertEqual(self.pm._nodes_info[(node.name, node)].timestamp[0], now)

    def test_calculate_all_data_within_timeframe(self):
        node = Node()
        self.pm.add_node(node)
        now = time.time()
        with self.pm:
            self.pm.update_progress(node=node, process_timestamp=now - 3)
            self.pm.update_progress(node=node, process_timestamp=now - 2.0)
            self.pm.update_progress(node=node, process_timestamp=now - 1.5)
            self.pm.update_progress(node=node, process_timestamp=now - 1.0)
            self.pm.update_progress(node=node, process_timestamp=now - 0.5)
            self.assertEqual(len(self.pm._nodes_info[(node.name, node)].timestamp), 4)
            self.assertListEqual(
                self.pm._nodes_info[(node.name, node)].timestamp,
                [now - 2.0, now - 1.5, now - 1.0, now - 0.5],
            )

    def test_calculate_all_data_when_too_little_data(self):
        node = Node()
        self.pm.add_node(node)
        now = time.time()
        with self.pm:
            self.pm.update_progress(node=node, process_timestamp=now - 10)
            self.pm.update_progress(node=node, process_timestamp=now - 5)
            self.pm.update_progress(node=node, process_timestamp=now)
            self.assertEqual(len(self.pm._nodes_info[(node.name, node)].timestamp), 3)
            self.assertListEqual(
                self.pm._nodes_info[(node.name, node)].timestamp,
                [now - 10, now - 5, now],
            )
