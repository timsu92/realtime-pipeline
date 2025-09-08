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
        self.assertAlmostEqual(
            self.pm._nodes_info[(node.name, node)].timestamp[0], now, delta=0.01
        )

    def test_calculate_all_data_within_timeframe(self):
        node = Node()
        self.pm.add_node(node)
        with self.pm:
            self.pm.update_progress(node=node, process_timestamp=time.time())  # 2.6s
            time.sleep(0.6)
            self.pm.update_progress(node=node, process_timestamp=time.time())  # 2.0s
            time.sleep(0.5)
            self.pm.update_progress(node=node, process_timestamp=time.time())  # 1.5s
            time.sleep(0.5)
            self.pm.update_progress(node=node, process_timestamp=time.time())  # 1.0s
            time.sleep(1)
            self.pm.update_progress(node=node, process_timestamp=time.time())  # 0s
            self.assertEqual(len(self.pm._nodes_info[(node.name, node)].timestamp), 4)
            now = time.time()
            self.assertAlmostEqual(
                self.pm._nodes_info[(node.name, node)].timestamp[0],
                now - 2.0,
                delta=0.01,
            )
            self.assertAlmostEqual(
                self.pm._nodes_info[(node.name, node)].timestamp[1],
                now - 1.5,
                delta=0.01,
            )
            self.assertAlmostEqual(
                self.pm._nodes_info[(node.name, node)].timestamp[2],
                now - 1.0,
                delta=0.01,
            )
            self.assertAlmostEqual(
                self.pm._nodes_info[(node.name, node)].timestamp[3],
                now - 0.0,
                delta=0.01,
            )

    def test_calculate_all_data_when_too_little_data(self):
        node = Node()
        self.pm.add_node(node)
        with self.pm:
            self.pm.update_progress(node=node, process_timestamp=time.time())  # 2.6s
            self.pm.update_progress(node=node, process_timestamp=time.time())  # 2.6s
            time.sleep(2.6)
            self.pm.update_progress(node=node, process_timestamp=time.time())  # 0s
            self.assertEqual(len(self.pm._nodes_info[(node.name, node)].timestamp), 3)
            now = time.time()
            self.assertAlmostEqual(
                self.pm._nodes_info[(node.name, node)].timestamp[0],
                now - 2.6,
                delta=0.01,
            )
            self.assertAlmostEqual(
                self.pm._nodes_info[(node.name, node)].timestamp[1],
                now - 2.6,
                delta=0.01,
            )
            self.assertAlmostEqual(
                self.pm._nodes_info[(node.name, node)].timestamp[2],
                now - 0.0,
                delta=0.01,
            )

    def test_remove_nonexist_node(self):
        self.pm.remove_node(Node())  # should not raise

    def test_add_node(self):
        node = Node(name="node name")
        self.pm.add_node(node)
        self.assertEqual(len(self.pm._nodes_info), 1)
        self.assertIn((node.name, node), self.pm._nodes_info.keys())
        self.assertEqual(len(self.pm._progress.tasks), 1)
        self.assertIn(
            self.pm._nodes_info[(node.name, node)].task_id, self.pm._progress.task_ids
        )
        self.assertIs(node.progress_manager, self.pm)

    def test_remove_exist_node(self):
        node = Node(name="node name")
        self.pm.add_node(node)
        self.pm.remove_node(node)
        self.assertEqual(len(self.pm._nodes_info), 0)
        self.assertEqual(len(self.pm._progress.tasks), 0)
        self.assertIsNone(node.progress_manager)
