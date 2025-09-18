"""Module "progress"
Put all Nodes in a single manager and track their progress on screen
This module requires `progress` extra optional group
"""

import threading
import time
from bisect import bisect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Type

try:
    from rich.progress import BarColumn, Progress, TaskID, TextColumn
except ImportError as e:
    raise ImportError(
        "Please install 'progress' extra of realtime-pipeline.\n"
        "You can do something like `pip install 'realtime-pipeline[progress]'`"
    ) from e

if TYPE_CHECKING:
    from types import TracebackType

    from realtime_pipeline.realtime_pipeline import Node


@dataclass
class _NodeInfo:
    task_id: TaskID
    timestamp: list[float]


class ProgressManager:
    def __init__(self) -> None:
        self._nodes_info: dict[tuple[str, "Node"], _NodeInfo] = {}
        self._progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=20),
            TextColumn("[cyan]{task.fields[rate]} Hz"),
            "•",
            TextColumn("[yellow]{task.fields[lag]}ms lag"),
            "•",
            TextColumn("[green]{task.fields[timestamp]}"),
            refresh_per_second=4,
        )
        self._data_lock = threading.Lock()
        self._render_lock = threading.Lock()
        self._start_time = time.time()

    def add_node(self, node: "Node", name: Optional[str] = None):
        if name is None:
            name = node.name
        task_id = self._progress.add_task(  # There's an RLock in Rich already
            name,
            total=100,  # 100 means the task is the most responsive one, and will get a green bar
            rate="N/A",
            lag="N/A",
            timestamp="N/A",
        )
        with self._data_lock:
            self._nodes_info[(name, node)] = _NodeInfo(task_id=task_id, timestamp=[])
        node.progress_manager = self

    def remove_node(self, node: "Node", name: Optional[str] = None):
        if name is None:
            name = node.name

        node.progress_manager = None

        # ignore if node not found
        try:
            self._progress.remove_task(
                self._nodes_info[(name, node)].task_id
            )  # There's an RLock in Rich already
        except KeyError:
            pass

        with self._data_lock:
            try:
                del self._nodes_info[(name, node)]
            except KeyError:
                pass

    def update_progress(
        self, *, name: Optional[str] = None, node: "Node", process_timestamp: float
    ):
        if name is None:
            name = node.name

        with self._data_lock:
            if (name, node) not in self._nodes_info:
                # just ignore
                return
            node_info = self._nodes_info[(name, node)]

            now = time.time()

            # Calculate speed of process within 2.5 seconds or last 3 timestamps, whichever is more
            cutoff_time = now - 2.5
            cutoff_index = bisect(node_info.timestamp, cutoff_time)
            if len(node_info.timestamp) - cutoff_index < 3:
                cutoff_index = max(0, len(node_info.timestamp) - 3)

            node_info.timestamp = node_info.timestamp[cutoff_index:]

            # Record timestamp
            node_info.timestamp.append(now)

        # stop if not started
        if not self._progress.live.is_started:
            return

        # Calculate process speed (Hz)
        if len(node_info.timestamp) > 1:
            time_span = node_info.timestamp[-1] - node_info.timestamp[0]
            rate = (len(node_info.timestamp) - 1) / time_span
        else:
            rate = 0.0

        # Calculate delay
        lag_ms = (now - process_timestamp) * 1000

        # Calculate relative progress (The slowest is 0%, while the fastest is 100%)
        all_rates = []
        with self._data_lock:
            for info in self._nodes_info.values():
                if len(info.timestamp) > 1:
                    time_span = info.timestamp[-1] - info.timestamp[0]
                    node_rate = (len(info.timestamp) - 1) / time_span
                    all_rates.append(node_rate)

        if all_rates:
            min_rate = min(all_rates)
            max_rate = max(all_rates)
            if max_rate > min_rate:
                progress_percent = (rate - min_rate) / (max_rate - min_rate) * 100
            else:
                progress_percent = 100  # Show 100% if all nodes are at the same speed
        else:
            progress_percent = 0

        with self._render_lock:
            try:
                self._progress.update(
                    node_info.task_id,
                    completed=progress_percent,
                    rate=f"{rate:.1f}",
                    lag=f"{lag_ms:.0f}",
                    timestamp=f"{process_timestamp - self._start_time:.3f}",
                )
            except KeyError:  # Possible update after delete task
                pass

    def start(self):
        """Start the progress display."""
        self._start_time = time.time()
        self._progress.start()

    def __enter__(self):
        self.start()
        return self

    def stop(self):
        """Stop the progress display."""
        self._progress.stop()

    @property
    def started(self) -> bool:
        return self._progress.live.is_started

    @started.setter
    def started(self, value: bool):
        current_state = self.started
        if value and not current_state:
            self.start()
        elif not value and current_state:
            self.stop()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional["TracebackType"],
    ):
        self.stop()
