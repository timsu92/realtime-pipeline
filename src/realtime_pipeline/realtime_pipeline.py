import logging
import threading
import time
import weakref
from bisect import bisect_left
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterable,
    Literal,
    Mapping,
    Optional,
    TypeVar,
)

from deprecation import deprecated
from readerwriterlock import rwlock
from sortedcontainers import SortedDict
from typing_extensions import TypeAlias, TypeVarTuple, Unpack

if TYPE_CHECKING:
    from realtime_pipeline.manager.progress import ProgressManager

from realtime_pipeline.utils.typings import (
    node_downstream_from_instance,
    node_upstream_from_instance,
)

Timestamp: TypeAlias = float

UpstreamT = TypeVarTuple("UpstreamT")
DownstreamT = TypeVar("DownstreamT")


class Node(Generic[Unpack[UpstreamT], DownstreamT], threading.Thread):
    def __init__(
        self,
        acceptable_time_bias=1.5,
        target: Optional[Callable[[Unpack[UpstreamT]], DownstreamT]] = None,
        args: Iterable[Any] = (),
        kwargs: Optional[Mapping[str, Any]] = None,
        *,
        name: Optional[str] = None,
        daemon: Optional[bool] = None,
        progress_manager: Optional["ProgressManager"] = None,
        wait_on_no_upstream: Literal[
            True, "warn_once", "warn_always", "ignore", "error"
        ] = None,  # for deprecation in the future
        check_upstream_types: bool = True,
        check_downstream_type: bool = False,
    ) -> None:
        """A processing node in the realtime data pipeline.

        Args:
            acceptable_time_bias (float): The maximum acceptable time difference for aligning data from upstream nodes.
            target (Callable): The processing function for this node. Can be supplied by overriding the `run` method instead as well. Please check the implementation of `run` method for more details.
            args (Iterable): Positional arguments for the target function.
            kwargs (Mapping): Keyword arguments for the target function.
            name (str): Name of the thread and logging functions.
            daemon (bool): Whether this thread is a daemon thread.
            progress_manager (ProgressManager): Optional progress manager to report progress to.
            wait_on_no_upstream (bool | str | None): Policy when upstream nodes are missing or when upstream data is not available.
                Acceptable values and behaviors:
                - True: Block and wait indefinitely for upstream data.
                - "warn_once": Log a warning once, then wait for upstream data.
                - "warn_always": Log a warning each time and wait for upstream data.
                - "ignore": Do not wait; proceed immediately (node will receive empty input and use the current time).
                - "error": Raise a ValueError immediately if required upstream nodes or data are missing.
                Notes:
                - Passing None currently triggers a deprecation warning; callers should set this explicitly.
                - Use the most appropriate policy for your pipeline semantics (e.g. "ignore" for best-effort or "error" for strict guarantees).
            check_upstream_types (bool): Whether to validate that subscribed upstream nodes output types are compatible with expected upstream types. Defaults to True.
            check_downstream_type (bool): Whether to validate that data passing to downstream matches in runtime. Defaults to False.
        """
        super().__init__(name=name, args=args, kwargs=kwargs, daemon=daemon)
        # data access
        self._data_lock = rwlock.RWLockFair()
        self._data = SortedDict[Timestamp, DownstreamT]()
        self._new_data_available = threading.Event()

        # node connections
        self._last_downstream_gots: dict[Node, Timestamp] = {}
        self._subscribe_lock = threading.Lock()
        self._upstreams: list[Node] = []
        self._has_upstreams_event = threading.Event()
        self.acceptable_time_bias = acceptable_time_bias
        self.check_upstream_types = check_upstream_types
        self.check_downstream_type = check_downstream_type
        # TODO: deprecate this in future releases
        if wait_on_no_upstream is None:
            logging.warning(
                DeprecationWarning(
                    "`wait_on_no_upstream` should be set explicitly.\n"
                    "Currently defaulting to `error`, but will be `True` in future releases."
                )
            )
            self.wait_on_no_upstream = "error"
        else:
            self.wait_on_no_upstream = wait_on_no_upstream
        self.__warn_no_upstream_shown = False

        # job
        self.target = target

        # context manager
        self._progress_manager_ref = (
            weakref.ref(progress_manager) if progress_manager else None
        )

    @cached_property
    def _expected_upstream(self):
        return node_upstream_from_instance(self)

    @cached_property
    def _expected_downstream(self):
        return node_downstream_from_instance(self)

    @property
    def progress_manager(self) -> Optional["ProgressManager"]:
        """Get the progress manager, if still alive"""
        return self._progress_manager_ref() if self._progress_manager_ref else None

    @progress_manager.setter
    def progress_manager(self, value: Optional["ProgressManager"]) -> None:
        """Set the progress manager using weak reference"""
        self._progress_manager_ref = weakref.ref(value) if value else None

    @deprecated(deprecated_in="0.3.0", details="Use `subscribe_to` instead.")
    def subscribe(self, subscriber: "Node"):
        """Downstream node subscribe to this node and receive data from this node."""
        return subscriber.subscribe_to(self)

    def subscribe_to(self, upstream_node: "Node"):
        """Subscribes this node to a upstream_node

        If check_upstream_types is enabled, validates that the upstream node's output type
        is compatible with one of the expected upstream types for this node.

        Raises:
            ValueError: If upstream node is already subscribed or if type checking fails.
        """
        with self._subscribe_lock, upstream_node._subscribe_lock:
            if upstream_node in self._upstreams:
                raise ValueError(f"Node {self} already subscribed to {upstream_node}")

            # Type checking if enabled
            if self.check_upstream_types:
                expected_upstream = self._expected_upstream
                # Only perform type checking if this node has expected upstream types
                if expected_upstream is not None:
                    upstream_output_type = node_downstream_from_instance(upstream_node)
                    # Only check if upstream node has defined output type
                    if upstream_output_type is not None:
                        # Check if upstream output type is in expected upstream types
                        if upstream_output_type not in expected_upstream:
                            raise ValueError(
                                f"Node {upstream_node} outputs type {upstream_output_type}, "
                                f"but {self} expects types {expected_upstream}"
                            )

            upstream_node._last_downstream_gots[self] = -1
            self._upstreams.append(upstream_node)
            self._has_upstreams_event.set()

    @deprecated(deprecated_in="0.3.0", details="Use `unsubscribe_from` instead.")
    def unsubscribe(self, subscriber: "Node"):
        """Downstream node unsubscribe from this node."""
        return subscriber.unsubscribe_from(self)

    def unsubscribe_from(self, upstream_node: "Node"):
        """Unsubscribes this node from a upstream_node"""
        with self._subscribe_lock, upstream_node._subscribe_lock:
            if upstream_node not in self._upstreams:
                raise ValueError(f"Node {self} not subscribed to {upstream_node}")
            upstream_node._last_downstream_gots.pop(self)
            self._upstreams.remove(upstream_node)
            if not self._upstreams:
                self._has_upstreams_event.clear()

    def _query_availables(self, downstream: "Node", block=True) -> list[Timestamp]:
        """Tries to query available data timestamps for the downstream node.
        If `block` is True, it will return with currently available timestamps or block until one is available.
        If `block` is False, it will return immediately with available timestamps or an empty list.
        """
        read_lock = self._data_lock.gen_rlock()
        read_lock.acquire()
        start_index = self._data.bisect_right(self._last_downstream_gots[downstream])
        if block and start_index == len(self._data):
            self._new_data_available.clear()
            read_lock.release()
            self._new_data_available.wait()
            return self._query_availables(downstream)
        availables: list[Timestamp] = list(self._data.islice(start_index, None))
        read_lock.release()
        return availables

    def _give_data(self, timestamp: Timestamp, subscriber: "Node") -> DownstreamT:
        # thread-safe
        data = self._data[timestamp]
        self._last_downstream_gots[subscriber] = timestamp
        return data

    def _get_from_upstream(self) -> tuple[tuple[Unpack[UpstreamT]], Timestamp]:
        while True:
            # The timestamps of processed data available from upstream nodes
            availables = [up._query_availables(self) for up in self._upstreams]

            ### Check on received data from upstream nodes ###

            if self._expected_upstream is None:
                if len(availables) == 0:
                    msg = f"Node {self} has no upstream nodes, but requested to get from upstream."
                    if self.wait_on_no_upstream == "error":
                        raise ValueError(msg)
                    elif (
                        self.wait_on_no_upstream == "warn_once"
                        and not self.__warn_no_upstream_shown
                    ) or self.wait_on_no_upstream == "warn_always":
                        logging.warning(RuntimeWarning(msg))
                        self.__warn_no_upstream_shown = True
                    elif self.wait_on_no_upstream == "ignore":
                        # No data available, return empty data with current timestamp
                        return (tuple(), time.time())
                    # Both warning and True will wait for upstream data
                    self._has_upstreams_event.wait()
                    continue
                # len(availables) > 0, proceed normally
            elif len(availables) < len(self._expected_upstream):
                msg = f"Node {self} got only {len(availables)} upstream nodes, expected {len(self._expected_upstream)}"
                if self.wait_on_no_upstream == "error":
                    raise ValueError(msg)
                elif self.wait_on_no_upstream in ["warn_once", "warn_always", True]:
                    if (
                        self.wait_on_no_upstream == "warn_once"
                        and not self.__warn_no_upstream_shown
                    ) or self.wait_on_no_upstream == "warn_always":
                        logging.warning(RuntimeWarning(msg))
                        self.__warn_no_upstream_shown = True
                    if len(self._upstreams) == 0:
                        self._has_upstreams_event.wait()
                    else:
                        self._wait_for_any_upstream_data()
                    continue
                elif self.wait_on_no_upstream == "ignore" and len(availables) == 0:
                    # No data available, return empty data with current timestamp
                    return (tuple(), time.time())
            # if len(availables) == len(self._expected_upstream) or ignore with some data available, proceed normally

            ### Process normally ###

            # Find the earliest timestamp among the latest timestamps from all upstream nodes
            min_of_latests = min(available_times[-1] for available_times in availables)
            # Find the closest timestamp to this time point in each upstream node
            nearest_index = [bisect_left(c, min_of_latests) for c in availables]
            for i, (cand, idx) in enumerate(zip(availables, nearest_index)):
                if (
                    idx != 0
                    and min_of_latests - cand[idx - 1] < cand[idx] - min_of_latests
                ):  # Both subtraction results should be >= 0?
                    nearest_index[i] -= 1

            # If any upstream node does not have data close enough to this timestamp, wait for new data and retry
            if any(
                abs(avai[idx] - min_of_latests) > self.acceptable_time_bias
                for avai, idx in zip(availables, nearest_index)
            ):
                # Wait for new data from any upstream node
                self._wait_for_any_upstream_data()
                continue

            # Time alignment successful, retrieve data
            datas = tuple(
                up._give_data(avai[idx], self)
                for up, avai, idx in zip(self._upstreams, availables, nearest_index)
            )
            return datas, min_of_latests

    def _cleanup_old_data(self):
        """Should be called by `run` function only. Not thread-safe"""
        if not self._last_downstream_gots:
            return  # No downstream subscribers, no need to clean up

        with self._data_lock.gen_wlock():
            threshold = min(self._last_downstream_gots.values())
            while len(self._data) > 0 and self._data.keys()[0] <= threshold:
                self._data.popitem(0)

    def _before_target(self):
        return self._get_from_upstream()

    def _after_target(self, result: DownstreamT, timestamp: Timestamp):
        if self.check_downstream_type:
            expected_downstream = self._expected_downstream
            if expected_downstream is not None:
                if not isinstance(result, expected_downstream):
                    raise ValueError(
                        f"Node {self} produced data of type {type(result)}, "
                        f"but expected type {expected_downstream}"
                    )
        self._data[timestamp] = result
        self._new_data_available.set()
        self._cleanup_old_data()
        if self.progress_manager:
            self.progress_manager.update_progress(
                node=self, process_timestamp=timestamp
            )

    # Retrieve data from upstream, process it, and clean up outdated data
    def run(self):
        if not callable(self.target):
            raise ValueError(
                "Target must be a callable function when initializing or `run` must be overridden."
            )
        while True:
            datas, timestamp = self._before_target()

            # ...perform the tasks this node is supposed to do...
            result = self.target(*datas)

            self._after_target(result, timestamp)

    def _wait_for_any_upstream_data(self):
        """Wait for new data to be available from any upstream node"""
        if not len(self._upstreams):
            raise ValueError("No upstream nodes available")

        # Create waiting threads to monitor new data from any upstream node
        threads = []
        result_event = threading.Event()

        def upstream_watcher(upstream: "Node"):
            upstream._new_data_available.clear()
            upstream._new_data_available.wait()
            result_event.set()  # Set the result event when new data is available from any upstream node

        # Create monitoring threads for each upstream node
        for upstream in self._upstreams:
            t = threading.Thread(target=upstream_watcher, args=(upstream,), daemon=True)
            t.start()
            threads.append(t)

        # Wait for new data from any upstream node
        result_event.wait()
