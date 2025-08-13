import threading
from bisect import bisect_left
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Mapping,
    Optional,
    TypeVar,
)

from deprecation import deprecated
from readerwriterlock import rwlock
from sortedcontainers import SortedDict
from typing_extensions import TypeAlias, TypeVarTuple, Unpack

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
    ) -> None:
        super().__init__(name=name, args=args, kwargs=kwargs, daemon=daemon)
        # data access
        self._data_lock = rwlock.RWLockFair()
        self._data = SortedDict()
        self._new_data_available = threading.Event()
        """ Timestamp -> Data """

        # node connections
        self._last_downstream_gots: dict[Node, Timestamp] = {}
        self._subscribe_lock = threading.Lock()
        self._upstreams: list[Node] = []
        self.acceptable_time_bias = acceptable_time_bias

        # job
        self.target = target

    @deprecated(deprecated_in="0.3.0", details="Use `subscribe_to` instead.")
    def subscribe(self, subscriber: "Node"):
        """Downstream node subscribe to this node and receive data from this node."""
        with self._subscribe_lock, subscriber._subscribe_lock:
            if subscriber in self._last_downstream_gots:
                raise ValueError(
                    f"Subscriber {subscriber} already subscribed to {self}"
                )
            self._last_downstream_gots[subscriber] = -1
            subscriber._upstreams.append(self)

    def subscribe_to(self, upstream_node: "Node"):
        """Subscribes this node to a upstream_node"""
        with self._subscribe_lock, upstream_node._subscribe_lock:
            if upstream_node in self._upstreams:
                raise ValueError(f"Node {self} already subscribed to {upstream_node}")
            upstream_node._last_downstream_gots[self] = -1
            self._upstreams.append(upstream_node)

    @deprecated(deprecated_in="0.3.0", details="Use `unsubscribe_from` instead.")
    def unsubscribe(self, subscriber: "Node"):
        """Downstream node unsubscribe from this node."""
        with self._subscribe_lock, subscriber._subscribe_lock:
            self._last_downstream_gots.pop(subscriber)
            subscriber._upstreams.remove(self)

    def unsubscribe_from(self, upstream_node: "Node"):
        """Unsubscribes this node from a upstream_node"""
        with self._subscribe_lock, upstream_node._subscribe_lock:
            if upstream_node not in self._upstreams:
                raise ValueError(f"Node {self} not subscribed to {upstream_node}")
            upstream_node._last_downstream_gots.pop(self)
            self._upstreams.remove(upstream_node)

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
        # The timestamps of processed data available from upstream nodes
        while True:
            availables = [up._query_availables(self) for up in self._upstreams]
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

    # Retrieve data from upstream, process it, and clean up outdated data
    def run(self):
        if not callable(self.target):
            raise ValueError(
                "Target must be a callable function when initializing or `run` must be overridden."
            )
        while True:
            datas, timestamp = self._get_from_upstream()

            # ...perform the tasks this node is supposed to do...
            if self.target is None:
                raise NotImplementedError(
                    f"Nothing specified to run in this node {self.name}."
                    "Please either set a target function when initializing or override the `run` method."
                )
            result = self.target(*datas)

            self._data[timestamp] = result
            self._new_data_available.set()
            self._cleanup_old_data()

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
