import inspect
from typing import Any, Optional, Union, get_args, get_origin


def is_type_compatible(actual_type: type, expected_type: type) -> bool:
    """Check if actual_type is compatible with expected_type.

    Args:
        actual_type: The type that is actually provided (e.g., from upstream node)
        expected_type: The type that is expected (e.g., by downstream node)

    Returns:
        True if actual_type is compatible with expected_type, False otherwise.

    Compatibility rules:
    - Direct equality: int == int
    - Concrete type to Optional: int is compatible with Optional[int]
      (because int can be used where Optional[int] is expected)
    - Optional to concrete: Optional[int] is NOT compatible with int
      (because Optional[int] might be None)
    - Generic types: NDArray[np.uint8] is compatible with NDArray
      (specialized type is compatible with generic type if origins match)
    """
    # Direct equality
    if actual_type == expected_type:
        return True

    # Check if expected_type is Optional (Union with None)
    expected_origin = get_origin(expected_type)
    if expected_origin is Union:
        expected_args = get_args(expected_type)
        # Optional[X] is Union[X, None] or Union[X, type(None)]
        # Check if actual_type is one of the non-None types in the Union
        if type(None) in expected_args:
            # This is Optional or a Union containing None
            non_none_types = [t for t in expected_args if t is not type(None)]
            # actual_type is compatible if it is compatible with one of the non-None types
            if any(is_type_compatible(actual_type, t) for t in non_none_types):
                return True

    # Check if actual_type is Optional (Union with None)
    actual_origin = get_origin(actual_type)
    if actual_origin is Union:
        actual_args = get_args(actual_type)
        # If actual is Optional but expected is concrete, not compatible
        # (Optional[int] cannot be used where int is expected, might be None)
        if type(None) in actual_args and expected_origin is not Union:
            return False

    # Generic type compatibility: if both have the same origin, consider them compatible
    # e.g., NDArray[np.uint8] (origin: ndarray) is compatible with NDArray (origin: ndarray)
    # This allows specialized generic types to be used where generic types are expected

    # Currently, there's no nice way to deep check generic parameters for full compatibility
    # NDArray[np.uint8] vs NDArray[str] would still be considered compatible here
    if actual_origin is not None and expected_origin is not None:
        if actual_origin == expected_origin:
            return True

    # If actual_type has a generic origin but expected_type doesn't,
    # check if the origin matches the expected_type
    # e.g., NDArray[np.uint8] (origin: ndarray) is compatible with ndarray
    if actual_origin is not None and expected_origin is None:
        if actual_origin == expected_type:
            return True

    return False


def _find_param_tuple_from_class(
    cls: type, target_type: type
) -> Optional[tuple[Any, ...]]:
    """
    Return the tuple of type arguments used to specialize targetType in cls's bases.
    e.g. for class Child(Parent[int, str]): return (int, str).
    If not found or not specialized, return None.
    """
    # Check if cls itself is a generic alias (e.g., Node[int, str])
    origin = get_origin(cls)
    if origin is target_type:
        return get_args(cls)

    # Look through MRO to find where targetType appears with concrete params
    for base in getattr(cls, "__orig_bases__", ()):
        origin = get_origin(base)
        if origin is target_type:
            return get_args(base)

    # Fallback: sometimes __orig_bases__ is not present; try annotations of bases
    # Only use getmro if cls is an actual class (not a generic alias)
    if isinstance(cls, type):
        for base in inspect.getmro(cls):
            if base is target_type:
                # Not specialized at this level
                return None
            # If this mro entry is itself a typing alias of targetType[...]
            origin = get_origin(base)
            if origin is target_type:
                return get_args(base)

    return None


def node_upstream_from_class(cls: type):
    """
    Determine the upstream type parameters (UpstreamT) used in the Node specialization for cls.
    Returns UpstreamT if known, else None.
    """
    from realtime_pipeline import Node

    param_tuple = _find_param_tuple_from_class(cls, Node)
    if param_tuple is None:
        return None
    # Node[*UpstreamT, DownstreamT] => Total parameters - 1 (last one is DownstreamT)
    return param_tuple[:-1]


def node_upstream_from_instance(obj: object):
    """
    Try to read obj.__orig_class__ to get the concrete type args used for this instance.
    Returns UpstreamT if known, else None.
    """
    from realtime_pipeline import Node

    orig = getattr(obj, "__orig_class__", None)
    if orig is None:
        # maybe try from the class
        return node_upstream_from_class(obj.__class__)

    origin = get_origin(orig)
    if origin is not Node:
        # If obj is a subclass instance, obj.__orig_class__ might be SubClass[...] instead of Node[...]
        # Look up the MRO to find the concrete specialization of Node[...]
        return node_upstream_from_class(obj.__class__)

    args = get_args(orig)
    if not args:
        return None
    # Node[*UpstreamT, DownstreamT] => Total parameters - 1 (last one is DownstreamT)
    return args[:-1]


def node_downstream_from_class(cls: type):
    """
    Determine the downstream type parameter (DownstreamT) used in the Node specialization for cls.
    Returns DownstreamT if known, else None.
    """
    from realtime_pipeline import Node

    param_tuple = _find_param_tuple_from_class(cls, Node)
    if param_tuple is None:
        return None
    # Node[*UpstreamT, DownstreamT] => Last parameter is DownstreamT
    return param_tuple[-1]


def node_downstream_from_instance(obj: object):
    """
    Try to read obj.__orig_class__ to get the concrete type args used for this instance.
    Returns DownstreamT if known, else None.
    """
    from realtime_pipeline import Node

    orig = getattr(obj, "__orig_class__", None)
    if orig is None:
        # maybe try from the class
        return node_downstream_from_class(obj.__class__)

    origin = get_origin(orig)
    if origin is not Node:
        # If obj is a subclass instance, obj.__orig_class__ might be SubClass[...] instead of Node[...]
        # Look up the MRO to find the concrete specialization of Node[...]
        return node_downstream_from_class(obj.__class__)

    args = get_args(orig)
    if not args:
        return None
    # Node[*UpstreamT, DownstreamT] => Last parameter is DownstreamT
    return args[-1]
