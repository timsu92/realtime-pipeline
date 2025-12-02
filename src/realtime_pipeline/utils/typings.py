import inspect
from typing import Any, Optional, get_args, get_origin


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
