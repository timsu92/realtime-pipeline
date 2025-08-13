from importlib.metadata import version

from realtime_pipeline.realtime_pipeline import Node

__all__ = ["Node", "__version__"]
__version__ = version("realtime_pipeline")
