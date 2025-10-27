from ..file.file_cache import FileCache
from .path_constants import PathConstants

_cached_cache = None

def get_cache():
    """
    Returns a cached instance of the FileCache.
    """
    global _cached_cache
    if _cached_cache is None:
        _cached_cache = FileCache(PathConstants.FILE_CACHE_PATH)
    return _cached_cache
