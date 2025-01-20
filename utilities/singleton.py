import threading
import logging

class SingletonMeta(type):
    """
    This is a thread-safe implementation of Singleton using a metaclass.
    """
    _instances = {}
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
                    #logging.getLogger('admin_logger').debug(f'Created a new instance of {cls.__name__}.')
        #else:
            #logging.getLogger('admin_logger').debug(f'Using existing instance of {cls.__name__}.')
        return cls._instances[cls]