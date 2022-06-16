import logging
from typing import Callable

logger = logging
logger.basicConfig(format=' %(levelname)s %(asctime)s (%(pathname)s - %(funcName)s - %(message)s):', level=logging.INFO)



def log_execution(fun:Callable) -> Callable:
    """
    Decorator to log the execution of arbitrary functions
    in the current log file
    """
    def wrapper(*args, **kwargs):
        logger.info(f"Calling function {fun.__name__} with arguments {args} and kw_args {kwargs}")
        result = fun(*args, **kwargs)
        logger.info(f"Function call returned {result}")
        return result

    return wrapper


