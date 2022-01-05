import logging
from datetime import datetime


class bcolors:
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    END = "\033[0m"


class LogService:
    def __init__(self) -> None:  # pragma: no cover
        self.logger = logging.getLogger("uvicorn.error")
        self.logger.setLevel(logging.DEBUG)

    def log_debug(self, message: str):
        self.logger.debug(
            f"{bcolors.CYAN}{datetime.utcnow().isoformat()}:\
 {message}{bcolors.END}"
        )

    def log_information(self, message: str):
        self.logger.info(
            f"{bcolors.GREEN}{datetime.utcnow().isoformat()}:\
 {message}{bcolors.END}"
        )

    def log_warning(self, message: str):
        self.logger.warning(
            f"{bcolors.YELLOW}{datetime.utcnow().isoformat()}:\
 {message}{bcolors.END}"
        )

    def log_error(self, message: str):
        self.logger.error(
            f"{bcolors.RED}{datetime.utcnow().isoformat()}:\
 {message}{bcolors.END}"
        )
