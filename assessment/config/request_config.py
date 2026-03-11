import sys
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class RequestConfig:
    __REQUIRED_ARGS = {"DB", "INPUT_LOCATION"}
    
    def __init__(self, args: Optional[List[str]] = None):
        self._args = args or sys.argv[1:]
        logger.debug(f"Parsing arguments: {self._args}")
        self._parsed_args = self._parse_request()
        self._validate_required_args()
        self._set_attributes()
        
    def _validate_required_args(self) -> None:
        missing_args = self.__REQUIRED_ARGS - set(self._parsed_args.keys())
        if missing_args:
            raise ValueError(f"Missing required arguments: {', '.join(missing_args)}")
    
    def _set_attributes(self) -> None:
        self.db = self._parsed_args["DB"]
        self.input_location = self._parsed_args["INPUT_LOCATION"]
        
    def _parse_request(self) -> Dict[str, str]:
        parsed = {}
        for arg in self._args:
            if "=" not in arg:
                logger.warning(f"Ignoring malformed argument: {arg}")
                # optionally, we can raise an error here
                # raise ValueError(f"Malformed argument: {arg}")
                continue
            key, value = arg.split("=")
            parsed[key.strip().upper()] = value.strip()
        return parsed