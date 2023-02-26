import socket
import logging
from typing import Union

logger = logging.getLogger(__file__)

def is_port_open(host: str="127.0.0.1", port: Union[str, int]=5052) -> bool:
    """Check if port is open on given host

    Args:
        host (str, optional): _description_. Defaults to "127.0.0.1"
        port (Union[str, int], optional): _description_. Defaults to 5052

    Returns:
        bool: True if port open
    """

    a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    location = (host, int(port))
    result_of_check = a_socket.connect_ex(location)

    if result_of_check == 0:
        logger.info(f"Port {str(port)} is open")
    else:
        logger.info(f"Port {str(port)} is closed")
 
    a_socket.close()

    return result_of_check == 0