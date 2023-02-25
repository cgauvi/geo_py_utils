import socket
import logging

logger = logging.getLogger(__file__)

def is_port_open(host="127.0.0.1": str, port=5052: int) -> bool:
    """Check if port is open on given host

    Args:
        host (_type_, optional): _description_. Defaults to "127.0.0.1":str.
        port (_type_, optional): _description_. Defaults to 5052:int.

    Returns:
        bool: True if port open
    """

    a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    location = (host, port)
    result_of_check = a_socket.connect_ex(location)

    if result_of_check == 0:
        logger.info("Port is open")
    else:
        logger.info("Port is closed")
 
    a_socket.close()

    return result_of_check == 0