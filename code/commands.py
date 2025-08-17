from enum import Enum

class Command(Enum):
    REGISTER_CLIENT = "register_client"
    CREATE_QUEUE = "create_queue"
    APPEND_VALUE = "append_value"
    READ_QUEUE = "read_queue"
    LIST_QUEUES = "list_queues"
