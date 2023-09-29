class UserMessages(object):
    def __init__(self, username, timestamp, message):
        self.username = username
        self.timestamp = timestamp
        self.message = message


def um_to_dict(Object, ctx):
    return {
        "username": Object.username,
        "timestamp": Object.timestamp,
        "message": Object.message,
    }


def dct_to_um(dict, ctx):
    return dict
