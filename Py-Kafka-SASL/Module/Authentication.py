class Authentication(object):
    def __init__(self, username, email, machine_id, session_id="", otp="", status=""):
        self.username = username
        self.email = email
        self.machine_id = machine_id
        self.session_id = session_id
        self.otp = otp
        self.status = status


def auth_to_dict(Object, ctx):
    return {
        "username": Object.username,
        "email": Object.email,
        "machine_id": Object.machine_id,
        "session_id": Object.session_id,
        "otp": Object.otp,
        "status": Object.status,
    }


def dct_to_auth(dict, ctx):
    return dict
