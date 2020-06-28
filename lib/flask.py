import string


class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


def validate_sha256(sha256):
    if not sha256:
        raise InvalidUsage('SHA256 empty', status_code=400)
    if len(sha256) != 64:
        raise InvalidUsage('SHA256 hash needs to be of length 64', status_code=400)
    if not all(c in string.hexdigits for c in sha256):
        raise InvalidUsage('SHA256 hash may only contain hex chars', status_code=400)


def validate_api_key(key):
    s = key.split('-')
    if len(s) != 5:
        raise InvalidUsage('Invalid API key format')
    if len(s[0]) != 8:
        raise InvalidUsage('Invalid API key format')
    if len(s[1]) != 4:
        raise InvalidUsage('Invalid API key format')
    if len(s[2]) != 4:
        raise InvalidUsage('Invalid API key format')
    if len(s[3]) != 4:
        raise InvalidUsage('Invalid API key format')
    if len(s[4]) != 12:
        raise InvalidUsage('Invalid API key format')
    for c in key:
        if c in string.hexdigits:
            continue
        if c == '-':
            continue
        raise InvalidUsage('Invalid API key format')
