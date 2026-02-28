import base64
import json
import random
import time

class AuthToken():
    def __init__(self, id: str, nonce: str=None, issue_time: int=None):
        self.id = id
        if (nonce == None):
            self.nonce = AuthToken.generate_nonce()
        else:
            self.nonce = nonce

        if (issue_time == None):
            self.issue_time = time.time()
        else:
            self.issue_time = issue_time
    def as_token(self):
        return base64.b64encode(json.dumps({
            "id": self.id,
            "nonce": self.nonce,
            "issue_time": self.issue_time
        }).encode('utf8')).decode('utf8')
    def from_token(token: str):
        token_payload = json.loads(base64.b64decode(token))
        return AuthToken(
            token_payload["id"],
            token_payload["nonce"],
            token_payload["issue_time"]
        )
    def generate_nonce():
        return base64.b64encode(random.randbytes(32)).decode()