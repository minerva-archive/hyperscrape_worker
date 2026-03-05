import base64
import json
import random
import time


class AuthToken():
    def __init__(self, id: str, nonce: str | None = None, issue_time: int | None = None):
        self.id = id
        self.nonce = nonce
        self.issue_time = issue_time

        if not self.nonce:
            self.nonce = AuthToken.generate_nonce()

        if not self.issue_time:
            self.issue_time = int(time.time())

    def as_token(self) -> str:
        fields = json.dumps({
            "id": self.id,
            "nonce": self.nonce,
            "issue_time": self.issue_time
        })
        token = base64.b64encode(fields.encode("utf8")).decode("utf8")
        return token
    
    @staticmethod
    def from_token(token: str):
        payload = json.loads(base64.b64decode(token))
        return AuthToken(
            payload["id"],
            payload["nonce"],
            payload["issue_time"]
        )

    @staticmethod
    def generate_nonce():
        return base64.b64encode(random.randbytes(32)).decode()
