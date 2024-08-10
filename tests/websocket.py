from urllib.parse import unquote, urlparse

from channels.testing.websocket import WebsocketCommunicator

class ExtendedWebsocketCommunicator(WebsocketCommunicator):
    def __init__(self, application, path, headers=None, subprotocols=None, spec_version=None, args=tuple(), kwargs=dict()):
        if not isinstance(path, str):
            raise TypeError("Expected str, got {}".format(type(path)))
        parsed = urlparse(path)
        self.scope = {
            "type": "websocket",
            "path": unquote(parsed.path),
            "query_string": parsed.query.encode("utf-8"),
            "headers": headers or [],
            "subprotocols": subprotocols or [],
            "url_route": {
                "args": args,
                "kwargs": kwargs
            }
        }
        if spec_version:
            self.scope["spec_version"] = spec_version
        super(WebsocketCommunicator, self).__init__(application, self.scope)
        self.response_headers = None