class APILimitExceeded(Exception):
    def __init__(self, url):
        super().__init__(f"URL: {url} limit exceeded")
