"""Minimal Agency HTTP client shared by the entrypoint and the workload."""

import requests

PORT = 8531
AGENTS = [f"agency-agent{i}" for i in (1, 2, 3)]

# HS256 JWT {"iss":"arangodb","server_id":"foo"} signed with the agents'
# shared secret "hello world" (the token from K's demo).
TOKEN = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJhcmFuZ29kYiIsInNlcnZlcl9pZCI6ImZvbyJ9."
    "Q7ieHXQQk0A2BTFTeXDYEujLYxaxC7oYK5BD3Xg4jFg"
)


class Definite(Exception):
    """The agency answered, and the answer says nothing was written."""


class Ambiguous(Exception):
    """We can't tell whether the request took effect (timeout, 408, 503, reset, ...)."""


class AgencyClient:
    def __init__(self, timeout=10.0):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"bearer {TOKEN}"

    def url(self, agent, path):
        return f"http://{agent}:{PORT}{path}"

    def config(self, agent):
        r = self.session.get(self.url(agent, "/_api/agency/config"), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def state(self, agent):
        # redirectToLeader is off by default: we want this agent's own view.
        r = self.session.get(self.url(agent, "/_api/agency/state"), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _post_to_leader(self, path, body, agent=AGENTS[0], max_hops=5):
        """POST, following 307s by hand (requests drops the Authorization header
        when a redirect changes host). Returns (response, agent_host_that_answered)."""
        url = self.url(agent, path)
        for _ in range(max_hops):
            r = self.session.post(url, json=body, timeout=self.timeout, allow_redirects=False)
            if r.status_code == 307:
                url = r.headers["Location"]
                continue
            host = url.split("//", 1)[1].split(":", 1)[0]
            return r, host
        raise Ambiguous(f"too many redirects for {path}")

    def write_raw(self, transactions, agent=AGENTS[0]):
        """Returns (per-transaction indices, answering host). Index 0 = precondition
        failed. Raises Ambiguous when the outcome is unknown."""
        try:
            r, host = self._post_to_leader("/_api/agency/write", transactions, agent)
        except requests.RequestException as exc:
            raise Ambiguous(repr(exc)) from exc
        if r.status_code in (200, 412):
            results = r.json().get("results")
            if isinstance(results, list) and len(results) == len(transactions):
                return results, host
        raise Ambiguous(f"HTTP {r.status_code}: {r.text[:200]}")

    def read_raw(self, queries, agent=AGENTS[0]):
        """Returns (result, answering host). Raises Ambiguous on any failure."""
        try:
            r, host = self._post_to_leader("/_api/agency/read", queries, agent)
        except requests.RequestException as exc:
            raise Ambiguous(repr(exc)) from exc
        if r.status_code == 200:
            return r.json(), host
        raise Ambiguous(f"HTTP {r.status_code}: {r.text[:200]}")

    def inquire(self, client_ids, agent=AGENTS[0]):
        """Index at which each clientId is in the leader's log (0 = not found)."""
        try:
            r, _ = self._post_to_leader("/_api/agency/inquire", client_ids, agent)
        except requests.RequestException as exc:
            raise Ambiguous(repr(exc)) from exc
        if r.status_code in (200, 404):
            results = r.json().get("results")
            if isinstance(results, list) and len(results) == len(client_ids):
                return results
        raise Ambiguous(f"HTTP {r.status_code}: {r.text[:200]}")

    # Convenience wrappers used by the bootstrap path.
    def write(self, transactions, agent=AGENTS[0]):
        results, _ = self.write_raw(transactions, agent)
        return {"results": results}

    def read(self, queries, agent=AGENTS[0]):
        result, _ = self.read_raw(queries, agent)
        return result
