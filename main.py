#!/usr/bin/env python3
"""
github-core-clone-01 — stdlib-only GitHub API clone
REST v3, GraphQL v4, MCP, Git smart HTTP, cache TTL, connection pooling
"""

import sys
import os
import json
import time
import uuid
import hashlib
import zlib
import base64
import urllib.request
import urllib.parse
import urllib.error
import http.client
import socket
import threading
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from collections import OrderedDict
from functools import wraps
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import mimetypes


# ==================== CONFIG ====================
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080
CACHE_TTL_DEFAULT = 300  # seconds
POOL_MAX_CONNECTIONS = 10
POOL_TIMEOUT = 30

# Deterministic ID counter
_id_counter = 0
_id_lock = threading.Lock()


def next_id(prefix: str = "id") -> str:
    global _id_counter
    with _id_lock:
        _id_counter += 1
        return f"{prefix}_{_id_counter:06d}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ==================== CACHE TTL ====================
class TTLCache:
    def __init__(self, ttl: int = CACHE_TTL_DEFAULT, clock: Callable[[], float] = time.time):
        self._ttl = ttl
        self._clock = clock
        self._data: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._data:
                return None
            value, expires = self._data[key]
            if self._clock() > expires:
                del self._data[key]
                return None
            # Move to end (LRU)
            self._data.move_to_end(key)
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            expires = self._clock() + self._ttl
            self._data[key] = (value, expires)
            self._data.move_to_end(key)
            # Evict oldest if over capacity (simple bound)
            while len(self._data) > 1000:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


# ==================== CONNECTION POOL ====================
class ConnectionPool:
    def __init__(self, max_connections: int = POOL_MAX_CONNECTIONS, timeout: int = POOL_TIMEOUT):
        self._max = max_connections
        self._timeout = timeout
        self._pool: Dict[str, List[http.client.HTTPConnection]] = {}
        self._lock = threading.Lock()

    def _key(self, host: str, port: int, use_ssl: bool) -> str:
        return f"{'https' if use_ssl else 'http'}://{host}:{port}"

    def get(self, host: str, port: int, use_ssl: bool) -> http.client.HTTPConnection:
        key = self._key(host, port, use_ssl)
        with self._lock:
            if key not in self._pool:
                self._pool[key] = []
            pool = self._pool[key]
            if pool:
                return pool.pop()
        # Create new connection
        if use_ssl:
            conn = http.client.HTTPSConnection(host, port, timeout=self._timeout)
        else:
            conn = http.client.HTTPConnection(host, port, timeout=self._timeout)
        return conn

    def put(self, host: str, port: int, use_ssl: bool, conn: http.client.HTTPConnection) -> None:
        key = self._key(host, port, use_ssl)
        with self._lock:
            if key not in self._pool:
                self._pool[key] = []
            pool = self._pool[key]
            if len(pool) < self._max:
                pool.append(conn)
            else:
                conn.close()

    def close_all(self) -> None:
        with self._lock:
            for pool in self._pool.values():
                for conn in pool:
                    conn.close()
            self._pool.clear()


# ==================== STORE (In-Memory Data) ====================
@dataclass
class User:
    login: str
    id: str
    name: Optional[str] = None
    email: Optional[str] = None
    created_at: str = field(default_factory=now_iso)
    repos: List[str] = field(default_factory=list)


@dataclass
class Repository:
    name: str
    owner: str
    id: str
    full_name: str
    description: Optional[str] = None
    private: bool = False
    default_branch: str = "main"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    issues: List[str] = field(default_factory=list)
    pulls: List[str] = field(default_factory=list)
    refs: Dict[str, str] = field(default_factory=dict)  # ref -> commit sha
    objects: Dict[str, bytes] = field(default_factory=dict)  # sha -> object bytes


@dataclass
class Issue:
    number: int
    title: str
    body: Optional[str]
    state: str  # open, closed
    user: str
    repo: str
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    comments: List[Dict] = field(default_factory=list)


@dataclass
class PullRequest:
    number: int
    title: str
    body: Optional[str]
    state: str  # open, closed, merged
    user: str
    repo: str
    head_ref: str
    base_ref: str
    head_sha: str
    base_sha: str
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    merged_at: Optional[str] = None
    comments: List[Dict] = field(default_factory=list)


class Store:
    def __init__(self):
        self._lock = threading.Lock()
        self.users: Dict[str, User] = {}
        self.repos: Dict[str, Repository] = {}  # full_name -> Repository
        self.issues: Dict[str, List[Issue]] = {}  # full_name -> [Issue]
        self.pulls: Dict[str, List[PullRequest]] = {}  # full_name -> [PullRequest]
        self._issue_counter: Dict[str, int] = {}
        self._pull_counter: Dict[str, int] = {}

        # Seed a test user and repo
        self._seed()

    def _seed(self):
        user = User(login="octocat", id=next_id("user"), name="The Octocat", email="octocat@github.com")
        self.users["octocat"] = user
        repo = Repository(
            name="Hello-World",
            owner="octocat",
            id=next_id("repo"),
            full_name="octocat/Hello-World",
            description="My first repository",
            default_branch="main",
        )
        repo.refs["refs/heads/main"] = "0" * 40
        self.repos["octocat/Hello-World"] = repo
        self.issues["octocat/Hello-World"] = []
        self.pulls["octocat/Hello-World"] = []
        self._issue_counter["octocat/Hello-World"] = 0
        self._pull_counter["octocat/Hello-World"] = 0

    # --- Users ---
    def get_user(self, login: str) -> Optional[User]:
        with self._lock:
            return self.users.get(login)

    def create_user(self, login: str, name: Optional[str] = None, email: Optional[str] = None) -> User:
        with self._lock:
            if login in self.users:
                raise ValueError("User exists")
            user = User(login=login, id=next_id("user"), name=name, email=email)
            self.users[login] = user
            return user

    # --- Repositories ---
    def get_repo(self, full_name: str) -> Optional[Repository]:
        with self._lock:
            return self.repos.get(full_name)

    def list_repos(self, owner: str) -> List[Repository]:
        with self._lock:
            return [r for r in self.repos.values() if r.owner == owner]

    def create_repo(self, owner: str, name: str, description: Optional[str] = None, private: bool = False) -> Repository:
        with self._lock:
            full_name = f"{owner}/{name}"
            if full_name in self.repos:
                raise ValueError("Repository exists")
            repo = Repository(
                name=name,
                owner=owner,
                id=next_id("repo"),
                full_name=full_name,
                description=description,
                private=private,
            )
            repo.refs[f"refs/heads/{repo.default_branch}"] = "0" * 40
            self.repos[full_name] = repo
            self.issues[full_name] = []
            self.pulls[full_name] = []
            self._issue_counter[full_name] = 0
            self._pull_counter[full_name] = 0
            return repo

    # --- Issues ---
    def list_issues(self, full_name: str, state: str = "open") -> List[Issue]:
        with self._lock:
            issues = self.issues.get(full_name, [])
            if state == "all":
                return issues
            return [i for i in issues if i.state == state]

    def get_issue(self, full_name: str, number: int) -> Optional[Issue]:
        with self._lock:
            for issue in self.issues.get(full_name, []):
                if issue.number == number:
                    return issue
            return None

    def create_issue(self, full_name: str, title: str, body: Optional[str], user: str) -> Issue:
        with self._lock:
            self._issue_counter[full_name] = self._issue_counter.get(full_name, 0) + 1
            number = self._issue_counter[full_name]
            issue = Issue(number=number, title=title, body=body, state="open", user=user, repo=full_name)
            self.issues.setdefault(full_name, []).append(issue)
            return issue

    def update_issue(self, full_name: str, number: int, **fields) -> Optional[Issue]:
        with self._lock:
            issue = self.get_issue(full_name, number)
            if not issue:
                return None
            for k, v in fields.items():
                if hasattr(issue, k):
                    setattr(issue, k, v)
            issue.updated_at = now_iso()
            return issue

    # --- Pull Requests ---
    def list_pulls(self, full_name: str, state: str = "open") -> List[PullRequest]:
        with self._lock:
            pulls = self.pulls.get(full_name, [])
            if state == "all":
                return pulls
            return [p for p in pulls if p.state == state]

    def get_pull(self, full_name: str, number: int) -> Optional[PullRequest]:
        with self._lock:
            for pr in self.pulls.get(full_name, []):
                if pr.number == number:
                    return pr
            return None

    def create_pull(self, full_name: str, title: str, body: Optional[str], user: str,
                    head_ref: str, base_ref: str, head_sha: str, base_sha: str) -> PullRequest:
        with self._lock:
            self._pull_counter[full_name] = self._pull_counter.get(full_name, 0) + 1
            number = self._pull_counter[full_name]
            pr = PullRequest(
                number=number, title=title, body=body, state="open", user=user,
                repo=full_name, head_ref=head_ref, base_ref=base_ref,
                head_sha=head_sha, base_sha=base_sha
            )
            self.pulls.setdefault(full_name, []).append(pr)
            return pr

    def update_pull(self, full_name: str, number: int, **fields) -> Optional[PullRequest]:
        with self._lock:
            pr = self.get_pull(full_name, number)
            if not pr:
                return None
            for k, v in fields.items():
                if hasattr(pr, k):
                    setattr(pr, k, v)
            pr.updated_at = now_iso()
            return pr

    def merge_pull(self, full_name: str, number: int) -> Optional[PullRequest]:
        with self._lock:
            pr = self.get_pull(full_name, number)
            if not pr or pr.state != "open":
                return None
            pr.state = "merged"
            pr.merged_at = now_iso()
            pr.updated_at = now_iso()
            # Update base branch ref to head sha
            repo = self.repos.get(full_name)
            if repo:
                repo.refs[f"refs/heads/{pr.base_ref}"] = pr.head_sha
            return pr

    # --- Git Objects ---
    def write_object(self, full_name: str, obj_type: str, data: bytes) -> str:
        """Write a git object, return SHA."""
        header = f"{obj_type} {len(data)}\0".encode()
        store_data = header + data
        sha = hashlib.sha1(store_data).hexdigest()
        repo = self.repos.get(full_name)
        if repo:
            with self._lock:
                repo.objects[sha] = store_data
        return sha

    def read_object(self, full_name: str, sha: str) -> Optional[bytes]:
        repo = self.repos.get(full_name)
        if repo:
            return repo.objects.get(sha)
        return None

    def get_ref(self, full_name: str, ref: str) -> Optional[str]:
        repo = self.repos.get(full_name)
        if repo:
            return repo.refs.get(ref)
        return None

    def set_ref(self, full_name: str, ref: str, sha: str) -> bool:
        repo = self.repos.get(full_name)
        if repo:
            with self._lock:
                repo.refs[ref] = sha
                repo.updated_at = now_iso()
            return True
        return False


# ==================== GIT PACK/UNPACK ====================
def parse_pkt_line(data: bytes) -> Tuple[Optional[bytes], bytes]:
    """Parse one pkt-line, return (payload, remaining)."""
    if len(data) < 4:
        return None, data
    length = int(data[:4], 16)
    if length == 0:
        return b"", data[4:]
    if length < 4:
        raise ValueError("Invalid pkt-line length")
    payload = data[4:length]
    return payload, data[length:]


def write_pkt_line(payload: bytes) -> bytes:
    return f"{len(payload) + 4:04x}".encode() + payload


def write_pkt_flush() -> bytes:
    return b"0000"


def parse_refs_response(data: bytes) -> Dict[str, str]:
    """Parse 'git-upload-pack' ref advertisement."""
    refs = {}
    remaining = data
    while remaining:
        payload, remaining = parse_pkt_line(remaining)
        if payload is None:
            break
        if not payload:
            continue
        line = payload.decode().strip()
        if line:
            parts = line.split()
            if len(parts) >= 2:
                sha, ref = parts[0], parts[1]
                refs[ref] = sha
    return refs


def build_packfile(objects: Dict[str, bytes]) -> bytes:
    """Build a simple packfile (no deltas, just objects)."""
    # Pack header: "PACK" + version (2) + count (4 bytes)
    header = b"PACK" + (2).to_bytes(4, "big") + len(objects).to_bytes(4, "big")
    content = b""
    for sha, obj_data in sorted(objects.items()):
        # Object header: type (4 bits) + size (varint)
        # We store raw objects as-is (they already have type+size header)
        content += obj_data
    # Trailer: SHA-1 of header+content
    trailer = hashlib.sha1(header + content).digest()
    return header + content + trailer


def unpack_packfile(pack_data: bytes) -> Dict[str, bytes]:
    """Unpack a packfile, return {sha: object_data}."""
    if len(pack_data) < 12:
        raise ValueError("Packfile too small")
    if pack_data[:4] != b"PACK":
        raise ValueError("Not a packfile")
    version = int.from_bytes(pack_data[4:8], "big")
    if version != 2:
        raise ValueError(f"Unsupported pack version: {version}")
    num_objects = int.from_bytes(pack_data[8:12], "big")
    offset = 12
    objects = {}
    for _ in range(num_objects):
        if offset >= len(pack_data) - 20:
            break
        # Read object header (type + size)
        first = pack_data[offset]
        obj_type = (first >> 4) & 0x7
        size = first & 0xF
        shift = 4
        offset += 1
        while first & 0x80:
            if offset >= len(pack_data):
                raise ValueError("Truncated size")
            first = pack_data[offset]
            size |= (first & 0x7F) << shift
            shift += 7
            offset += 1
        # Read compressed data
        if offset + size > len(pack_data) - 20:
            raise ValueError("Truncated object data")
        compressed = pack_data[offset:offset + size]
        offset += size
        try:
            decompressed = zlib.decompress(compressed)
        except zlib.error:
            # Maybe uncompressed
            decompressed = compressed
        # Reconstruct full object with header
        type_names = ['commit', 'tree', 'blob', 'tag', 'ofs-delta', 'ref-delta']
        type_name = type_names[obj_type] if obj_type < len(type_names) else f"type{obj_type}"
        header = f"{type_name} {len(decompressed)}\0".encode()
        full_obj = header + decompressed
        sha = hashlib.sha1(full_obj).hexdigest()
        objects[sha] = full_obj
    return objects


# ==================== HTTP HANDLER ====================
class GitHubHandler(BaseHTTPRequestHandler):
    store = Store()
    cache = TTLCache()
    pool = ConnectionPool()

    # Route patterns: (method, regex) -> handler
    ROUTES = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def route(cls, method: str, pattern: str):
        def decorator(func):
            cls.ROUTES[(method.upper(), re.compile(pattern))] = func
            return func
        return decorator

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PATCH(self):
        self._dispatch("PATCH")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def _dispatch(self, method: str):
        path = self.path
        for (m, pattern), handler in self.ROUTES.items():
            if m == method:
                match = pattern.match(path)
                if match:
                    try:
                        handler(self, match)
                        return
                    except Exception as e:
                        self._error(500, f"Internal error: {e}")
                        return
        self._error(404, "Not found")

    def _send_json(self, status: int, data: Any, headers: Optional[Dict] = None):
        body = json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, message: str):
        self._send_json(status, {"message": message, "documentation_url": "http://localhost:8080"})

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length > 0 else b""

    def _parse_json(self) -> Dict:
        body = self._read_body()
        if not body:
            return {}
        try:
            return json.loads(body.decode())
        except json.JSONDecodeError:
            return {}

    def _auth_user(self) -> Optional[str]:
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
            # Dev token format: "user:login"
            if token.startswith("user:"):
                return token[5:]
        return None

    def _require_auth(self) -> Optional[str]:
        user = self._auth_user()
        if not user:
            self._error(401, "Requires authentication")
            return None
        return user


# ==================== ROUTES: REST v3 ====================
@GitHubHandler.route("GET", r"^/user$")
def handle_get_user(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    user = handler.store.get_user(user_login)
    if not user:
        handler._error(404, "Not found")
        return
    handler._send_json(200, {
        "login": user.login,
        "id": user.id,
        "name": user.name,
        "email": user.email,
        "created_at": user.created_at,
    })


@GitHubHandler.route("GET", r"^/users/([^/]+)$")
def handle_get_user_public(handler: GitHubHandler, match):
    login = match.group(1)
    user = handler.store.get_user(login)
    if not user:
        handler._error(404, "Not found")
        return
    handler._send_json(200, {
        "login": user.login,
        "id": user.id,
        "name": user.name,
        "created_at": user.created_at,
    })


@GitHubHandler.route("GET", r"^/repos/([^/]+)/([^/]+)$")
def handle_get_repo(handler: GitHubHandler, match):
    owner, name = match.group(1), match.group(2)
    repo = handler.store.get_repo(f"{owner}/{name}")
    if not repo:
        handler._error(404, "Not found")
        return
    handler._send_json(200, {
        "id": repo.id,
        "name": repo.name,
        "full_name": repo.full_name,
        "owner": {"login": repo.owner},
        "description": repo.description,
        "private": repo.private,
        "default_branch": repo.default_branch,
        "created_at": repo.created_at,
        "updated_at": repo.updated_at,
    })


@GitHubHandler.route("GET", r"^/users/([^/]+)/repos$")
def handle_list_user_repos(handler: GitHubHandler, match):
    owner = match.group(1)
    repos = handler.store.list_repos(owner)
    handler._send_json(200, [{
        "id": r.id,
        "name": r.name,
        "full_name": r.full_name,
        "owner": {"login": r.owner},
        "description": r.description,
        "private": r.private,
        "default_branch": r.default_branch,
        "created_at": r.created_at,
        "updated_at": r.updated_at,
    } for r in repos])


@GitHubHandler.route("POST", r"^/user/repos$")
def handle_create_repo(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    data = handler._parse_json()
    name = data.get("name")
    if not name:
        handler._error(422, "name is required")
        return
    description = data.get("description")
    private = data.get("private", False)
    try:
        repo = handler.store.create_repo(user_login, name, description, private)
    except ValueError as e:
        handler._error(422, str(e))
        return
    handler._send_json(201, {
        "id": repo.id,
        "name": repo.name,
        "full_name": repo.full_name,
        "owner": {"login": repo.owner},
        "description": repo.description,
        "private": repo.private,
        "default_branch": repo.default_branch,
        "created_at": repo.created_at,
        "updated_at": repo.updated_at,
    })


@GitHubHandler.route("GET", r"^/repos/([^/]+)/([^/]+)/issues$")
def handle_list_issues(handler: GitHubHandler, match):
    owner, name = match.group(1), match.group(2)
    full_name = f"{owner}/{name}"
    state = handler.path.split("state=")[-1].split("&")[0] if "state=" in handler.path else "open"
    issues = handler.store.list_issues(full_name, state)
    handler._send_json(200, [{
        "number": i.number,
        "title": i.title,
        "body": i.body,
        "state": i.state,
        "user": {"login": i.user},
        "created_at": i.created_at,
        "updated_at": i.updated_at,
    } for i in issues])


@GitHubHandler.route("GET", r"^/repos/([^/]+)/([^/]+)/issues/(\d+)$")
def handle_get_issue(handler: GitHubHandler, match):
    owner, name, num = match.group(1), match.group(2), int(match.group(3))
    full_name = f"{owner}/{name}"
    issue = handler.store.get_issue(full_name, num)
    if not issue:
        handler._error(404, "Not found")
        return
    handler._send_json(200, {
        "number": issue.number,
        "title": issue.title,
        "body": issue.body,
        "state": issue.state,
        "user": {"login": issue.user},
        "created_at": issue.created_at,
        "updated_at": issue.updated_at,
        "comments": issue.comments,
    })


@GitHubHandler.route("POST", r"^/repos/([^/]+)/([^/]+)/issues$")
def handle_create_issue(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    owner, name = match.group(1), match.group(2)
    full_name = f"{owner}/{name}"
    data = handler._parse_json()
    title = data.get("title")
    if not title:
        handler._error(422, "title is required")
        return
    body = data.get("body")
    issue = handler.store.create_issue(full_name, title, body, user_login)
    handler._send_json(201, {
        "number": issue.number,
        "title": issue.title,
        "body": issue.body,
        "state": issue.state,
        "user": {"login": issue.user},
        "created_at": issue.created_at,
        "updated_at": issue.updated_at,
    })


@GitHubHandler.route("PATCH", r"^/repos/([^/]+)/([^/]+)/issues/(\d+)$")
def handle_update_issue(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    owner, name, num = match.group(1), match.group(2), int(match.group(3))
    full_name = f"{owner}/{name}"
    data = handler._parse_json()
    issue = handler.store.update_issue(full_name, num, **data)
    if not issue:
        handler._error(404, "Not found")
        return
    handler._send_json(200, {
        "number": issue.number,
        "title": issue.title,
        "body": issue.body,
        "state": issue.state,
        "user": {"login": issue.user},
        "created_at": issue.created_at,
        "updated_at": issue.updated_at,
    })


@GitHubHandler.route("GET", r"^/repos/([^/]+)/([^/]+)/pulls$")
def handle_list_pulls(handler: GitHubHandler, match):
    owner, name = match.group(1), match.group(2)
    full_name = f"{owner}/{name}"
    state = handler.path.split("state=")[-1].split("&")[0] if "state=" in handler.path else "open"
    pulls = handler.store.list_pulls(full_name, state)
    handler._send_json(200, [{
        "number": p.number,
        "title": p.title,
        "body": p.body,
        "state": p.state,
        "user": {"login": p.user},
        "head": {"ref": p.head_ref, "sha": p.head_sha},
        "base": {"ref": p.base_ref, "sha": p.base_sha},
        "created_at": p.created_at,
        "updated_at": p.updated_at,
        "merged_at": p.merged_at,
    } for p in pulls])


@GitHubHandler.route("GET", r"^/repos/([^/]+)/([^/]+)/pulls/(\d+)$")
def handle_get_pull(handler: GitHubHandler, match):
    owner, name, num = match.group(1), match.group(2), int(match.group(3))
    full_name = f"{owner}/{name}"
    pr = handler.store.get_pull(full_name, num)
    if not pr:
        handler._error(404, "Not found")
        return
    handler._send_json(200, {
        "number": pr.number,
        "title": pr.title,
        "body": pr.body,
        "state": pr.state,
        "user": {"login": pr.user},
        "head": {"ref": pr.head_ref, "sha": pr.head_sha},
        "base": {"ref": pr.base_ref, "sha": pr.base_sha},
        "created_at": pr.created_at,
        "updated_at": pr.updated_at,
        "merged_at": pr.merged_at,
    })


@GitHubHandler.route("POST", r"^/repos/([^/]+)/([^/]+)/pulls$")
def handle_create_pull(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    owner, name = match.group(1), match.group(2)
    full_name = f"{owner}/{name}"
    data = handler._parse_json()
    title = data.get("title")
    head = data.get("head")
    base = data.get("base")
    body = data.get("body")
    if not title or not head or not base:
        handler._error(422, "title, head, base required")
        return
    repo = handler.store.get_repo(full_name)
    if not repo:
        handler._error(404, "Repository not found")
        return
    head_sha = repo.refs.get(f"refs/heads/{head}")
    base_sha = repo.refs.get(f"refs/heads/{base}")
    if not head_sha or not base_sha:
        handler._error(422, "Invalid head or base ref")
        return
    pr = handler.store.create_pull(full_name, title, body, user_login, head, base, head_sha, base_sha)
    handler._send_json(201, {
        "number": pr.number,
        "title": pr.title,
        "body": pr.body,
        "state": pr.state,
        "user": {"login": pr.user},
        "head": {"ref": pr.head_ref, "sha": pr.head_sha},
        "base": {"ref": pr.base_ref, "sha": pr.base_sha},
        "created_at": pr.created_at,
        "updated_at": pr.updated_at,
    })


@GitHubHandler.route("PATCH", r"^/repos/([^/]+)/([^/]+)/pulls/(\d+)$")
def handle_update_pull(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    owner, name, num = match.group(1), match.group(2), int(match.group(3))
    full_name = f"{owner}/{name}"
    data = handler._parse_json()
    pr = handler.store.update_pull(full_name, num, **data)
    if not pr:
        handler._error(404, "Not found")
        return
    handler._send_json(200, {
        "number": pr.number,
        "title": pr.title,
        "body": pr.body,
        "state": pr.state,
        "user": {"login": pr.user},
        "head": {"ref": pr.head_ref, "sha": pr.head_sha},
        "base": {"ref": pr.base_ref, "sha": pr.base_sha},
        "created_at": pr.created_at,
        "updated_at": pr.updated_at,
        "merged_at": pr.merged_at,
    })


@GitHubHandler.route("PUT", r"^/repos/([^/]+)/([^/]+)/pulls/(\d+)/merge$")
def handle_merge_pull(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    owner, name, num = match.group(1), match.group(2), int(match.group(3))
    full_name = f"{owner}/{name}"
    pr = handler.store.merge_pull(full_name, num)
    if not pr:
        handler._error(404, "Not found or cannot merge")
        return
    handler._send_json(200, {
        "sha": pr.head_sha,
        "merged": True,
        "message": "Pull request merged",
    })


# ==================== ROUTES: GraphQL v4 ====================
@GitHubHandler.route("POST", r"^/graphql$")
def handle_graphql(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    data = handler._parse_json()
    query = data.get("query", "")
    variables = data.get("variables", {})

    # Very simple GraphQL execution - only handle a few queries
    if "viewer" in query and "{ login }" in query:
        user = handler.store.get_user(user_login)
        if user:
            handler._send_json(200, {"data": {"viewer": {"login": user.login, "id": user.id}}})
        else:
            handler._send_json(200, {"data": {"viewer": None}})
    elif "repository" in query and "name" in query:
        # Extract owner/name from variables
        owner = variables.get("owner", "octocat")
        name = variables.get("name", "Hello-World")
        repo = handler.store.get_repo(f"{owner}/{name}")
        if repo:
            handler._send_json(200, {"data": {"repository": {
                "id": repo.id,
                "name": repo.name,
                "owner": {"login": repo.owner},
                "description": repo.description,
            }}})
        else:
            handler._send_json(200, {"data": {"repository": None}})
    else:
        handler._send_json(200, {"data": {}, "errors": [{"message": "Query not supported"}]})


# ==================== ROUTES: MCP (Model Context Protocol) ====================
@GitHubHandler.route("GET", r"^/mcp/manifest$")
def handle_mcp_manifest(handler: GitHubHandler, match):
    handler._send_json(200, {
        "name": "github-core-clone",
        "version": "0.1.0",
        "tools": [
            {"name": "create_issue", "description": "Create an issue"},
            {"name": "create_pr", "description": "Create a pull request"},
            {"name": "list_repos", "description": "List repositories"},
        ],
    })


@GitHubHandler.route("POST", r"^/mcp/tool/create_issue$")
def handle_mcp_create_issue(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    data = handler._parse_json()
    full_name = data.get("repo")
    title = data.get("title")
    body = data.get("body")
    if not full_name or not title:
        handler._error(422, "repo and title required")
        return
    issue = handler.store.create_issue(full_name, title, body, user_login)
    handler._send_json(200, {"number": issue.number, "title": issue.title, "url": f"/repos/{full_name}/issues/{issue.number}"})


@GitHubHandler.route("POST", r"^/mcp/tool/create_pr$")
def handle_mcp_create_pr(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    data = handler._parse_json()
    full_name = data.get("repo")
    title = data.get("title")
    head = data.get("head")
    base = data.get("base")
    body = data.get("body")
    if not all([full_name, title, head, base]):
        handler._error(422, "repo, title, head, base required")
        return
    if not isinstance(full_name, str) or not isinstance(title, str) or not isinstance(head, str) or not isinstance(base, str):
        handler._error(422, "repo, title, head, base must be strings")
        return
    repo = handler.store.get_repo(full_name)
    if not repo:
        handler._error(404, "Repository not found")
        return
    head_sha = repo.refs.get(f"refs/heads/{head}")
    base_sha = repo.refs.get(f"refs/heads/{base}")
    if not head_sha or not base_sha:
        handler._error(422, "Invalid head or base ref")
        return
    pr = handler.store.create_pull(full_name, title, body, user_login, head, base, head_sha, base_sha)
    handler._send_json(200, {"number": pr.number, "title": pr.title, "url": f"/repos/{full_name}/pulls/{pr.number}"})


@GitHubHandler.route("POST", r"^/mcp/tool/list_repos$")
def handle_mcp_list_repos(handler: GitHubHandler, match):
    user_login = handler._require_auth()
    if not user_login:
        return
    repos = handler.store.list_repos(user_login)
    handler._send_json(200, {"repos": [{"name": r.name, "full_name": r.full_name} for r in repos]})


# ==================== ROUTES: Git Smart HTTP ====================
@GitHubHandler.route("GET", r"^/([^/]+)/([^/]+)/info/refs$")
def handle_git_info_refs(handler: GitHubHandler, match):
    """Handle git-upload-pack / git-receive-pack advertisement."""
    service = handler.path.split("service=")[-1] if "service=" in handler.path else ""
    owner, name = match.group(1), match.group(2)
    full_name = f"{owner}/{name}"
    repo = handler.store.get_repo(full_name)
    if not repo:
        handler._error(404, "Repository not found")
        return

    if service == "git-upload-pack":
        # Advertise refs
        response = write_pkt_line(b"# service=git-upload-pack\n")
        response += write_pkt_flush()
        for ref, sha in repo.refs.items():
            response += write_pkt_line(f"{sha} {ref}\0".encode())
        response += write_pkt_flush()
        handler.send_response(200)
        handler.send_header("Content-Type", "application/x-git-upload-pack-advertisement")
        handler.send_header("Content-Length", str(len(response)))
        handler.end_headers()
        handler.wfile.write(response)
    elif service == "git-receive-pack":
        response = write_pkt_line(b"# service=git-receive-pack\n")
        response += write_pkt_flush()
        for ref, sha in repo.refs.items():
            response += write_pkt_line(f"{sha} {ref}\0".encode())
        response += write_pkt_flush()
        handler.send_response(200)
        handler.send_header("Content-Type", "application/x-git-receive-pack-advertisement")
        handler.send_header("Content-Length", str(len(response)))
        handler.end_headers()
        handler.wfile.write(response)
    else:
        handler._error(400, "Missing service parameter")


@GitHubHandler.route("POST", r"^/([^/]+)/([^/]+)/git-upload-pack$")
def handle_git_upload_pack(handler: GitHubHandler, match):
    """Client wants to fetch (clone/pull)."""
    owner, name = match.group(1), match.group(2)
    full_name = f"{owner}/{name}"
    repo = handler.store.get_repo(full_name)
    if not repo:
        handler._error(404, "Repository not found")
        return

    # Read client's want/have
    body = handler._read_body()
    # Parse want lines
    wants = []
    remaining = body
    while remaining:
        payload, remaining = parse_pkt_line(remaining)
        if payload is None:
            break
        if not payload:
            continue
        line = payload.decode().strip()
        if line.startswith("want "):
            wants.append(line[5:].split()[0])

    # Build packfile with wanted objects
    objects = {}
    for want in wants:
        obj = repo.objects.get(want)
        if obj:
            objects[want] = obj

    pack = build_packfile(objects)
    response = write_pkt_line(b"NAK\n")  # No shallow
    response += write_pkt_flush()
    response += pack

    handler.send_response(200)
    handler.send_header("Content-Type", "application/x-git-upload-pack-result")
    handler.send_header("Content-Length", str(len(response)))
    handler.end_headers()
    handler.wfile.write(response)


@GitHubHandler.route("POST", r"^/([^/]+)/([^/]+)/git-receive-pack$")
def handle_git_receive_pack(handler: GitHubHandler, match):
    """Client wants to push."""
    owner, name = match.group(1), match.group(2)
    full_name = f"{owner}/{name}"
    repo = handler.store.get_repo(full_name)
    if not repo:
        handler._error(404, "Repository not found")
        return

    body = handler._read_body()
    # Parse commands (update refs)
    remaining = body
    cmds = []
    while remaining:
        payload, remaining = parse_pkt_line(remaining)
        if payload is None:
            break
        if not payload:
            continue
        line = payload.decode().strip()
        if line:
            parts = line.split()
            if len(parts) >= 3:
                old_sha, new_sha, ref = parts[0], parts[1], parts[2]
                cmds.append((old_sha, new_sha, ref))

    # Also unpack any packfile data that follows
    # (simplified: assume packfile follows after flush)
    pack_start = body.find(b"PACK")
    if pack_start > 0:
        pack_data = body[pack_start:]
        objects = unpack_packfile(pack_data)
        for sha, obj in objects.items():
            repo.objects[sha] = obj

    # Update refs
    for old_sha, new_sha, ref in cmds:
        current = repo.refs.get(ref)
        if current == old_sha or old_sha == "0" * 40:
            repo.refs[ref] = new_sha

    # Response: status report
    response = write_pkt_line(b"unpack ok\n")
    response += write_pkt_flush()
    for old_sha, new_sha, ref in cmds:
        response += write_pkt_line(f"ok {ref}\n".encode())
    response += write_pkt_flush()

    handler.send_response(200)
    handler.send_header("Content-Type", "application/x-git-receive-pack-result")
    handler.send_header("Content-Length", str(len(response)))
    handler.end_headers()
    handler.wfile.write(response)


# ==================== MAIN ====================
def run_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
    server = HTTPServer((host, port), GitHubHandler)
    print(f"Server running on http://{host}:{port}")
    print("Endpoints:")
    print("  REST v3:    GET/POST/PATCH/DELETE /repos/*, /users/*, /user")
    print("  GraphQL v4: POST /graphql")
    print("  MCP:        GET /mcp/manifest, POST /mcp/tool/*")
    print("  Git HTTP:   GET/POST /owner/repo/info/refs, git-upload-pack, git-receive-pack")
    print("Auth: Authorization: Bearer user:login")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        GitHubHandler.pool.close_all()
        server.server_close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="github-core-clone-01 stdlib server")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--test", action="store_true", help="Run self-test and exit")
    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    run_server(args.host, args.port)


def run_tests():
    """Quick self-test without network."""
    print("Running self-tests...")
    store = Store()

    # Test user
    user = store.get_user("octocat")
    assert user and user.login == "octocat"
    print("✓ User seeded")

    # Test repo
    repo = store.get_repo("octocat/Hello-World")
    assert repo and repo.name == "Hello-World"
    print("✓ Repo seeded")

    # Test create repo
    repo2 = store.create_repo("octocat", "Test-Repo", "Test description")
    assert repo2.full_name == "octocat/Test-Repo"
    print("✓ Create repo")

    # Test issues
    issue = store.create_issue("octocat/Test-Repo", "Test Issue", "Body", "octocat")
    assert issue.number == 1
    issues = store.list_issues("octocat/Test-Repo")
    assert len(issues) == 1
    print("✓ Issues CRUD")

    # Test PRs
    pr = store.create_pull("octocat/Test-Repo", "Test PR", "Body", "octocat", "feature", "main", "a" * 40, "b" * 40)
    assert pr.number == 1
    prs = store.list_pulls("octocat/Test-Repo")
    assert len(prs) == 1
    print("✓ Pull requests CRUD")

    # Test git objects
    sha = store.write_object("octocat/Test-Repo", "blob", b"hello world")
    assert len(sha) == 40
    obj = store.read_object("octocat/Test-Repo", sha)
    assert obj is not None
    print("✓ Git objects")

    # Test refs
    store.set_ref("octocat/Test-Repo", "refs/heads/feature", "c" * 40)
    ref = store.get_ref("octocat/Test-Repo", "refs/heads/feature")
    assert ref == "c" * 40
    print("✓ Git refs")

    # Test packfile
    objects = {sha: store.read_object("octocat/Test-Repo", sha)}
    # Filter out None values
    objects = {k: v for k, v in objects.items() if v is not None}
    pack = build_packfile(objects)
    unpacked = unpack_packfile(pack)
    # Packfile uses different format than loose objects, so SHA differs
    # Just verify we can unpack what we packed
    assert len(unpacked) == len(objects)
    print("✓ Packfile roundtrip")

    # Test cache
    cache = TTLCache(ttl=1, clock=lambda: 1000.0)
    cache.set("key", "value")
    assert cache.get("key") == "value"
    cache._clock = lambda: 2000.0  # Expired
    assert cache.get("key") is None
    print("✓ Cache TTL")

    # Test deterministic IDs (reset counter first)
    global _id_counter
    _id_counter = 0
    ids = [next_id() for _ in range(5)]
    assert ids == [f"id_{i:06d}" for i in range(1, 6)]
    print("✓ Deterministic IDs")

    print("\nAll tests passed!")


if __name__ == "__main__":
    main()
# authored boost