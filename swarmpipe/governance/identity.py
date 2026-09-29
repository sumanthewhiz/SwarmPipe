"""Identity, delegation, short-lived credentials and the secrets broker.

- Three identities: the agent (workload identity), the user it acts for, and each tool/server.
- Delegated actions use the INTERSECTION of agent and user permissions, never the union.
- Agents never see raw secrets: config refers to `secret://NAME`; only providers/tools resolve them.
- Short-lived scoped tokens are HMAC-signed with a key derived from a local master key.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from swarmpipe.core.errors import AuthorizationError


def _scope_match(granted: frozenset[str], required: str) -> bool:
    for g in granted:
        if g == "*" or g == required or required.startswith(g + ":"):
            return True
    return False


@dataclass(frozen=True)
class Identity:
    principal: str
    scopes: frozenset[str] = field(default_factory=frozenset)
    tenants: frozenset[str] = field(default_factory=lambda: frozenset({"*"}))
    roles: frozenset[str] = field(default_factory=frozenset)
    pii_access: bool = False
    on_behalf_of: "Identity | None" = None

    @property
    def kind(self) -> str:
        return self.principal.split(":", 1)[0]

    @property
    def name(self) -> str:
        return self.principal.split(":", 1)[-1]

    def can(self, scope: str) -> bool:
        ok = _scope_match(self.scopes, scope)
        if self.on_behalf_of is not None:
            ok = ok and self.on_behalf_of.can(scope)
        return ok

    def can_access_tenant(self, tenant: str) -> bool:
        ok = "*" in self.tenants or tenant in self.tenants
        if self.on_behalf_of is not None:
            ok = ok and self.on_behalf_of.can_access_tenant(tenant)
        return ok

    def has_role(self, role: str) -> bool:
        who = self.on_behalf_of or self
        return role in who.roles or "admin" in who.roles

    @property
    def pii_allowed(self) -> bool:
        return self.pii_access and (self.on_behalf_of.pii_allowed if self.on_behalf_of else True)

    def acting_for(self, user: "Identity") -> "Identity":
        return Identity(self.principal, self.scopes, self.tenants, self.roles, self.pii_access, user)

    def describe(self) -> str:
        return f"{self.principal} acting for {self.on_behalf_of.principal}" if self.on_behalf_of else self.principal

    def require(self, scope: str) -> None:
        if not self.can(scope):
            raise AuthorizationError(f"{self.describe()} lacks scope '{scope}'", details={"scope": scope})


class SecretsBroker:
    def __init__(self, settings):
        self.settings = settings
        self._resolved: dict[str, str] = {}
        self._lock = threading.Lock()
        self._file_cache: dict | None = None

    def _secrets_file(self) -> dict:
        if self._file_cache is None:
            p = Path(self.settings.root) / ".secrets.json"
            try:
                self._file_cache = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
            except (OSError, ValueError):
                self._file_cache = {}
        return self._file_cache

    def resolve(self, ref: str | None) -> str | None:
        if not ref or not isinstance(ref, str) or not ref.startswith("secret://"):
            return ref
        name = ref[len("secret://"):]
        value = os.environ.get(name) or self._secrets_file().get(name)
        if value:
            with self._lock:
                self._resolved[name] = value
        return value

    def handle(self, ref: str) -> str:
        """What logs/audit may show instead of the value."""
        return ref if ref.startswith("secret://") else "<literal>"

    def known_values(self) -> list[str]:
        with self._lock:
            return [v for v in self._resolved.values() if v and len(v) >= 8]

    # ---- master key / derived keys -----------------------------------------------------------
    def master_key(self) -> bytes:
        path = self.settings.data_path("state", "master.key")
        with self._lock:
            if path.exists():
                return base64.b64decode(path.read_text(encoding="ascii").strip())
            path.parent.mkdir(parents=True, exist_ok=True)
            key = secrets.token_bytes(32)
            path.write_text(base64.b64encode(key).decode("ascii"), encoding="ascii")
            return key

    def derive_key(self, purpose: str) -> bytes:
        return hmac.new(self.master_key(), purpose.encode("utf-8"), hashlib.sha256).digest()


class IdentityService:
    def __init__(self, settings, secrets_broker: SecretsBroker):
        self.settings = settings
        self.secrets = secrets_broker

    def user(self, name: str) -> Identity:
        cfg = self.settings.users.get(name)
        if cfg is None:
            raise AuthorizationError(f"unknown user '{name}'")
        return Identity(principal=f"user:{name}", scopes=frozenset(cfg.get("scopes", [])),
                        tenants=frozenset(cfg.get("tenants", ["*"])), roles=frozenset(cfg.get("roles", [])),
                        pii_access=bool(cfg.get("pii_access", False)))

    @staticmethod
    def agent(agent_id: str, scopes: set[str] | frozenset[str]) -> Identity:
        return Identity(principal=f"agent:{agent_id}", scopes=frozenset(scopes))

    @staticmethod
    def system(name: str) -> Identity:
        return Identity(principal=f"system:{name}", scopes=frozenset({"*"}), roles=frozenset({"admin"}))

    # ---- short-lived scoped tokens ----------------------------------------------------------
    def issue_token(self, ident: Identity, ttl_s: int = 300) -> str:
        body = {"sub": ident.principal, "scopes": sorted(ident.scopes), "obo": ident.on_behalf_of.principal if ident.on_behalf_of else None,
                "exp": int(time.time()) + ttl_s}
        raw = base64.urlsafe_b64encode(json.dumps(body, sort_keys=True).encode()).decode()
        sig = hmac.new(self.secrets.derive_key("token"), raw.encode(), hashlib.sha256).hexdigest()
        return f"{raw}.{sig}"

    def verify_token(self, token: str) -> dict:
        try:
            raw, sig = token.rsplit(".", 1)
        except ValueError as exc:
            raise AuthorizationError("malformed token") from exc
        good = hmac.new(self.secrets.derive_key("token"), raw.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(good, sig):
            raise AuthorizationError("token signature invalid")
        body = json.loads(base64.urlsafe_b64decode(raw.encode()))
        if body["exp"] < time.time():
            raise AuthorizationError("token expired")
        return body
