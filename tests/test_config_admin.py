"""The admin gate must fail closed, unlike the deliberately fail-open user ACL.

``/admin`` used to read ``if ADMIN_USER_ID and user_id != ADMIN_USER_ID``, which
authorised *everybody* when the id was unset. These tests pin the corrected
contract: no configured admin means no admin, while the ACL keeps its documented
fail-open behaviour so an empty allow-list never locks users out.
"""

import config


def test_unset_admin_id_authorises_nobody(monkeypatch):
    monkeypatch.setattr(config, "ADMIN_USER_ID", None)

    assert config.is_admin_user(123) is False
    assert config.is_admin_user(0) is False
    assert config.is_admin_user(None) is False


def test_only_the_configured_admin_passes(monkeypatch):
    monkeypatch.setattr(config, "ADMIN_USER_ID", 42)

    assert config.is_admin_user(42) is True
    assert config.is_admin_user(43) is False
    assert config.is_admin_user(-42) is False
    assert config.is_admin_user(None) is False


def test_junk_user_id_does_not_raise(monkeypatch):
    """A malformed id from an update must read as "not admin", never raise."""
    monkeypatch.setattr(config, "ADMIN_USER_ID", 42)

    assert config.is_admin_user("not-a-number") is False
    assert config.is_admin_user("") is False


def test_user_acl_stays_fail_open(monkeypatch):
    """The ACL is open by design; only the admin gate is fail-closed."""
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", set())
    monkeypatch.setattr(config, "ADMIN_USER_ID", None)

    assert config.is_user_allowed(999) is True


def test_admin_is_always_allowed_by_the_acl(monkeypatch):
    monkeypatch.setattr(config, "ALLOWED_USER_IDS", {7})
    monkeypatch.setattr(config, "ADMIN_USER_ID", 42)

    assert config.is_user_allowed(42) is True
