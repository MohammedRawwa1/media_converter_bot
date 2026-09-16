"""The suite must not depend on a developer's ``.env`` file.

``config`` loads ``.env`` while it is first imported, so anything the file
defines would reach every test that reads the environment - while CI, which has
no such file, sees none of it. ``tests/conftest.py`` hides those values for the
whole session; these tests pin that isolation.
"""

import os


def test_no_dotenv_name_is_visible_to_the_suite(developer_dotenv_keys):
    leaked = sorted(name for name in developer_dotenv_keys if os.environ.get(name))
    assert leaked == [], f"names from a developer's .env are visible to the suite: {leaked}"


def test_no_dotenv_value_is_visible_under_another_name(developer_dotenv_keys, developer_dotenv_values):
    """A value must not reach a test under a different name either.

    ``config`` copies the Mongo URL into ``MONGODB_URL``, ``MONGO_URL`` and
    ``MONGODB_URI``, so hiding only the names the file spells out would leave the
    same value readable.
    """
    aliased = sorted(
        name
        for name, value in os.environ.items()
        if value in developer_dotenv_values and name not in developer_dotenv_keys
    )
    assert aliased == [], f"values from a developer's .env are visible as: {aliased}"


def test_config_constants_come_from_the_process_environment():
    """The import-time constants matter most: they are read once and baked in.

    Both of these are set in a typical ``.env``, and ``config`` reads both while
    it is imported - so a leak here would survive any later cleanup.
    """
    import config

    raw_admin = os.getenv("ADMIN_USER_ID") or ""
    expected_token = os.getenv("BOT_TOKEN") or ""
    expected_admin = int(raw_admin) if raw_admin.isdigit() else None

    assert expected_token == config.BOT_TOKEN
    assert expected_admin == config.ADMIN_USER_ID
