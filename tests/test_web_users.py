"""Per-user web tokens: a token names one user, and rotation/revocation are real.

The store's Redis leg is not exercised here - the suite runs without a broker -
so these tests drive the in-process mirror, which is the same code path a Flask
process without Redis uses. What matters is that a token resolves to exactly the
user it was minted for, and that rotating or revoking actually stops the old one.
"""

import asyncio
import os
import unittest

from source_helpers import read_source

from utils import web_users


class UserTokenTests(unittest.TestCase):
    def setUp(self):
        # No Redis in the suite, and no state carried between tests: the module
        # keeps a process-wide mirror that would otherwise leak minted tokens.
        self._redis_url = os.environ.pop("REDIS_URL", None)
        web_users._local_by_digest.clear()

    def tearDown(self):
        if self._redis_url is not None:
            os.environ["REDIS_URL"] = self._redis_url
        web_users._local_by_digest.clear()

    def _issue(self, user_id):
        return asyncio.run(web_users.issue_user_token(user_id))

    def _resolve(self, token):
        return asyncio.run(web_users.resolve_user_token(token))

    def _ok(self, user_id, token):
        return asyncio.run(web_users.user_token_ok(user_id, token))

    def test_a_token_resolves_to_the_user_it_was_minted_for(self):
        token = self._issue(4242)
        self.assertTrue(token)
        self.assertEqual(self._resolve(token), 4242)

    def test_unknown_and_empty_tokens_fail_closed(self):
        self._issue(1)
        self.assertIsNone(self._resolve("not-a-real-token"))
        self.assertIsNone(self._resolve(""))
        self.assertIsNone(self._resolve(None))

    def test_a_token_is_valid_only_for_its_own_user(self):
        token = self._issue(7)
        self.assertTrue(self._ok(7, token))
        self.assertFalse(self._ok(8, token))
        self.assertFalse(self._ok(7, "someone-elses-token"))

    def test_rotating_a_token_retires_the_previous_one(self):
        first = self._issue(55)
        second = self._issue(55)
        self.assertNotEqual(first, second)
        self.assertIsNone(self._resolve(first))
        self.assertEqual(self._resolve(second), 55)
        self.assertFalse(self._ok(55, first))

    def test_revoking_a_token_cuts_the_user_off(self):
        token = self._issue(99)
        self.assertTrue(asyncio.run(web_users.revoke_user_token(99)))
        self.assertIsNone(self._resolve(token))
        self.assertFalse(self._ok(99, token))

    def test_a_missing_user_id_gets_no_token(self):
        self.assertEqual(asyncio.run(web_users.issue_user_token(None)), "")
        self.assertFalse(asyncio.run(web_users.revoke_user_token(None)))

    def test_the_digest_hides_the_plaintext(self):
        token = self._issue(3)
        digest = web_users.user_token_digest(token)
        self.assertTrue(digest)
        self.assertNotEqual(digest, token)
        # Only the digest is remembered, never the token itself.
        self.assertIn(digest, web_users._local_by_digest)
        self.assertNotIn(token, web_users._local_by_digest)


class UserTokenWiringTests(unittest.TestCase):
    def test_the_job_api_accepts_a_user_token_and_stamps_the_owner(self):
        src = read_source("web", "webapp.py")
        self.assertIn("authorized, caller_user_id = _user_or_service_ok(incoming_token)", src)
        self.assertIn("_job_access_ok(job_id) or _job_owner_ok(job_id)", src)
        self.assertIn('"user_id": caller_user_id', src)

    def test_the_archive_members_inherit_the_uploaders_identity(self):
        src = read_source("web", "webapp.py")
        self.assertIn("_archive_member_job(member, parent_job_id, request_id, user_id)", src)


if __name__ == "__main__":
    unittest.main()
