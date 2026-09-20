"""An oversized archive is split into ``.001``/``.002`` volumes that reassemble.

Splitting exists only to fit a single Telegram send, so the piece that has to be
right is exactness: the volumes concatenated in order must be the archive, byte
for byte, and the names must be the numbered scheme an extractor joins. An
archive that already fits one send is never split, and a source is only removed
once every part is safely on disk.
"""

import asyncio
import os
import tempfile
import unittest

from tasks.conversion_tasks import _volume_name, split_archive_volumes


def _write(path: str, data: bytes) -> str:
    with open(path, "wb") as fh:
        fh.write(data)
    return path


def _reassemble(volumes: list[str]) -> bytes:
    out = bytearray()
    for volume in volumes:
        with open(volume, "rb") as fh:
            out += fh.read()
    return bytes(out)


class VolumeNameTests(unittest.TestCase):
    def test_the_numbered_suffix_is_the_part_scheme(self):
        self.assertEqual(_volume_name("myclips.zip", 1), "myclips.zip.001")
        self.assertEqual(_volume_name("myclips.zip", 12), "myclips.zip.012")
        # A path is never kept in the delivered name.
        self.assertEqual(_volume_name("out/myclips.zip", 2), "myclips.zip.002")

    def test_a_missing_name_falls_back_to_archive(self):
        self.assertEqual(_volume_name("", 1), "archive.zip.001")
        self.assertEqual(_volume_name(None, 1), "archive.zip.001")


class SplitArchiveVolumesTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="archive_split_")
        self.archive = os.path.join(self.dir, "bundle.zip")

    def test_an_archive_that_fits_one_send_is_not_split(self):
        payload = _write(self.archive, b"x" * 100)
        volumes = asyncio.run(split_archive_volumes(self.archive, archive_filename="bundle.zip", max_bytes=1000))
        self.assertEqual(volumes, [payload])
        self.assertTrue(os.path.exists(self.archive))

    def test_splitting_switched_off_keeps_one_file(self):
        payload = _write(self.archive, b"x" * 5000)
        volumes = asyncio.run(split_archive_volumes(self.archive, archive_filename="bundle.zip", max_bytes=0))
        self.assertEqual(volumes, [payload])
        self.assertTrue(os.path.exists(self.archive))

    def test_the_volumes_reassemble_to_the_archive_and_use_the_part_names(self):
        data = os.urandom(1000)
        _write(self.archive, data)
        volumes = asyncio.run(split_archive_volumes(self.archive, archive_filename="bundle.zip", max_bytes=300))
        names = [os.path.basename(v) for v in volumes]
        self.assertEqual(names, ["bundle.zip.001", "bundle.zip.002", "bundle.zip.003", "bundle.zip.004"])
        self.assertEqual(_reassemble(volumes), data)
        # The last volume holds the remainder, not a padded one.
        self.assertEqual([os.path.getsize(v) for v in volumes], [300, 300, 300, 100])

    def test_an_exact_multiple_has_no_empty_tail_volume(self):
        _write(self.archive, b"y" * 600)
        volumes = asyncio.run(split_archive_volumes(self.archive, archive_filename="bundle.zip", max_bytes=300))
        self.assertEqual([os.path.basename(v) for v in volumes], ["bundle.zip.001", "bundle.zip.002"])
        self.assertEqual(_reassemble(volumes), b"y" * 600)

    def test_the_source_archive_is_removed_once_every_part_is_written(self):
        _write(self.archive, b"z" * 700)
        volumes = asyncio.run(split_archive_volumes(self.archive, archive_filename="bundle.zip", max_bytes=300))
        self.assertEqual(len(volumes), 3)
        self.assertFalse(os.path.exists(self.archive))

    def test_keeping_the_source_is_opt_in(self):
        _write(self.archive, b"z" * 700)
        asyncio.run(
            split_archive_volumes(self.archive, archive_filename="bundle.zip", max_bytes=300, remove_source=False)
        )
        self.assertTrue(os.path.exists(self.archive))


if __name__ == "__main__":
    unittest.main()
