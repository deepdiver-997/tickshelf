"""Archive validation against the C++ history worker and standard Parquet reader."""

import argparse
from contextlib import redirect_stdout
from decimal import Decimal
from io import StringIO
from pathlib import Path
import sys
import tempfile
import unittest

import iquant_archive as archive
from iquant import protocol as wire
from iquant.engine import Worker, read_group
import pyarrow.parquet as pq


KEY = "0123456789abcdef" * 2
BINARY = None


class ArchiveTests(unittest.TestCase):
    def test_incremental_export_checksum_and_raw_restore(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            work = root / "original"
            raw = work / "raw"
            raw.mkdir(parents=True)
            target = work / "archive"
            target.mkdir()
            source = raw / (KEY + ".raw")
            frames = [
                wire.pack(wire.DATA, 1, 1, b'[["600000.SH",{"lastPrice":214748.3651,"volume":3000000000,'
                          b'"askPrice":[1.2345,null],"amount":92233720368.54775808,"time":1700000000000}]]'),
                wire.pack(wire.GAP, 2, 3),
                wire.pack(wire.DATA, 4, 4, b'broken quote payload'),
                wire.pack(wire.DATA, 5, 5, b'[["IF2712.IF",{"lastPrice":0.0000001,"volume":9223372036854775808,'
                          b'"bidVol":[1,2],"askPrice":[NaN]}]]'),
            ]
            source.write_bytes(b"".join(frames))
            cut = sum(map(len, frames[:3]))
            worker = Worker(BINARY, work)
            try:
                worker.write(KEY, read_group(source, 0, 0, cut), True)
                worker.write(KEY, read_group(source, cut, 4, source.stat().st_size), False)
                with source.open("r+b") as file:
                    file.seek(wire.HEADER.size)
                    file.write(b"!")
                with self.assertRaisesRegex(wire.ProtocolError, "checksum"):
                    archive.export(work, target, 160)
                self.assertEqual(archive.read_catalog(target), {})
                source.write_bytes(b"".join(frames))
                with redirect_stdout(StringIO()):
                    archive.export(work, target, 160)
                first_parts = archive.read_catalog(target)[KEY]
                self.assertGreaterEqual(len(first_parts), 2)
                self.assertEqual(first_parts[-1].meta["last_seq"], 4)
                worker.write(KEY, read_group(source, cut, 4, source.stat().st_size), True)
                with redirect_stdout(StringIO()):
                    archive.export(work, target, 160)
                with redirect_stdout(StringIO()) as captured:
                    archive.export(work, target, 160)
                self.assertEqual(captured.getvalue(), "")
                parts = archive.read_catalog(target)[KEY]
                self.assertEqual(parts[:-1], first_parts)
                self.assertEqual(parts[-1].meta["last_seq"], 5)
                for part in parts:
                    archive.check_part(part)
                quotes = [row for part in parts for row in pq.read_table(part.path / "quotes.parquet").to_pylist()]
                self.assertEqual([row["batch_seq"] for row in quotes], [1, 5])
                self.assertEqual(quotes[0]["last_price"], Decimal("214748.365100"))
                self.assertEqual(quotes[0]["volume"], 3000000000)
                self.assertEqual(quotes[0]["amount"], Decimal("92233720368.54775808"))
                self.assertEqual(quotes[0]["ask_price"], [Decimal("1.234500"), None])
                self.assertEqual(quotes[0]["projection_errors"], [])
                self.assertEqual(set(quotes[1]["projection_errors"]), {"lastPrice", "volume", "askPrice"})
                self.assertIsNone(quotes[1]["last_price"])
                self.assertEqual(quotes[1]["bid_volume"], [1, 2])
                metadata = pq.ParquetFile(parts[0].path / "quotes.parquet").schema
                self.assertEqual(metadata.column(metadata.names.index("ordinal")).physical_type, "INT32")
                self.assertEqual(metadata.column(metadata.names.index("last_price")).physical_type, "INT64")
                frame_rows = [row for part in parts for row in pq.read_table(part.path / "frames.parquet").to_pylist()]
                self.assertEqual([row["status"] for row in frame_rows], ["data", "gap", "quarantine", "data"])
                restored = root / "restored"
                archive.restore({KEY: parts}, restored)
                self.assertEqual((restored / "raw" / source.name).read_bytes(), source.read_bytes())
                rebuilt = Worker(BINARY, restored)
                try:
                    rebuilt.write(KEY, read_group(restored / "raw" / source.name, 0, 0, source.stat().st_size), True)
                    self.assertEqual(rebuilt.cursor(KEY), (source.stat().st_size, 5))
                finally:
                    rebuilt.close()
                with (parts[-1].path / "raw.zst").open("r+b") as file:
                    initial = file.read(1)
                    file.seek(0)
                    file.write(bytes([initial[0] ^ 1]))
                with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                    archive.restore({KEY: parts}, root / "damaged")
                self.assertFalse((root / "damaged").exists())
            finally:
                worker.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine-bin", type=Path, required=True)
    args, remaining = parser.parse_known_args()
    BINARY = str(args.engine_bin.resolve())
    unittest.main(argv=[sys.argv[0], *remaining])
