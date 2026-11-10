import io
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import download

class DownloadTests(unittest.TestCase):
    def setUp(self):
        download.stop.clear()
        download.total = 0

    def test_429_stops_without_retry(self):
        err = HTTPError('url', 429, 'limited', {}, None)
        with tempfile.TemporaryDirectory() as d, patch.object(download.urllib.request, 'urlopen', side_effect=err) as request, patch.object(download.time, 'sleep') as sleep:
            with self.assertRaises(RuntimeError): download.fetch(1, pathlib.Path(d))
            self.assertTrue(download.stop.is_set())
            self.assertEqual(request.call_count, 1)
            sleep.assert_not_called()

    def test_repeated_5xx_stops_after_backoff(self):
        err = HTTPError('url', 503, 'unavailable', {}, None)
        with tempfile.TemporaryDirectory() as d, patch.object(download.urllib.request, 'urlopen', side_effect=err) as request, patch.object(download.time, 'sleep') as sleep:
            with self.assertRaises(RuntimeError): download.fetch(1, pathlib.Path(d))
            self.assertEqual(request.call_count, 4)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 2, 4])
            self.assertTrue(download.stop.is_set())

    def test_downloaded_bytes_count_towards_limit(self):
        with tempfile.TemporaryDirectory() as d, patch.object(download.urllib.request, 'urlopen', return_value=io.BytesIO(b'abc')):
            download.fetch(1, pathlib.Path(d))
            self.assertEqual(download.total, 3)
            self.assertEqual((pathlib.Path(d) / '1.binpb.zst').read_bytes(), b'abc')

if __name__ == '__main__': unittest.main()
