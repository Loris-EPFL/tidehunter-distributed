import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import analyze

class WireTests(unittest.TestCase):
    def test_fields(self):
        self.assertEqual(list(analyze.fields(bytes([8, 150, 1, 18, 2, 97, 98]))), [(1, 150), (2, b'ab')])
    def test_empty(self):
        self.assertEqual(list(analyze.fields(b'')), [])
    def test_truncated_varint(self):
        with self.assertRaises(ValueError): list(analyze.fields(b'\x08\x80'))
    def test_truncated_bytes(self):
        with self.assertRaises(ValueError): list(analyze.fields(b'\x0a\x03\x01'))
    def test_zero_tag(self):
        with self.assertRaises(ValueError): list(analyze.fields(b'\x00'))
    def test_duplicate_required_field(self):
        with self.assertRaises(ValueError): analyze.one(b'\x08\x01\x08\x02', 1)
    def test_variant(self):
        self.assertEqual(analyze.variant({'A': 1}), ('A', 1))
        self.assertEqual(analyze.variant('None'), ('None', None))
        with self.assertRaises(ValueError): analyze.variant({})

if __name__ == '__main__': unittest.main()
