"""Strict wire/BCS primitives copied from the mainnet size survey, without private data paths.

Schema provenance is pinned in schema.json. Unknown owner/type/status variants fail closed.
Canonical values remain opaque only where derivation does not inspect their fields.
"""
import collections

def varint(b, i):
    value = 0
    for shift in range(0, 70, 7):
        if i >= len(b):
            raise ValueError('truncated varint')
        x = b[i]
        i += 1
        value |= (x & 127) << shift
        if x < 128:
            return (value, i)
    raise ValueError('oversized varint')

def uleb(n):
    if not 0 <= n <= 0xFFFFFFFF:
        raise ValueError('BCS length/variant outside u32')
    result = bytearray()
    while n >= 128:
        result.append(n & 127 | 128)
        n >>= 7
    result.append(n)
    return bytes(result)

def proto(b):
    result = collections.defaultdict(list)
    i = 0
    while i < len(b):
        tag, i = varint(b, i)
        field, wire = (tag >> 3, tag & 7)
        if not field > 0:
            raise ValueError('invalid BCS field')
        if wire == 0:
            value, i = varint(b, i)
        elif wire in (1, 2, 5):
            if wire == 2:
                size, i = varint(b, i)
            else:
                size = 8 if wire == 1 else 4
            if not i + size <= len(b):
                raise ValueError('truncated protobuf')
            value = b[i:i + size]
            i += size
        else:
            raise ValueError(f'unsupported protobuf wire type {wire}')
        result[field].append(value)
    return result

def one(d, k):
    values = d.get(k, [])
    if not len(values) == 1:
        raise ValueError((k, len(values)))
    return values[0]

def bcs_payload(d, expected):
    wrapper = proto(one(d, 1))
    if 1 in wrapper:
        if not one(wrapper, 1).decode() == expected:
            raise ValueError('invalid BCS field')
    return one(wrapper, 2)

class BCS:

    def __init__(self, b):
        self.b, self.i = (b, 0)

    def take(self, n):
        if not (n >= 0 and self.i + n <= len(self.b)):
            raise ValueError(('truncated BCS', self.i, n))
        v = self.b[self.i:self.i + n]
        self.i += n
        return v

    def u(self, n=8):
        return int.from_bytes(self.take(n), 'little')

    def tag(self):
        start = self.i
        v, self.i = varint(self.b, self.i)
        if v > 0xFFFFFFFF or self.b[start:self.i] != uleb(v):
            raise ValueError('noncanonical BCS ULEB128')
        return v

    def blob(self):
        return self.take(self.tag())

    def digest(self):
        value = self.blob()
        if not len(value) == 32:
            raise ValueError('invalid BCS field')
        return value

    def option(self, fn):
        t = self.tag()
        if not t in (0, 1):
            raise ValueError('invalid BCS field')
        return fn() if t else None

    def end(self):
        if not self.i == len(self.b):
            raise ValueError(('trailing BCS', self.i, len(self.b)))

    def typetag(self):
        t = self.tag()
        if t in (0, 1, 2, 3, 4, 5, 8, 9, 10):
            return
        if t == 6:
            self.typetag()
        elif t == 7:
            self.structtag()
        else:
            raise ValueError(f'unknown TypeTag {t}')

    def structtag(self):
        self.take(32)
        self.blob()
        self.blob()
        for _ in range(self.tag()):
            self.typetag()

    def owner(self):
        t = self.tag()
        if t in (0, 1):
            self.take(32)
        elif t == 2:
            self.take(8)
        elif t == 3:
            pass
        elif t == 4:
            self.take(40)
        else:
            raise ValueError(f'unknown Owner {t}')

    def location(self):
        self.take(32)
        self.blob()
        self.take(4)
        self.option(self.blob)

    def status(self):
        status = self.tag()
        if not status in (0, 1):
            raise ValueError('invalid BCS field')
        error = None
        if status:
            error = self.tag()
            if error in (4, 5, 24, 28, 37, 38):
                self.take(16)
            elif error in (6, 41):
                self.take(32)
            elif error == 11:
                self.option(self.location)
            elif error == 12:
                self.location()
                self.take(8)
            elif error == 19:
                self.take(2)
                k = self.tag()
                if not k <= 19:
                    raise ValueError(('CommandArgumentError', k))
                self.take({4: 2, 5: 4, 6: 2}.get(k, 0))
            elif error == 20:
                self.take(2)
                if not self.tag() in (0, 1):
                    raise ValueError('invalid BCS field')
            elif error == 21:
                self.take(4)
            elif error == 22:
                self.take(2)
            elif error == 27:
                k = self.tag()
                if k in (0, 1):
                    self.take(32)
                elif k == 2:
                    pass
                elif k == 3:
                    self.blob()
                elif k == 4:
                    self.take(1)
                elif k == 5:
                    self.take(64)
                else:
                    raise ValueError(('PackageUpgradeError', k))
            elif error == 33:
                self.take(32 * self.tag())
            elif error == 34:
                self.take(32)
                self.blob()
            elif error == 35:
                self.blob()
            elif error not in (0, 1, 2, 3, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 23, 25, 26, 29, 30, 31, 32, 36, 39, 40):
                raise ValueError(f'unknown ExecutionErrorKind {error}')
            self.option(lambda: self.take(8))
        return error
