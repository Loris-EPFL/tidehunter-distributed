import unittest

from portable_transport import (
    is_software,
    protocol_key,
    runtime_backend,
    select_backends,
    select_transports,
    transport_for_case,
    validate_transport,
)


class PortableTransportTests(unittest.TestCase):
    def test_research_labels_map_to_canonical_transport_and_runtime_kind(self):
        expected = {
            'tidehunter_tcp': {'kind': 'tcp'},
            'tidehunter_efa': {'kind': 'ofi', 'fabric': {'backend': 'efa'}, 'security': 'mutual-tls'},
            'tidehunter_efa_direct': {'kind': 'ofi', 'fabric': {'backend': 'efa-direct'}, 'security': 'mutual-tls'},
            'tidehunter_efa_native': {'kind': 'ofi', 'fabric': {'backend': 'efa-direct'}, 'security': 'trusted'},
            'tidehunter_efa_trusted': {'kind': 'ofi', 'fabric': {'backend': 'efa'}, 'security': 'trusted'},
            'tidehunter_verbs_native': {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'trusted'},
        }
        for label, transport in expected.items():
            with self.subTest(label=label):
                self.assertEqual(transport_for_case(label), transport)
                self.assertEqual(runtime_backend(label), 'tidehunter')
        self.assertEqual(transport_for_case('fabric-tcp')['fabric']['backend'], 'software-tcp')
        self.assertEqual(runtime_backend('rocksdb'), 'rocksdb')

    def test_explicit_transport_preserves_peer_local_selectors(self):
        client = {'kind': 'ofi', 'fabric': {'backend': 'verbs', 'domain': 'mlx5_0',
                                            'source_address': '10.0.0.1'}, 'security': 'trusted'}
        owners = [
            {'kind': 'ofi', 'fabric': {'backend': 'verbs', 'domain': 'mlx5_1',
                                       'source_address': f'10.0.0.{n}'}, 'security': 'trusted'}
            for n in range(2, 7)
        ]
        selected = select_transports(client, owners, 5)
        self.assertEqual([item['fabric']['source_address'] for item in selected],
                         [f'10.0.0.{n}' for n in range(2, 7)])
        self.assertEqual(selected[0]['fabric']['domain'], 'mlx5_1')
        self.assertEqual(client['fabric']['source_address'], '10.0.0.1')

    def test_client_nic_binding_requires_explicit_matching_owner_vector(self):
        bound = {'kind': 'ofi', 'fabric': {'backend': 'verbs', 'source_address': '10.0.0.1'},
                 'security': 'trusted'}
        with self.assertRaisesRegex(ValueError, 'explicit owner_transports'):
            select_transports(bound, None, 5)
        owner = {'kind': 'ofi', 'fabric': {'backend': 'efa'},
                 'security': 'trusted'}
        with self.assertRaisesRegex(ValueError, 'match client'):
            select_transports(bound, [owner] * 5, 5)
        with self.assertRaisesRegex(ValueError, 'cover every owner'):
            select_transports(bound, [bound] * 4, 5)
        cohosted = [bound, bound]
        self.assertEqual(select_transports(bound, cohosted, 2), cohosted)

    def test_backend_selection_uses_tracked_default_and_rejects_bad_subsets(self):
        configured = ['native', 'tidehunter_tcp', 'valkey_tcp']
        supported = {'native', 'tidehunter_tcp', 'tidehunter_efa', 'valkey_tcp'}
        self.assertEqual(select_backends(None, configured, supported), configured)
        self.assertEqual(select_backends('valkey_tcp,native', configured, supported),
                         ['valkey_tcp', 'native'])
        for raw in ('', ',native', 'native,', 'native,native', 'unknown'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                select_backends(raw, configured, supported)
        with self.assertRaisesRegex(ValueError, 'tracked config'):
            select_backends('tidehunter_efa', configured, supported)
        for tracked in ([], ['native', 'native'], [{'kind': 'tidehunter'}]):
            with self.subTest(tracked=tracked), self.assertRaises(ValueError):
                select_backends(None, tracked, supported)

    def test_schema_and_selector_validation_is_fail_closed(self):
        valid = {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'trusted'}
        self.assertEqual(protocol_key(valid), ('ofi', 'verbs', 'trusted'))
        self.assertFalse(is_software(valid))
        software = transport_for_case('fabric-tcp')
        self.assertTrue(is_software(software))
        invalid = (
            valid | {'legacy_transport': 'efa'},
            {'kind': 'ofi', 'fabric': {'backend': 'efa', 'unknown': True}},
            {'kind': 'ofi', 'fabric': {'backend': 'verbs', 'source_address': '0.0.0.0'}},
            {'kind': 'ofi', 'fabric': {'backend': 'verbs', 'source_address': '224.0.0.1'}},
            {'kind': 'ofi', 'fabric': {'backend': 'verbs', 'domain': ''}},
            {'kind': 'ofi', 'fabric': {'backend': 'verbs'}, 'security': 'plaintext'},
            {'kind': 'ofi', 'fabric': {'backend': 'efa', 'source_address': '192.0.2.3'}},
            {'kind': 'ofi', 'fabric': {'backend': 'efa-direct', 'source_address': '192.0.2.3'}},
            {'kind': 'ofi', 'fabric': {'backend': 'verbs', 'source_address': '2001:db8::2'}},
            {'kind': 'tcp', 'security': 'trusted'},
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_transport(value)
        software_v6 = {'kind': 'ofi', 'fabric': {'backend': 'software-tcp',
                       'source_address': '2001:db8::2'}, 'security': 'trusted'}
        requested = {'kind': 'ofi', 'fabric': {'backend': 'software-tcp', 'domain': None,
                     'source_address': '2001:0db8:0:0:0:0:0:2'}}
        prepared = {'kind': 'ofi', 'fabric': {'backend': 'software-tcp',
                    'source_address': '2001:db8::2'}, 'security': 'mutual-tls'}
        self.assertEqual(validate_transport(requested), validate_transport(prepared))
        self.assertEqual(validate_transport(software_v6), software_v6)


if __name__ == '__main__':
    unittest.main()
