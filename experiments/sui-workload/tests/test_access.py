import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from analyze import reconstruct_inputs, version_age

class AccessTests(unittest.TestCase):
    def test_immutable_input_absent_from_effects(self):
        effects = {'changed_objects': [], 'unchanged_consensus_objects': []}
        data = {'gas_payment': {'objects': []}}
        program = {'inputs': [{'Object': {'ImmutableOrOwned': {'object_id': 'a', 'version': 7}}}]}
        inputs, shared = reconstruct_inputs(effects, data, 'ProgrammableTransaction', program)
        self.assertEqual(inputs, {'a': (7, None)})
        self.assertFalse(shared)

    def test_read_only_shared_root(self):
        effects = {'unchanged_consensus_objects': [{'object_id': 'clock', 'kind': {'ReadOnlyRoot': {'version': 12}}}]}
        inputs, _ = reconstruct_inputs(effects, {}, 'system', None)
        self.assertEqual(inputs['clock'], (12, {'shared': 0}))

    def test_version_age_and_unknown_prior_version(self):
        created = {('object', 3): (100, 4)}
        self.assertEqual(version_age(created, 'object', 3, 100, 9), (100, 4, 0, 5))
        self.assertEqual(version_age(created, 'object', 2, 100, 9), ('', '', '', ''))

if __name__ == '__main__': unittest.main()
