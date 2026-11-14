import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import owner_ftrace


class OwnerFtraceTests(unittest.TestCase):
    def test_missing_events_and_failed_start_remove_instance(self):
        with tempfile.TemporaryDirectory() as root:
            tracefs = Path(root)
            (tracefs / 'instances').mkdir()
            # A fake tracefs cannot create control files. Inject the kernel error.
            with patch.object(Path, 'write_text', side_effect=OSError('denied')):
                instance = owner_ftrace.start('case', tracefs)
            state = owner_ftrace.START_STATES.pop(str(instance))
            self.assertFalse(instance.exists())
            self.assertEqual(state['events_missing'], list(owner_ftrace.EVENTS))
            self.assertTrue(state['errors'])

    def test_stop_cap_stats_and_removal_even_on_error(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            instance = root / 'instance'
            instance.mkdir()
            (instance / 'tracing_on').touch()
            (instance / 'trace').write_bytes(b'123456')
            stats = instance / 'per_cpu/cpu0'
            stats.mkdir(parents=True)
            (stats / 'stats').write_text('overrun: 7\n')
            def remove(path):
                import shutil
                shutil.rmtree(path)
            with patch.object(owner_ftrace, 'MAX_BYTES', 4), patch.object(Path, 'rmdir', remove):
                state = owner_ftrace.stop(instance, root / 'out')
            self.assertFalse(instance.exists())
            self.assertTrue(state['truncated'])
            self.assertEqual(state['buffer_overruns'], 7)
            self.assertEqual((root / 'out/owner-ftrace.txt').read_bytes(), b'1234')
            self.assertEqual(json.loads((root / 'out/owner-ftrace.json').read_text()), state)

    def test_output_error_still_removes_instance(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            instance = root / 'instance'
            instance.mkdir()
            output = root / 'not-a-directory'
            output.touch()
            with patch.object(Path, 'write_text', side_effect=OSError('denied')):
                state = owner_ftrace.stop(instance, output)
            self.assertFalse(instance.exists())
            self.assertTrue(any(e.startswith('output:') for e in state['errors']))
            self.assertTrue(any(e.startswith('metadata:') for e in state['errors']))

    def test_probe_reports_available_events(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            event = root / 'events' / owner_ftrace.EVENTS[0] / 'enable'
            event.parent.mkdir(parents=True)
            event.touch()
            self.assertEqual(owner_ftrace.probe(root)['events_enabled'], [owner_ftrace.EVENTS[0]])
