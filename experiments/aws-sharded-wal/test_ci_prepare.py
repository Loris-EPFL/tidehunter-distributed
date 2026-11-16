import unittest

import ci_prepare


class PreparationAdmissionTests(unittest.TestCase):
    def environment(self):
        return {'GITHUB_REPOSITORY': ci_prepare.REPOSITORY,
                'GITHUB_REF': 'refs/heads/' + ci_prepare.BRANCH,
                'GITHUB_EVENT_NAME': 'push', 'GITHUB_SHA': 'a' * 40}

    def test_untrusted_ref_or_event_cannot_reach_preparation_even_when_disarmed(self):
        for change in ({'GITHUB_REF': 'refs/heads/main'}, {'GITHUB_EVENT_NAME': 'pull_request'},
                       {'GITHUB_REPOSITORY': 'someone/tidehunter-distributed'}, {'GITHUB_SHA': 'main'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                ci_prepare.settings(self.environment() | change)

    def test_missing_activation_reports_disarmed_instead_of_claiming_readiness(self):
        self.assertEqual(ci_prepare.settings(self.environment()), {
            'enabled': False, 'reason': 'AWS benchmarks are disarmed; no AWS operation or build requested'})
        with self.assertRaisesRegex(ValueError, 'missing GitHub environment'):
            ci_prepare.settings(self.environment() | {'AWS_BENCH_ENABLED': 'true'})



if __name__ == '__main__':
    unittest.main()
