#!/usr/bin/env python
import sys
import os

from pulsar.apps.test import TestSuite
from pulsar.apps.test.plugins import bench, profile


def run():
    args = sys.argv
    if '--coveralls' in args:
        import pq
        from pulsar.utils.path import Path
        from pulsar.apps.test.cov import coveralls

        repo_token = None
        strip_dirs = [Path(pq.__file__).parent.parent, os.getcwd()]
        if os.path.isfile('.coveralls-repo-token'):
            with open('.coveralls-repo-token') as f:
                repo_token = f.read().strip()
        coveralls(strip_dirs=strip_dirs, repo_token=repo_token)
        sys.exit(0)
    # Run the test suite
    #
    TestSuite(description='Pulsar queue test suite',
              modules=['tests'],
              plugins=(bench.BenchMark(), profile.Profile()),
              test_timeout=30).start()


if __name__ == '__main__':
    run()
