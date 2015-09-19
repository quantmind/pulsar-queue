#!/usr/bin/env python
import sys
import os

from pulsar.apps.test import TestSuite, TestPlugin
from pulsar.apps.test.plugins import bench, profile


class PostgreSql(TestPlugin):
    name = 'postgresql'
    meta = "CONNECTION_STRING"
    default = 'postgresql+green://odm:odmtest@127.0.0.1:5432/odmtests'
    desc = 'Default connection string for the PostgreSql server'


def run(**params):
    args = params.get('argv', sys.argv)
    if '--coveralls' in args:
        import odm
        from pulsar.utils.path import Path
        from pulsar.apps.test.cov import coveralls

        repo_token = None
        strip_dirs = [Path(odm.__file__).parent.parent, os.getcwd()]
        if os.path.isfile('.coveralls-repo-token'):
            with open('.coveralls-repo-token') as f:
                repo_token = f.read().strip()
        coveralls(strip_dirs=strip_dirs, repo_token=repo_token)
        sys.exit(0)
    # Run the test suite
    #
    TestSuite(description='ODM test suite',
              modules=['tests'],
              plugins=(bench.BenchMark(),
                       profile.Profile(),
                       PostgreSql()),
              **params).start()


if __name__ == '__main__':
    run()
