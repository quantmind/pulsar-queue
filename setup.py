import os
import sys
from setuptools import setup, find_packages

try:
    import pulsar   # noqa
except ImportError:
    os.environ['pulsar_queue_setup'] = 'yes'

package_name = 'pulsar-queue'
mod = __import__('pq')


def read(fname):
    with open(fname) as f:
        return f.read()


def requirements():
    req = read('requirements.txt').replace('\r', '').split('\n')
    result = []
    for r in req:
        r = r.replace(' ', '')
        if r:
            result.append(r)
    return result


def run(argv=None):
    if argv:
        sys.argv = list(argv)

    setup(name=package_name,
          zip_safe=False,
          version=mod.__version__,
          author=mod.__author__,
          author_email=mod.__contact__,
          url=mod.__homepage__,
          license='BSD',
          description=mod.__doc__,
          long_description=read('README.rst'),
          packages=find_packages(exclude=('tests', 'tests.*')),
          install_requires=requirements(),
          classifiers=['Development Status :: 2 - Pre-Alpha',
                       'Environment :: Web Environment',
                       'Intended Audience :: Developers',
                       'License :: OSI Approved :: BSD License',
                       'Operating System :: OS Independent',
                       'Programming Language :: Python',
                       'Programming Language :: Python :: 3.4',
                       'Topic :: Utilities'])

if __name__ == '__main__':
    run()
