from setuptools import setup, find_packages

import queue_config as config


def run():

    meta = dict(
        name='pulsar-queue',
        author='Luca Sbardella',
        author_email="luca@quantmind.com",
        maintainer_email="luca@quantmind.com",
        url="https://github.com/quantmind/pulsar-queue",
        license="BSD",
        long_description=config.read('README.rst'),
        packages=find_packages(exclude=['tests', 'tests.*']),
        include_package_data=True,
        zip_safe=False,
        setup_requires=['pulsar'],
        install_requires=config.requirements('requirements.txt')[0],
        # tests_require=config.requirements('requirements-dev.txt')[0],
        classifiers=[
            'Development Status :: 2 - Pre-Alpha',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: BSD License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Topic :: Utilities'
        ]
    )

    setup(**config.setup(meta, 'pq'))


if __name__ == '__main__':
    run()
