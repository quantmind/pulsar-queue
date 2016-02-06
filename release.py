import os

from agile import AgileManager


version_file = os.path.join(os.path.dirname(__file__),
                            'pq', '__init__.py')

note_file = 'docs/notes.md'

if __name__ == '__main__':
    AgileManager(config='release.py').start()
