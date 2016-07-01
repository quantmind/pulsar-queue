

def dummy(backend):
    # Just a dummy callable for testing coverage.
    # A callable is invoked when the taskqueue starts
    pass


def simple_task(self, value=0):
    return self.v0 + value
