

def string_exception(exc):
    args = []
    for arg in exc.args:
        if isinstance(arg, str):
            args.append(arg)
    if args:
        return args[0] if len(args) == 1 else args
    return str(exc)
