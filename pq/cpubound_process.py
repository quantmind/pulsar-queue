import sys, time
import traceback


def main(task, *args):
    import pq
    time.sleep(2)
    print ("cpubound_process file")
    print (argv)


if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except:
        exc_info = sys.exc_info()
        stacktrace = traceback.format_tb(exc_info[2])
        sys.stderr.write('\n'.join(stacktrace))
