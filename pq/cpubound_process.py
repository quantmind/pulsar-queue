import sys, time

def main(argv):
    print ("cpubound_process file")
    print (argv)

if __name__ == '__main__':
    main(sys.argv[1:])
    for i in range(5):
        time.sleep(2)
        print ("process sleep: ", i)
