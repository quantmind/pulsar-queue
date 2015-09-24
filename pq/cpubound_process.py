import sys, time

def main(argv):
    print ("cpubound_process file")
    print (argv)
    for i in range(5):
        time.sleep(2)
        print ("process sleep: ", i)

if __name__ == '__main__':
    main(sys.argv[1:])


# exit(0)
