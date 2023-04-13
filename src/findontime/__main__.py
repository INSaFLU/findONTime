
import signal
import sys
import time

from findontime.manager import MainInsaflu


def signal_handler(signal, frame):
    print("exiting")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    main_insaflu = MainInsaflu()
    main_insaflu.run()


if __name__ == "__main__":
    main()
