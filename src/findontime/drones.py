import os
import signal
import sys
import threading
import time
from _thread import interrupt_main
from threading import Event, Thread

from findontime.insaflu_uploads import InsafluFileProcess, TelevirFileProcess


class LockWithOwner:

    lock = threading.RLock()
    owner = 'A'

    def acquire_for(self, owner):
        n = 0
        while True:
            self.lock.acquire()
            if self.owner == owner:
                break
            n += 1
            self.lock.release()

    def release_to(self, new_owner):
        self.owner = new_owner
        self.lock.release()


class InsafluFileProcessThread(Thread):
    def __init__(self, compressor: InsafluFileProcess, thread_lock: LockWithOwner):
        super(InsafluFileProcessThread, self).__init__()
        self.lock = thread_lock
        self.processor = compressor
        self._stopevent = Event()  # initialize the event
        self.counter = 0
        self.error = False
        self.schedule_stop = False

    def run(self):
        try:
            while not self._stopevent.is_set():
                self._stopevent.clear()  # Make sure the thread is unset
                self.lock.acquire_for("A")

                print("--------------------")
                print("Processing files, cycle number: {}".format(
                    self.counter))
                self.processor.run()

                self.counter += 1

                if self.counter == 1:
                    if self.processor.run_metadata.monitor is False:
                        self._stopevent.set()

                self.lock.release_to('B')

        except KeyboardInterrupt:
            pass

        except Exception as e:
            print("Error in thread, stopping...")
            print(e)
            self.error = True
            self.stop()
            interrupt_main()

    def stop(self):
        self._stopevent.set()

    def join(self, timeout=None):
        self._stopevent.set()
        Thread.join(self, timeout)


class TelevirFileProcessThread(Thread):
    def __init__(self, processor: TelevirFileProcess, thread_lock: LockWithOwner):
        super(TelevirFileProcessThread, self).__init__()
        self.lock = thread_lock
        self.processor = processor
        self._stopevent = Event()  # initialize the event
        self.work_period = processor.real_sleep
        self.counter = 0
        self.error = False

    def run(self):
        try:

            while not self._stopevent.is_set():
                start_time = time.time()
                execution_time = 0
                self._stopevent.clear()  # Make sure the thread is unset
                self.lock.acquire_for("B")

                while execution_time < self.work_period:
                    self.processor.run()
                    execution_time = time.time() - start_time

                    self.counter += 1

                    if self.counter == 1:
                        if self.processor.run_metadata.monitor is False:
                            self._stopevent.set()
                            break

                    time.sleep(1)

                self.lock.release_to('A')

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            print("Error in TelevirFileProcessThread")
            print(e)
            self.error = True

            self.stop()

            interrupt_main()

    def stop(self):
        self._stopevent.set()

    def join(self, timeout=None):
        self._stopevent.set()
        Thread.join(self, timeout)


def signal_handler(signal, frame):
    print("Exiting program")
    sys.exit(0)
