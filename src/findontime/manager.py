
import argparse
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Protocol, Tuple

from fastq_handler.actions import (ProcessActionDownsize,
                                   ProcessActionMergeWithLast)

from findontime.configs import InfluConfig, RunConfigMeta
from findontime.connectors import ConnectorDocker, ConnectorParamiko
from findontime.drones import (InsafluFileProcessThread, LockWithOwner,
                               TelevirFileProcessThread, signal_handler)
from findontime.insaflu_uploads import (InfluConfig, InsafluFileProcess,
                                        PreMainWithMetadata,
                                        TelevirFileProcess)
from findontime.upload_utils import (InsafluUploadRemote, UploadAll,
                                     UploadLast, UploadNone)


def get_arguments():

    parser = argparse.ArgumentParser(description="Process fastq files.")
    parser.add_argument(
        "-i", "--in_dir", help="Input directory", required=True)
    parser.add_argument("-o", "--out_dir",
                        help="Output directory", required=True)
    parser.add_argument("-s", "--sleep", help="Sleep time between checks in monitor mode", default=600,
                        type=int)

    parser.add_argument("-n", "--tag", help="name tag, if given, will be added to the output file names",
                        required=False, type=str, default="")

    parser.add_argument("--config", help="config file",
                        required=False, type=str, default="config.ini")

    parser.add_argument(
        "--max_size", help="max size of the output file, in kilobytes", type=int, default=400000)

    parser.add_argument("--merge", help="merge files", action="store_true")

    parser.add_argument(
        "--downsize", help="downsize fastq files", action="store_true")

    parser.add_argument('--upload',
                        default='last',
                        choices=['last', 'all', 'none'],
                        help='file upload stategy (default: last)',)

    parser.add_argument('--connect',
                        default='docker',
                        choices=['docker', 'ssh'],
                        help='file upload stategy (default: docker)',)

    parser.add_argument(
        "--keep_names", help="keep original file names", action="store_true")

    parser.add_argument(
        "--monitor", help="monitor directory until killed", action="store_true")

    parser.add_argument(
        "--televir", help="deploy televir pathogen identification on each sample", action="store_true"
    )

    return ArgsClass(**parser.parse_args().__dict__)


@dataclass
class ArgsClass:

    in_dir: str
    out_dir: str
    sleep: int
    tag: str
    config: str
    merge: bool
    downsize: bool
    upload: str
    connect: str
    keep_names: bool
    monitor: bool
    televir: bool
    max_size: int

    def __post_init__(self):
        self.in_dir = os.path.abspath(self.in_dir)
        self.out_dir = os.path.abspath(self.out_dir)
        self.config = os.path.abspath(self.config)
        self.max_size = self.max_size * 1000


class MainInsaflu:

    def __init__(self):
        pass

    def select_manager(self, args: ArgsClass):

        if args.upload == 'none':
            print("No upload specified, using fastq handler")
            return FastqHandlerManager(args)
        else:
            return InsafluManager(args)

    def run(self):
        args = get_arguments()
        manager = self.select_manager(args)
        manager.run()


class Deployer(Protocol):

    def __init__(self, args: ArgsClass) -> None:
        ...

    def run(self) -> None:
        ...


class FastqHandlerManager:

    def __init__(self, args: ArgsClass):
        self.args = args

    def setup_config(self, args: ArgsClass):

        actions = []

        if args.merge:
            actions.append(ProcessActionMergeWithLast())

        if args.downsize:
            actions.append(ProcessActionDownsize(args.max_size))

        if not actions:
            print("No actions specified, will merge files by default")
            actions.append(ProcessActionMergeWithLast())

        run_metadata = RunConfigMeta(
            fastq_dir=args.in_dir,
            output_dir=args.out_dir,
            name_tag=args.tag,
            actions=actions,
            keep_name=args.keep_names,
            sleep_time=args.sleep,
            max_size=(args.max_size),
        )
        actions = []

        return run_metadata

    def setup_compressor(self, run_metadata: RunConfigMeta):

        compressor = PreMainWithMetadata(run_metadata)

        return compressor

    def run(self):

        compressor = self.setup_compressor(self.setup_config(self.args))

        if self.args.monitor:

            compressor.run_until_killed()

        else:

            compressor.run()


class InsafluManager:

    def __init__(self, args: ArgsClass):
        self.args = args

    def setup_config(self, args: ArgsClass, test=False):

        # check input dir exists
        if not os.path.isdir(args.in_dir):
            print("Input directory does not exist")
            sys.exit(1)

        # check config file exists
        if not os.path.isfile(args.config):
            print("Config file does not exist")
            sys.exit(1)

        # create connector
        if args.connect == 'docker':
            connector = ConnectorDocker(args.config)
            if test:
                connector.set_interactive(False)

        else:
            connector = ConnectorParamiko(args.config)

        insaflu_upload = InsafluUploadRemote(connector, args.config)

        # determine upload strategy
        if args.upload == 'last':
            upload_strategy = UploadLast
        elif args.upload == 'none':
            upload_strategy = UploadNone
        else:
            upload_strategy = UploadAll

        # determine actions
        actions = []
        if args.merge:
            actions.append(ProcessActionMergeWithLast())

        if args.downsize:
            actions.append(ProcessActionDownsize(args.max_size))

        if not actions:
            print("No actions specified, will merge files by default")
            actions.append(ProcessActionMergeWithLast())

        # create run metadata

        run_metadata = InfluConfig(
            fastq_dir=args.in_dir,
            output_dir=args.out_dir,
            name_tag=args.tag,
            uploader=insaflu_upload,
            upload_strategy=upload_strategy,
            actions=actions,
            max_size=args.max_size,
            keep_name=args.keep_names,
            sleep_time=args.sleep,
            deploy_televir=args.televir,
            monitor=args.monitor,
        )

        return run_metadata

    def generate_runners(self, run_metadata: InfluConfig) -> Tuple[InsafluFileProcess, TelevirFileProcess]:

        influ_compressor = InsafluFileProcess(
            run_metadata,
        )

        televir_processor = TelevirFileProcess(
            run_metadata,
        )

        return influ_compressor, televir_processor

    def generate_threads(self, file_processor: InsafluFileProcess, televir_processor: TelevirFileProcess) -> Tuple[InsafluFileProcessThread, TelevirFileProcessThread]:

        lock = LockWithOwner()
        lock.owner = 'A'

        file_processor_task = InsafluFileProcessThread(
            file_processor, lock
        )
        televir_processor_task = TelevirFileProcessThread(
            televir_processor, lock
        )

        file_processor_task.daemon = True
        televir_processor_task.daemon = True

        return file_processor_task, televir_processor_task

    def monitor_tasks(self, file_processor_task: InsafluFileProcessThread, televir_processor_task: TelevirFileProcessThread, args: ArgsClass):

        if not args.monitor:
            if file_processor_task.counter > 0 and televir_processor_task.counter > 0:
                return False

        if not file_processor_task.is_alive() and not televir_processor_task.is_alive():
            return False

        if file_processor_task.error or televir_processor_task.error:
            return False

        return True

    def run(self):

        args = self.args

        run_metadata = self.setup_config(args)

        influ_compressor, televir_processor = self.generate_runners(
            run_metadata)

        file_processor_task, televir_processor_task = self.generate_threads(
            influ_compressor, televir_processor)

        signal.signal(signal.SIGINT, signal_handler)

        loop = True

        try:

            file_processor_task.start()
            televir_processor_task.start()

            while loop:
                new_loop = self.monitor_tasks(
                    file_processor_task, televir_processor_task, args)
                loop = new_loop

                time.sleep(2)

            print("Waiting for tasks to finish...")

        except Exception as e:
            print("Error in main thread.")
            print(e)

        finally:

            print("Stopping tasks...")

            file_processor_task.stop()
            televir_processor_task.stop()

            file_processor_task.join()
            televir_processor_task.join()

            print("Done!")

            sys.exit(0)
