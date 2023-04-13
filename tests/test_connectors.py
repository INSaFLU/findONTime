import gzip
import os
import unittest
from dataclasses import dataclass

import mockssh
import pandas as pd
import pytest

from fastq_handler.records import ProcessActionMergeWithLast
from findontime.configs import InfluConfig
from findontime.connectors import ConnectorDocker, ConnectorParamiko
from findontime.insaflu_uploads import (InfluProcessed, InsafluFileProcess,
                                        TelevirFileProcess)
from findontime.manager import ArgsClass, MainInsaflu
from findontime.records import MetadataEntry
from findontime.upload_utils import (InsafluFile, InsafluSampleCodes,
                                     InsafluUploadRemote, UploadAll,
                                     UploadLast, UploadLog)


@pytest.fixture(scope="session")
def temp_config_file(tmp_path_factory):
    config_file = tmp_path_factory.mktemp("config") / "config.ini"

    with open(config_file, "w") as f:

        f.write(
            """
            [SSH]
            username = localhost
            ip_address = 127.0.0.1
            rsa_key = /home/bioinf/.ssh/id_rsa
            """
        )

    return config_file


class TestUploadLog(unittest.TestCase):

    def test_init(self):
        upload_log = UploadLog()

        assert upload_log.log.equals(pd.DataFrame(columns=UploadLog.columns))

    def test_new_entry(self):
        upload_log = UploadLog()

        new_entry = pd.concat(
            [
                upload_log.log,
                pd.DataFrame(
                    data=[["test", "test", "test", "test", 0, "test"]],
                    columns=UploadLog.columns)
            ]
        )

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "test",)

        assert upload_log.log.equals(new_entry)

    def test_modify_entry_status(self):
        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "test",)

        upload_log.modify_entry_status("test", 2)

        assert upload_log.log.loc[0, "status"] == 2

    def test_update_log_existing(self):
        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "test",)

        upload_log.update_log("test", "test", "test", "test", 2)

        assert upload_log.log.loc[0, "status"] == 2

    def test_check_entry_exists(self):

        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "test",)

        assert upload_log.check_entry_exists("test") == True

    def test_update_log_new(self):
        upload_log = UploadLog()

        upload_log.new_entry(
            "test1",
            "test1",
            "test1",
            "test1",
            0,
            "test1",)

        upload_log.update_log("test", "test", "test", "test", 2)

        assert upload_log.check_entry_exists("test") == True

    def test_get_log(self):

        upload_log = UploadLog()

        assert upload_log.get_log().equals(pd.DataFrame(columns=UploadLog.columns))

    def test_get_file_status(self):

        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "test",)

        assert upload_log.get_file_status("test") == 0

    def test_generate_InsafluSample(self):

        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "test",)

        sample = upload_log.generate_InsafluFile(
            upload_log.log.loc[0],
        )

        assert sample.sample_id == "test"
        assert sample.barcode == "test"
        assert sample.file_path == "test"
        assert sample.remote_path == "test"
        assert sample.status == 0

    def test_get_sample(self):
        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "fastq",)

        sample_files = upload_log.get_sample_files("test")
        sample = sample_files[0]
        assert sample.sample_id == "test"
        assert sample.barcode == "test"
        assert sample.file_path == "test"
        assert sample.remote_path == "test"
        assert sample.status == 0

    def test_get_fastq_by_status(self):

        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "fastq",)

        upload_log.new_entry(
            "test1",
            "test1",
            "test1",
            "test1",
            1,
            "fastq",)

        selected_samples = upload_log.generate_fastq_list_status(0)

        assert len(selected_samples) == 1

    def get_sample_remotepaths(self):
        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "fastq",)

        sample_files = upload_log.get_sample_remotepaths("test")
        sample = sample_files[0]
        assert sample == "test"

    def test_get_samples_list(self):
        """
        test get_samples_list"""
        upload_log = UploadLog()

        upload_log.new_entry(
            "test",
            "test",
            "test",
            "test",
            0,
            "fastq",)

        sample_list = upload_log.generate_fastq_list()

        sample = sample_list[0]
        assert sample.sample_id == "test"


class ConnectorParamikoProxy(ConnectorParamiko):

    def connect(self):
        """
        create local ssh mock server"""
        users = {
            "test-user": "/home/bioinf/.ssh/id_rsa",
        }
        self.server = mockssh.Server(users)

    def __enter__(self):
        self.server.__enter__()
        with self.server as s:
            self.conn = s.client("test-user")
            return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.server.__exit__(exc_type, exc_value, traceback)


class TestConnectorParamiko:

    def test_init(self, tmpdir, temp_config_file):

        # config_file = tmpdir.mkdir("config").join("config.ini")
        #
        # config_file.write(
        #    """
        #    [SSH]
        #    username = localhost
        #    ip_address = 127.0.0.1
        #    rsa_key = /home/bioinf/.ssh/id_rsa
        #    """
        # )

        connector = ConnectorParamikoProxy(config_file=temp_config_file)

        assert connector.username == "localhost"
        assert connector.ip_address == "127.0.0.1"
        assert connector.rsa_key_path == "/home/bioinf/.ssh/id_rsa"

    def test_check_file_exists(self, tmpdir, temp_config_file):
        """
        test check_file_exists method"""
        connector = ConnectorParamikoProxy(config_file=temp_config_file)

        assert connector.check_file_exists("test") == False

        temp_file = tmpdir.mkdir("temp").join("temp.txt")

        temp_file.write("test")

        assert connector.check_file_exists(temp_file) == True

    def test_execute_command(self, tmpdir, temp_config_file):
        """
        test execute_command method"""
        connector = ConnectorParamikoProxy(config_file=temp_config_file)

        assert connector.execute_command("echo test").strip() == "test"

    def test_download_file(self, tmpdir, temp_config_file):
        """
        test download_file method"""
        connector = ConnectorParamikoProxy(config_file=temp_config_file)

        tmpdir = tmpdir.mkdir("temp")
        temp_file = tmpdir.join("temp.txt")
        output_file = tmpdir.join("output.txt")

        temp_file.write("test")

        connector.download_file(str(temp_file), str(output_file))

        assert output_file.read() == "test"

    def test_upload_file(self, tmpdir, temp_config_file):
        """
        test upload_file method"""
        connector = ConnectorParamikoProxy(config_file=temp_config_file)

        tmpdir = tmpdir.mkdir("temp")
        temp_file = tmpdir.join("temp.txt")
        output_file = tmpdir.join("output.txt")

        temp_file.write("test")

        connector.upload_file(str(temp_file), str(output_file))

        assert output_file.read() == "test"


@pytest.fixture
def config_file(request):
    """
    create config file for user"""
    config_file = request.config.getoption("--config-file")

    return config_file


@pytest.fixture
def remote_uploader(config_file):

    connector = ConnectorDocker(config_file=config_file)
    connector.set_interactive(False)
    insaflu_upload = InsafluUploadRemote(connector, config_file)

    return insaflu_upload


@dataclass
class TempInput:
    input_dir: str
    fastq_dir: str
    remote_dir: str
    fastq_file_one: str
    fastq_file_two: str
    fastq_one_size: int
    fastq_two_size: int

    def __post_init__(self):
        self.fastq_files = [self.fastq_file_one, self.fastq_file_two]
        self.fastq_sizes = [self.fastq_one_size, self.fastq_two_size]
        self.full_size = sum(self.fastq_sizes)
        self.generate_metadata()
        self.set_remote_files()

    @staticmethod
    def metadata_file_name(fastq_file: str):
        return os.path.basename(fastq_file).replace(".fastq.gz", ".metadata.txt")

    def generate_metadata(self):
        met_entry_fastq_one = MetadataEntry(
            sample_name="test1",
            fastq1=self.fastq_file_one,
            tag="test",
        )

        met_entry_fastq_two = MetadataEntry(
            sample_name="test2",
            fastq1=self.fastq_file_two,
            tag="test",
        )

        self.fastq_one_metadata = met_entry_fastq_one
        self.fastq_two_metadata = met_entry_fastq_two
        self.fastq_one_metadata_path = os.path.join(
            self.fastq_dir,
            self.metadata_file_name(self.fastq_file_one)
        )

        self.fastq_two_metadata_path = os.path.join(
            self.fastq_dir,
            self.metadata_file_name(self.fastq_file_two)
        )

        met_entry_fastq_one = met_entry_fastq_one.export_as_dataframe()
        met_entry_fastq_two = met_entry_fastq_two.export_as_dataframe()

        met_entry_fastq_one.to_csv(
            self.fastq_one_metadata_path,
            sep="\t",
            index=False
        )

        met_entry_fastq_two.to_csv(
            self.fastq_two_metadata_path,
            sep="\t",
            index=False
        )

    def set_remote_files(self):
        """
        set remote files"""

        self.remote_file_one = os.path.join(
            self.remote_dir,
            "test1.fastq.gz"
        )
        self.remote_file_two = os.path.join(
            self.remote_dir,
            "test2.fastq.gz"
        )

        self.remote_metadata_one = os.path.join(
            self.remote_dir,
            self.metadata_file_name(self.fastq_file_one)
        )

        self.remote_metadata_two = os.path.join(
            self.remote_dir,
            self.metadata_file_name(self.fastq_file_two)
        )


@pytest.fixture(scope="function")
def prep_input(tmp_path_factory, remote_uploader: InsafluUploadRemote):

    ##
    input_dir = tmp_path_factory.mktemp("input")
    fastq_dir = os.path.join(
        input_dir,
        "fastq"
    )
    fastq_file_one = os.path.join(
        fastq_dir,
        "test1.fastq.gz"
    )
    fastq_file_two = os.path.join(
        fastq_dir,
        "test2.fastq.gz"
    )

    # write fastq files
    os.makedirs(fastq_dir, exist_ok=True)
    with gzip.open(fastq_file_one, "wb") as f:
        f.write(b"test")
    with gzip.open(fastq_file_two, "wb") as f:
        f.write(b"test")

    # size of fastq files
    fastq_one_size = os.path.getsize(fastq_file_one)
    fastq_two_size = os.path.getsize(fastq_file_two)

    # prep metadata

    # create Input
    temp_input = TempInput(
        input_dir=input_dir,
        fastq_dir=fastq_dir,
        remote_dir=remote_uploader.remote_dir,
        fastq_file_one=fastq_file_one,
        fastq_file_two=fastq_file_two,
        fastq_one_size=fastq_one_size,
        fastq_two_size=fastq_two_size,
    )

    return temp_input


@pytest.fixture(scope="function")
def influ_config(tmp_path_factory, prep_input: TempInput, config_file, remote_uploader: InsafluUploadRemote):

    main_insaflu = MainInsaflu()

    output_dir = tmp_path_factory.mktemp("output")

    args = ArgsClass(
        in_dir=prep_input.input_dir,
        out_dir=output_dir,
        sleep=2,
        tag="test",
        config=config_file,
        merge=True,
        upload="last",
        connect="docker",
        keep_names=False,
        monitor=False,
        televir=True

    )

    run_config = main_insaflu.setup_config(args, test=True)
    # run_config.delete_db()

    return run_config


@pytest.fixture(scope="function")
def insaflu_process(influ_config: InfluConfig):

    return TelevirFileProcess(influ_config)


@pytest.fixture(scope="function")
def televir_process(influ_config: InfluConfig):

    return TelevirFileProcess(influ_config)


@pytest.mark.docker
def test_config_file_exists(config_file):
    """
    test config file exists"""

    assert os.path.isfile(config_file)


@pytest.mark.docker
class TestInsafluUpload:

    def test_file_exists(self, remote_uploader: InsafluUploadRemote):
        """
        test file exists"""
        assert remote_uploader.check_file_exists("test") == False

    def test_upload_file(self, remote_uploader: InsafluUploadRemote, prep_input: TempInput):
        """
        test upload file"""

        remote_uploader.upload_file(prep_input.fastq_file_one,
                                    prep_input.remote_file_one)

        assert remote_uploader.check_file_exists(
            prep_input.remote_file_one) == True

        remote_uploader.clean_upload(prep_input.remote_file_one)

    def test_download_file(self, remote_uploader: InsafluUploadRemote, prep_input: TempInput):
        """
        test download file"""

        remote_uploader.upload_file(
            prep_input.fastq_file_one, prep_input.remote_file_one)

        local_path = os.path.join(
            prep_input.fastq_dir, "test_dl.fastq.gz"
        )
        remote_uploader.download_file(
            prep_input.remote_file_one, local_path)

        assert os.path.isfile(local_path) == True

        remote_uploader.clean_upload(prep_input.remote_file_one)

    def test_sample_does_not_exist_message(self, remote_uploader: InsafluUploadRemote):
        """
        test sample does not exist message"""

        assert "exists in database" not in remote_uploader.submit_sample(
            "test")
        assert "file was processed" not in remote_uploader.submit_sample(
            "test")

    def test_submit_sample(self, remote_uploader: InsafluUploadRemote, prep_input: TempInput):
        """
        test upload sample"""

        remote_uploader.upload_file(
            prep_input.fastq_file_one, prep_input.remote_file_one)

        remote_uploader.upload_file(
            prep_input.fastq_one_metadata_path, prep_input.remote_metadata_one)

        assert remote_uploader.logger.get_file_status(
            prep_input.fastq_one_metadata_path) == InsafluSampleCodes.STATUS_UPLOADED

        output = remote_uploader.submit_sample(
            prep_input.remote_metadata_one, test=True)

        success = remote_uploader.check_submission_success(output)

        assert success == True

        fake_output_error = "error"
        fake_output_exists = "exists in database"

        assert remote_uploader.check_submission_success(
            fake_output_error) == False
        assert remote_uploader.check_submission_success(
            fake_output_exists) == True

        remote_uploader.clean_upload(prep_input.remote_file_one)
        remote_uploader.clean_upload(prep_input.remote_metadata_one)

    def test_get_sample_status(self, remote_uploader: InsafluUploadRemote):
        """
        test get sample status"""

        assert remote_uploader.get_sample_status(
            "test123") == InsafluSampleCodes.STATUS_MISSING

        assert remote_uploader.get_sample_status(
            "test123", test_ready=True) == InsafluSampleCodes.STATUS_SUBMITTED

    def test_televir_launch(self, remote_uploader: InsafluUploadRemote):
        """
        test televir launch sample messages"""

        random_name = "random_name"
        random_project = "random_project"

        launch_status = remote_uploader.launch_televir_project(
            random_name, random_project, test=True)

        assert launch_status == InsafluSampleCodes.STATUS_TELEVIR_SUBMITTED

        error_status = remote_uploader.launch_televir_project(
            random_name, random_project)

        assert error_status == InsafluSampleCodes.STATUS_SUBMISSION_ERROR


@pytest.mark.docker
class TestFileProcess:

    def test_insaflu_process(self, influ_config: InfluConfig):
        """
        test insaflu process"""

        ##### test insaflu process #####

        influ_config.deploy_televir = False

        insaflu_process = InsafluFileProcess(influ_config)
        televir_process = TelevirFileProcess(influ_config)

        insaflu_process.test = True
        televir_process.test = True

        output_dir = insaflu_process.run_metadata.output_dir

        insaflu_process.run()

        televir_process.run()

        assert os.path.exists(output_dir) == True

        assert insaflu_process.processed.processed.shape[0] == 2
        assert insaflu_process.uploader.logger.log.shape[0] == 2

        assert set(['metadata_dir', 'logs', 'fastq']
                   ) == set(os.listdir(output_dir))

        assert televir_process.uploader.logger.get_file_status(
            insaflu_process.processed.processed.iloc[0].merged) == InsafluSampleCodes.STATUS_MISSING
        assert televir_process.uploader.logger.get_file_status(
            insaflu_process.processed.processed.iloc[1].merged) == InsafluSampleCodes.STATUS_SUBMITTED

    def test_televir_process(self, influ_config: InfluConfig):
        """
        test insaflu process"""
        insaflu_process = InsafluFileProcess(influ_config)
        televir_process = TelevirFileProcess(influ_config)
        insaflu_process.test = True
        televir_process.run_metadata.deploy_televir = True
        televir_process.test = True

        insaflu_process.run()
        televir_process.run()

        assert televir_process.uploader.logger.get_file_status(
            insaflu_process.processed.processed.iloc[1].merged) == InsafluSampleCodes.STATUS_TELEVIR_SUBMITTED

    def test_televir_process_all(self, influ_config: InfluConfig):
        """
        test insaflu process"""
        influ_config.upload_strategy = UploadAll
        insaflu_process = InsafluFileProcess(influ_config)
        televir_process = TelevirFileProcess(influ_config)
        televir_process.run_metadata.deploy_televir = True
        insaflu_process.test = True
        televir_process.test = True

        insaflu_process.run()
        televir_process.run()

        assert televir_process.uploader.logger.log.shape[0] == 3
