
import datetime
import logging
import os
import sys
from typing import List

import pandas as pd
from fastq_handler.fastq_handler import DirectoryProcessingSimple, PreMain
from fastq_handler.records import Processed

from findontime.configs import InfluConfig, default_log_handler
from findontime.plot_utils import plot_project_results
from findontime.records import InsafluFile, MetadataEntry
from findontime.upload_utils import InsafluSampleCodes, InsafluUpload


class InfluProcessed(Processed):
    """
    TelevirProcessed class
    """

    def __init__(self, output_dir: str):
        super().__init__(output_dir)

    def generate_metadata_entry_row(self, row: pd.Series):
        """
        create tsv
        """
        fastq_dir = row.dir
        fastq_file = row.fastq
        merged_file = row.merged

        new_entry = self.generate_metadata_entry(
            fastq_file, fastq_dir, merged_file)

        return new_entry.export_as_dataframe()

    def generate_metadata_entry(self, fastq_file: str, fastq_dir: str, merged_file: str, tag=""):

        sample_name, _ = self.get_run_barcode(
            merged_file, fastq_dir)

        time_elapsed = self.get_file_time(
            fastq_file=fastq_file,
            fastq_dir=fastq_dir
        )
        new_entry = MetadataEntry(
            sample_name=sample_name,
            fastq1=merged_file,
            time_elapsed=float(time_elapsed),
            fdir=os.path.dirname(merged_file),
            tag=tag
        )

        return new_entry

    def generate_metadata(self):
        """
        create tsv
        """
        metadata = pd.DataFrame()

        for index, row in self.processed.iterrows():
            metadata = pd.concat(
                [metadata, self.generate_metadata_entry_row(row)])

        return metadata

    def export_metadata(self, run_metadata: InfluConfig):
        """
        export metadata
        """
        influ_metadata = self.generate_metadata()

        influ_metadata.to_csv(
            os.path.join(
                run_metadata.metadata_dir,
                run_metadata.tsv_temp_name
            ),
            sep="\t",
            index=False,
        )


class InfluDirectoryProcessing(DirectoryProcessingSimple):
    """
    TelevirDirectoryProcessing class
    replace directory gen to include metadata
    """

    metadata_suffix: str = "_metadata.tsv"

    uploader: InsafluUpload

    def __init__(self, fastq_dir: str, run_metadata: InfluConfig, processed: InfluProcessed,
                 start_time: float, test: bool = False):
        super().__init__(fastq_dir, run_metadata, processed, start_time)

        self.run_metadata = run_metadata
        self.processed = processed
        self.uploader = run_metadata.uploader
        self.test = test

    def prep_output_dirs(self):
        """create output dirs"""

        for outdir in [
            self.merged_gz_dir,
        ]:
            os.makedirs(outdir, exist_ok=True)

    @staticmethod
    def get_filename_from_path(file_name):
        filename = os.path.basename(file_name)
        filename, ext = os.path.splitext(filename)
        if ext == ".gz":
            filename, ext = os.path.splitext(filename)
        return filename

    def metadata_name_for_sample(self, sample_name):
        filename = self.get_filename_from_path(sample_name)

        return filename + self.metadata_suffix

    def register_sample(self, metadata_entry: MetadataEntry):
        """
        register sample to remote server"""

        if self.uploader.check_file_exists(metadata_entry.r1_local):
            return

        _, barcode = self.processed.get_run_barcode(
            metadata_entry.fastq1, self.fastq_dir)

        sample_id = self.processed.get_sample_id_from_merged(
            metadata_entry.fastq1
        )

        self.uploader.register_sample(
            metadata_entry.r1_local,
            sample_id=sample_id,
            barcode=barcode,
        )

    def upload_sample(self, metadata_entry: MetadataEntry):
        """
        upload sample to remote server"""

        _, barcode = self.processed.get_run_barcode(
            metadata_entry.fastq1, self.fastq_dir)

        sample_id = self.processed.get_sample_id_from_merged(
            metadata_entry.fastq1
        )

        self.uploader.upload_sample(
            metadata_entry.r1_local,
            sample_id=sample_id,
            barcode=barcode,
        )

    def insaflu_process(self):
        """
        prepare processed files for upload
        """

        files_to_upload = self.processed.processed_fastq_list()

        processed_df = self.processed.processed_entries

        for ix, row in processed_df.iterrows():

            fastq_file = row.fastq
            merged_file = row.merged
            merged_name = self.get_filename_from_path(merged_file)

            if self.run_metadata.upload_strategy.is_to_upload(files_to_upload, ix):
                status = self.uploader.get_sample_status(
                    merged_name, test_missing=self.test)

                metadata_entry = self.processed.generate_metadata_entry(
                    fastq_file, self.fastq_dir, merged_file, tag=self.run_metadata.name_tag
                )

                self.register_sample(metadata_entry)

                if status in [
                    InsafluSampleCodes.STATUS_MISSING,
                    InsafluSampleCodes.STATUS_ERROR,
                ]:

                    self.upload_sample(
                        metadata_entry
                    )

    def process_folder(self):
        """
        process folder, merge and update metadata
        submit to televir only the last file.
        """
        super().process_folder()
        self.insaflu_process()


class InsafluSetup:
    """
    InsafluSetup class"""

    def __init__(self, run_metadata: InfluConfig):
        self.run_metadata = run_metadata
        self.uploader = run_metadata.uploader
        self.processed = InfluProcessed(
            self.run_metadata.logs_dir,
        )


class PreMainWithMetadata(PreMain):
    """
    """
    metadata_dirname = "metadata_dir"

    processed: InfluProcessed

    def __init__(self, run_metadata: InfluConfig):
        super().__init__(run_metadata)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(default_log_handler)

        self.prep_metadata_dir()
        self.run_metadata = run_metadata
        self.processed = InfluProcessed(
            self.run_metadata.logs_dir,
        )

    def prep_metadata_dir(self):
        """
        create output directories
        """

        os.makedirs(self.run_metadata.metadata_dir, exist_ok=True)

    def write_metadata(self, metadata: List[MetadataEntry], metadata_filename: str):
        """
        update metadata
        """

        metadata_df = [
            x.export_as_dataframe() for x in metadata
        ]

        metadata_df = pd.concat(metadata_df)

        metadata_df.to_csv(
            metadata_filename,
            sep="\t",
            index=False,
        )

    def metadata_from_files(self, files_list: List[InsafluFile]) -> List[MetadataEntry]:
        """
        generate metadata from files
        """
        metadata_list = [
            self.processed.generate_metadata_entry(
                x.file_path,
                os.path.dirname(x.file_path),
                x.remote_path,
                tag=self.run_metadata.name_tag
            ) for x in files_list
        ]
        return metadata_list

    def generate_metatadata_filename(self):
        """
        generate metadata filename
        """
        time_now = datetime.datetime.now()
        formatted_time = time_now.strftime("%Y%m%d_%H%M%S")

        metadata_filename = f"{self.run_metadata.name_tag}_{formatted_time}_metadata.tsv"
        return metadata_filename

    def get_samples_to_submit(self):
        """
        get samples to submit
        """
        files_to_upload = self.processed.processed.fastq.tolist()
        merged_files = self.processed.processed.merged.tolist()
        dirs = self.processed.processed.dir.tolist()

        insaflu_file_list = []
        for i, fastq1 in enumerate(files_to_upload):

            _, barcode = self.processed.get_run_barcode(fastq1, self.fastq_dir)
            sample_id = self.processed.get_sample_id_from_merged(
                merged_files[i]
            )

            insaflu_file_list.append(
                InsafluFile(
                    sample_id=sample_id,
                    barcode=barcode,
                    file_path=os.path.join(dirs[i], fastq1),
                    remote_path=merged_files[i],
                    status=0)
            )

        return insaflu_file_list

    def metadata_prepare(self):
        """
        submit sample to remote server"""

        samples_to_submit = self.get_samples_to_submit()
        sample_metadata = self.metadata_from_files(samples_to_submit)

        if len(sample_metadata) == 0:
            return ""

        self.logger.info(f"Submitting {len(sample_metadata)} sample(s)")

        insaflu_metadata_file = self.generate_metatadata_filename()
        metadata_filepath = os.path.join(
            self.run_metadata.metadata_dir,
            insaflu_metadata_file
        )

        self.write_metadata(
            sample_metadata,
            metadata_filepath
        )

        return metadata_filepath

    def run(self):
        """
        run main
        """
        super().run()
        metadata_filepath = self.metadata_prepare()

        return metadata_filepath


class InsafluFileProcess(PreMainWithMetadata, InsafluSetup):
    """
    InsafluUpload class
    """

    processed: InfluProcessed
    run_metadata: InfluConfig
    projects_results: list = []
    metadata_dirname = "metadata_dir"

    def __init__(self, run_metadata: InfluConfig):
        PreMainWithMetadata.__init__(self, run_metadata)
        InsafluSetup.__init__(self, run_metadata)

        self.test = False

    def update_projects(self, project_file: str):
        """
        update project added
        """
        if project_file not in self.projects_results:
            if os.path.exists(project_file):
                self.projects_results.append(project_file)

    def get_directory_processing(self, fastq_dir: str):

        return InfluDirectoryProcessing(fastq_dir, self.run_metadata, self.processed,
                                        self.start_time, test=self.test)

    def get_samples_to_submit(self) -> List[InsafluFile]:
        """
        get samples to submit
        """
        samples_to_submit = self.uploader.logger.generate_fastq_list_status(
            InsafluSampleCodes.STATUS_UPLOADED)

        return samples_to_submit

    def clean_remote(self):
        """
        clean remote server
        """
        samples_to_clean = self.uploader.logger.generate_file_list_status(
            InsafluSampleCodes.STATUS_SUBMITTED)

        for file in samples_to_clean:
            self.uploader.clean_upload(file.remote_path)

    def submit_samples(self, metadata_filepath: str):
        """
        submit samples
        """

        if not os.path.exists(metadata_filepath):
            return

        insaflu_metadata_file = os.path.basename(metadata_filepath)

        self.uploader.upload_file(
            metadata_filepath,
            self.uploader.get_remote_path(insaflu_metadata_file),
            "metad",
            "metad",
            self.uploader.TAG_METADATA,
        )

        status = self.uploader.logger.get_file_status(metadata_filepath)

        if status == InsafluSampleCodes.STATUS_UPLOADED:

            self.uploader.submit_sample_update(
                metadata_filepath, test=self.test
            )

    def export_global_metadata(self):

        self.prep_metadata_dir()
        self.processed.export_metadata(
            self.run_metadata
        )

    def save_to_db(self):
        """
        save to db
        """
        if self.run_metadata.tables.is_connected:
            self.uploader.logger.save_entries_to_db(
                self.run_metadata.tables.insaflu_files)

    def monitor_samples_status(self):
        """
        process samples
        """

        fastq_list = self.uploader.logger.generate_fastq_list()

        for fastq in fastq_list:

            file_name, _ = self.processed.get_run_info(fastq.file_path)

            _ = self.uploader.update_sample_status_remote(
                file_name, fastq.file_path, test=self.test)

    def run(self):
        PreMainWithMetadata.run(self)
        metadata_filepath = self.metadata_prepare()
        self.submit_samples(metadata_filepath)
        self.clean_remote()
        self.monitor_samples_status()
        self.export_global_metadata()
        self.save_to_db()


class TelevirFileProcess(InsafluSetup):
    """
    InsafluUpload class
    """

    processed: InfluProcessed
    run_metadata: InfluConfig
    projects_results: list = []
    output_dirname: str = "televir_results"
    output_dir: str = "metagen"

    def __init__(self, run_metadata: InfluConfig):
        super().__init__(run_metadata)

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(default_log_handler)

        self.real_sleep = self.run_metadata.sleep_time
        self.prep_output_dir()

        self.test = False

    def prep_output_dir(self):
        """
        create output dir
        """
        if self.run_metadata.deploy_televir:

            self.output_dir = os.path.join(
                self.run_metadata.output_dir,
                self.output_dirname
            )

            os.makedirs(
                self.output_dir, exist_ok=True
            )

    def update_projects(self, project_file: str):
        """
        update project added
        """
        if project_file not in self.projects_results:
            if os.path.exists(project_file):
                self.projects_results.append(project_file)

    def deploy_televir_sample(self, sample_id, file_name, file_path, project_name):
        """
        deploy televir sample
        """

        status = self.uploader.get_sample_status(
            file_name, test_ready=self.test)

        if status == InsafluSampleCodes.STATUS_SUBMITTED:

            status = self.uploader.deploy_televir_sample(
                sample_id,
                file_name,
                file_path,
                project_name,
                test=self.test
            )

    def assign_project_name(self, insaflu_file: InsafluFile):
        """
        assign project name
        """
        project_name = self.run_metadata.name_tag

        return project_name

    def deploy_televir_batch(self):
        """
        deploy televir batch
        """
        self.logger.info("Deploying televir batch")

        fastq_list = self.uploader.logger.generate_fastq_list_status(
            InsafluSampleCodes.STATUS_SUBMITTED)

        for fastq in fastq_list:
            project_name = self.assign_project_name(fastq)

            file_name, _ = self.processed.get_run_info(fastq.file_path)
            self.deploy_televir_sample(
                fastq.sample_id,
                file_name,
                fastq.file_path,
                project_name,
            )

    def download_project_results(self):
        """
        download project results
        """
        self.logger.info("Downloading project results")

        for sample_id in self.uploader.logger.available_samples:

            if InsafluSampleCodes.STATUS_TELEVIR_SUBMITTED in self.uploader.logger.get_sample_status_set(sample_id):

                project_file = os.path.join(
                    self.output_dir,
                    sample_id + ".tsv"
                )

                self.uploader.get_project_results(
                    sample_id, project_file, test=self.test
                )

                self.update_projects(project_file)

    def run(self):

        if self.run_metadata.deploy_televir:

            self.deploy_televir_batch()

            self.download_project_results()

            self.logger.info("Plotting results")
            _ = plot_project_results(
                self.projects_results, self.processed.processed_entries, self.output_dir)

    def run_return_plot(self):

        if self.run_metadata.deploy_televir:

            self.deploy_televir_batch()

            self.download_project_results()
            return plot_project_results(
                self.projects_results,  self.processed.processed_entries, self.run_metadata.output_dir, write_html=False)

        return None
