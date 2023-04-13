import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Type

import pandas as pd


class InsafluSampleCodes:
    STATUS_MISSING = 0
    STATUS_UPLOADING = 1
    STATUS_UPLOADED = 2
    STATUS_SUBMITTED = 3
    STATUS_TELEVIR_SUBMITTED = 4
    STATUS_PROCESSING = 5
    STATUS_PROCESSED = 6
    STATUS_SUBMISSION_ERROR = 7
    STATUS_ERROR = 8


@dataclass(frozen=False)
class InsafluFile:
    sample_id: str
    barcode: str
    file_path: str
    remote_path: str
    status: int

    def __post_init__(self):
        self.sample_id = self.sample_id.strip()
        self.barcode = self.barcode.strip()
        self.file_path = self.file_path.strip()
        self.remote_path = self.remote_path.strip()
        self.status = int(self.status)

    def __str__(self):
        return f"{self.sample_id},{self.barcode},{self.file_path},{self.remote_path},{self.status}"

    def __repr__(self):
        return f"{self.sample_id},{self.barcode},{self.file_path},{self.remote_path},{self.status}"

    def __eq__(self, other):
        return self.sample_id == other.sample_id and \
            self.barcode == other.barcode and \
            self.file_path == other.file_path and \
            self.remote_path == other.remote_path and \
            self.status == other.status

    def __hash__(self):
        return hash((self.sample_id, self.barcode, self.file_path, self.remote_path, self.status))


class MetadataEntry():
    """
    MetadataEntry class
    """

    sample_name: str
    fastq1: str
    fastq2: str
    data_set: str
    vaccine_status: str
    week: str
    onset_date: str
    collection_date: str
    lab_reception_date: str
    latitude: str
    longitude: str
    region: str
    country: str
    division: str
    location: str
    time_elapsed: float
    dir: str
    tag: str
    r1_local: str
    r2_local: str

    def __init__(self, sample_name: str, fastq1: str, fastq2: str = "", data_set: str = "", vaccine_status: str = "", week: str = "", onset_date: str = "", collection_date: str = "", lab_reception_date: str = "",
                 latitude: str = "", longitude: str = "", region: str = "", country: str = "", division: str = "", location: str = "", time_elapsed: float = 0, fdir: str = "", tag: str = ""):
        self.sample_name = sample_name
        self.fastq1 = os.path.basename(fastq1)
        self.fastq2 = os.path.basename(fastq2)
        self.data_set = data_set
        self.vaccine_status = vaccine_status
        self.week = week
        self.onset_date = onset_date
        self.collection_date = collection_date
        self.lab_reception_date = lab_reception_date
        self.latitude = latitude
        self.longitude = longitude
        self.region = region
        self.country = country
        self.division = division
        self.location = location
        self.time_elapsed = time_elapsed
        self.dir = fdir
        self.tag = tag
        self.r1_local = fastq1
        self.r2_local = fastq2

    def export_as_dataframe(self):

        metad = pd.DataFrame([self.__dict__])

        metad = metad.rename(columns={
            "sample_name": "sample name",
            "vaccine_status": "vaccine status",
            "onset_date": "onset date",
            "collection_date": "collection date",
            "data_set": "data set",
            "lab_reception_date": "lab reception date",
        })

        return metad
