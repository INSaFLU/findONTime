
from typing import List, Optional

from sqlalchemy import (ARRAY, TIMESTAMP, Column, Integer, MetaData, String,
                        Table, create_engine)
from sqlalchemy.orm import registry, sessionmaker

from findontime.records import InsafluFile


class InsafluTables:

    def __init__(self, db_path: str) -> None:
        """
        Setup the database
        """
        self.db_path = db_path

        self.engine = None

    @property
    def is_connected(self) -> bool:
        """
        Check if the database is connected
        """
        return self.engine is not None

    def setup(self):
        self._setup_engine(self.db_path)

        self.insaflu_files = InsafluFilesTable(self.engine)

    def _setup_engine(self, db_path: str):
        """
        Setup the engine for the database"""
        self.engine = create_engine(f"sqlite:///{db_path}")

    def drop_database(self):
        """
        Drop the database
        """

        self.engine.dispose()


class InsafluFilesTable:

    def __init__(self, engine) -> None:
        """
        Setup the database
        """
        self.engine = engine

        self._create_table()

    def _create_table(self):
        """
        Create the table if it doesn't exist"""
        metadata = MetaData()
        mapper_registry = registry(metadata=metadata)
        insaflu_table = Table(
            "insaflu_files",
            mapper_registry.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("sample_id", String),
            Column("barcode", String),
            Column("file_path", String),
            Column("remote_path", String),
            Column("status", Integer),
        )

        mapper_registry.map_imperatively(InsafluFile, insaflu_table)

        metadata.create_all(self.engine)

    def _get_session(self):
        """
        Get a session for the database"""
        Session = sessionmaker(bind=self.engine)
        return Session()

    def _get_sample(self, sample_id: str, barcode: str, file_path: str, remote_path: str) -> Optional[InsafluFile]:
        """
        Get a sample from the database"""

        session = self._get_session()
        sample = session.query(InsafluFile).filter_by(
            sample_id=sample_id, barcode=barcode, file_path=file_path, remote_path=remote_path).first()
        session.close()
        return sample

    def get_sample(self, sample_id: str, barcode: str, file_path: str, remote_path: str) -> Optional[InsafluFile]:
        """
        Get a sample from the database"""

        return self._get_sample(sample_id, barcode, file_path, remote_path)

    def get_sample_by_id(self, sample_id: str) -> Optional[InsafluFile]:
        """
        Get a sample from the database"""

        return self._get_sample(sample_id, "", "", "")

    def add_sample(self, sample: InsafluFile):
        """
        Add a sample to the database"""

        if self.check_sample_in_db(sample):
            return

        session = self._get_session()
        session.add(sample)
        session.commit()
        session.close()

    def check_sample_in_db(self, insaflu_file: InsafluFile) -> bool:
        """
        Check if a sample is in the database"""

        sample = self.get_sample(insaflu_file.sample_id, insaflu_file.barcode,
                                 insaflu_file.file_path, insaflu_file.remote_path)
        return sample is not None
