import configparser
import subprocess
import sys
from abc import ABC, abstractmethod
from typing import Optional

import paramiko


class Connector(ABC):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def test_connection(self):
        pass

    @abstractmethod
    def execute_command(self, command: str) -> str:
        pass

    @abstractmethod
    def check_file_exists(self, file_path: str) -> bool:
        pass

    @abstractmethod
    def upload_file(self, file_path: str, remote_path: str):
        pass

    @abstractmethod
    def download_file(self, file_path: str, remote_path: str):
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class ConnectorNull(Connector):

    def __init__(self):
        super().__init__()

    def test_connection(self) -> None:
        pass

    def execute_command(self, command: str) -> str:
        return ""

    def check_file_exists(self, file_path: str) -> bool:
        return False

    def upload_file(self, file_path: str, remote_path: str):
        pass

    def download_file(self, file_path: str, remote_path: str):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class ConnectorParamiko(Connector):

    def __init__(self, config_file: str) -> None:
        super().__init__()
        self.prep_input(config_file)
        self.connect()

    def input_config(self, config_file: str):

        config = configparser.ConfigParser()
        config.read(config_file)

        username = config["SSH"].get("username", None)
        ip_address = config["SSH"].get("ip_address", None)
        rsa_key_path = config["SSH"].get("rsa_key", None)

        if username is None or ip_address is None or rsa_key_path is None:
            raise ValueError("Config file is missing values")

        return username, ip_address, rsa_key_path

    def input_user(self):
        username = input("username: ")
        ip_address = input("ip_address: ")
        rsa_key_path = input("rsa_key (full path): ")

        return username, ip_address, rsa_key_path

    def prep_input(self, config_file: Optional[str] = None):
        if config_file is None:
            username, ip_address, rsa_key_path = self.input_user()
        try:
            username, ip_address, rsa_key_path = self.input_config(config_file)
        except FileNotFoundError:
            print("Config file not found, please input manually")
            username, ip_address, rsa_key_path = self.input_user()

        self.username = username
        self.ip_address = ip_address
        self.rsa_key_path = rsa_key_path

    def connect(self):

        paramiko_rsa_key = paramiko.RSAKey.from_private_key_file(
            self.rsa_key_path)
        self.rsa_key = paramiko_rsa_key

        self.conn = paramiko.SSHClient()
        self.conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.test_connection()

    def __enter__(self):

        try:

            self.conn.connect(
                hostname=f"{self.ip_address}",
                username=f"{self.username}",
                pkey=self.rsa_key
            )

        except paramiko.ssh_exception.SSHException as error:
            print("SSH connection error")
            sys.exit(1)

        except KeyboardInterrupt:
            print("Keyboard interrupt")
            sys.exit(1)

        except Exception as error:
            print("Unknown error")
            print(error)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def test_connection(self) -> None:
        """
        test using paramiko using rsa key. If successful, close connection, else exit
        """

        try:
            self.conn.connect(
                hostname=f"{self.ip_address}",
                username=f"{self.username}",
                pkey=self.rsa_key
            )
            self.conn.close()
        except Exception:
            print("Authentication failed, please verify your credentials")
            sys.exit(1)

    def execute_command(self, command: str) -> str:
        """
        execute command using paramiko"""

        with self as conn:

            stdin, stdout, stderr = self.conn.exec_command(command)
            stdout = stdout.read().decode("utf-8")

            return stdout

    def check_file_exists(self, file_path: str) -> bool:
        """
        check file exists using paramiko"""

        with self as conn:
            stdin, stdout, stderr = self.conn.exec_command(f"ls {file_path}")

            if len(stdout.read().decode("utf-8")) > 0:
                if "cannot access" in stdout.read().decode("utf-8"):
                    return False

                return True

            return False

    def upload_file(self, file_path: str, remote_path: str):
        """
        upload file using paramiko"""
        with self as conn:
            ftp_client = self.conn.open_sftp()
            ftp_client.put(file_path, remote_path)
            ftp_client.close()

    def download_file(self, remote_path: str, file_path: str):
        """
        download file using paramiko"""
        with self as conn:
            ftp_client = self.conn.open_sftp()
            ftp_client.get(remote_path, file_path)
            ftp_client.close()


class ConnectorDocker(Connector):
    """
    Connector for docker containers
    """
    config_image_name = "image"
    televir_username = "flu_user"

    def __init__(self, config_file: str) -> None:
        super().__init__()
        self.prep_input(config_file)
        self.set_interactive(True)

    def set_interactive(self, interactive: bool):
        if interactive:
            self.interactive = "-it"
        else:
            self.interactive = ""

    def input_config(self, config_file: str):
        """
        read config file for docker"""

        config = configparser.ConfigParser()
        config.read(config_file)

        server_name = config["DOCKER"].get(self.config_image_name, None)

        if server_name is None:
            raise ValueError("Config file is missing values")

        return server_name

    def input_user(self):
        """
        input user for docker"""
        server_name = input("server name: ")

        return server_name

    def prep_input(self, config_file: Optional[str] = None):
        """
        prepare input for docker"""
        if config_file is None:
            server_name = self.input_user()
        try:
            server_name = self.input_config(config_file)
        except FileNotFoundError:
            print("Config file not found, please input manually")
            server_name = self.input_user()

        self.server_name = server_name

    def test_connection(self) -> None:
        """
        test using paramiko using rsa key. If successful, close connection, else exit
        """

        try:
            bash_command = f"docker exec {self.interactive} {self.server_name} echo 'test'"

            process = subprocess.Popen(
                bash_command.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()

            if error is not None:
                raise Exception

        except Exception:
            print("Authentication failed, please verify your credentials")
            sys.exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute_command(self, command: str) -> str:
        """
        execute command using in docker container"""

        command = f'su - {self.televir_username} -c "{command}"'

        bash_command = f"docker exec {self.interactive} {self.server_name} {command}"

        # SUBMIT COMMAND STRING WITHOUT SPLITTING
        process = subprocess.Popen(
            bash_command, stdout=subprocess.PIPE, shell=True)

        output, error = process.communicate()

        return output.decode("utf-8")

    def check_file_exists(self, file_path: str) -> bool:
        """
        check file exists in docker container"""

        bash_command = f"docker exec {self.interactive} {self.server_name} ls {file_path}"

        process = subprocess.Popen(
            bash_command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

        if len(output.decode("utf-8")) > 0:
            if "cannot access" in output.decode("utf-8"):
                return False

            return True

        return False

    def upload_file(self, file_path: str, remote_path: str):
        """
        upload file using docker cp"""

        bash_command = f"docker cp {file_path} {self.server_name}:{remote_path}"

        process = subprocess.Popen(
            bash_command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

    def download_file(self, file_path: str, remote_path: str):
        """
        download file using docker cp"""

        bash_command = f"docker cp {self.server_name}:{file_path} {remote_path}"

        process = subprocess.Popen(
            bash_command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
