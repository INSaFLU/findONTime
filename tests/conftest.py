
import os

import pytest


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--config-file",
        action="store",
        default="config.ini",
        help="Path to config file",
    )
    parser.addoption(
        "--docker", action="store_true", default=False, help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "docker: run test using functional insaflu.")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--docker"):

        if os.path.exists(config.getoption("--config-file")):
            # run docker tests if config file exists and --docker option is set.

            return

        else:
            # skip docker tests if config file does not exist and --docker option is set.
            skip_docker = pytest.mark.skip(reason="config file does not exist")
            # print message that tests will be skipped

            print("config file does not exist, skipping docker tests.")

            for item in items:
                if "docker" in item.keywords:
                    item.add_marker(skip_docker)

    skip_config = pytest.mark.skip(reason="need --docker option to run")
    for item in items:
        if "docker" in item.keywords:
            item.add_marker(skip_config)
