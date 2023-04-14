# findONTime

[![PyPI version](https://badge.fury.io/py/findontime.svg)](https://badge.fury.io/py/findontime)
[![PyPI version](https://img.shields.io/pypi/pyversions/findontime.svg)](https://pypi.python.org/pypi/findontime/)
[![PyPI version](https://img.shields.io/pypi/format/findontime.svg)](https://pypi.python.org/pypi/findontime/)

The **findONTime** tool **runs concurrently with MinION sequencing** and **merges (at user defined time intervals) the FASTQ files** that are being generated in real-time for each sample. It can also automatically **upload the files the INSaFLU-TELEVIR platform** (https://insaflu.insa.pt/) and launch the **metagenomics virus detection** analysis using the TELEVIR module.

## Motivation

Reducing the time needed for pathogen detection and the sequencing costs per sample is crucial in the context of diagnostics using metagenomics sequencing. In fact, when performing hypothesis-free viral diagnosis by sequencing complex biological samples, the proportion of the virus in a sample is unknown. As such, the amount of sequencing data, and consequently run length, needed to accurately detect the virus cannot be predicted a priori.

**findONTime runs concurrently with MinION sequencing and monitors the FASTQ files that are being generated in real-time for each sample, merges the files (at user defined time intervals), uploads them to the INSaFLU-TELEVIR platform and launches the metagenomics virus detection analysis using the [TELEVIR module](https://insaflu.readthedocs.io/en/latest/metagenomics_virus_detection.html)**.

This allows users to **detect a virus in a sample as early as possible during the sequencing run**, reducing the time gap between obtaining the sample and the diagnosis, and also **reducing sequencing costs** (as ONT runs can be stopped at any time and the flow cells can be cleaned and reused). **findONTime** can be used as a “start-to-end” solution or for particular tasks (e.g., merging ONT output files, metadata preparation and upload to INSaFLU-TELEVIR).

## Details

- It **runs concurrently with MinION sequencing** and **merges (at user defined time intervals) the FASTQ files** that are being generated in real-time for each sample. For this, it relies on [fastq-handler](https://pypi.org/project/fastq-handler/), a package to process ONT fastq files by concatenating reads as they are generated during a sequencing run.

- It can also automatically **upload the files the INSaFLU-TELEVIR platform** (docker installation or local server) and launch the **metagenomics virus detection** analysis using the TELEVIR module.

- The user has the option to upload all files collected throughout the ONT run (sampling occurs at user-defined period) or only upload the lastest file (i.e, the file compiling all reads generated until the lastest sampling point).

- For upload, metadata files are also generated for each sequence file, according to the INSaFLU-TELEVIR input template file. Metadata files are stored in the metadata sub-directory following the output directory specified by the user.

### Upload reads to INSaFLU-TELEVIR

_findONTime_ can interact with the INSaFLU-TELEVIR platfotm in two ways:

- **Docker**. The user needs to have the INSaFLU-TELEVIR docker installed and running. The tool will then upload the files to the docker image. The user needs to provide the name of the docker image and the path for uploads.

- **SSH**. The user needs to have access to an INSaFLU-TELEVIR database server. The tool will then upload the files to the database using SSH. The user needs to provide the path for uploads and the credentials for the database server.

**Note**: Automatic upload to the [INSaFLU-TELEVIR website](https://insaflu.insa.pt/) accounts is not available yet. If you only have an online account (and not a local INSaFLU installation), findONTime will be run concurrently with MinION sequencing to monitor and concatenate the FASTQ files that are being generated in real-time for each sample and prepare metadata templates ready to be upload to INSaFLU-TELEVIR.

### Launch a virus detection analysis (TELEVIR)

If requested the tool creates one INSaFLU-TELEVIR project for each inpp directory containing fastq files. The project name is the name of the directory. Files generated within the same directory are uploaded to the same project.

If you have a local INSaFLU-TELEVIR installation (docker or server), and set the "--televir argument", findONTtimeThe tool can creates one INSaFLU-TELEVIR (virus detection) project including the samples under ONT sequencing. The project name is defined by the user (--tag argument) and the sample names are the ones of the input directory (usually barcode01, barcode02, etc) with an extra user-defined tag as suffix.

### Input Files

- **fastq.gz** - Output directory for the ONT run, containing sequence files. The files can be in subfolders. The files can be gzipped or not.

- **config.ini** - A configuration file containing the parameters for the tool. The file is generated by the tool when it is run for the first time. The user can edit the file to change the parameters.

Config must contain:

- section [INSAFLU] containing insaflu username and app directory path.

- (optional) section [SSH] containing ssh credentials: username, ip_address and rsa key;

- (optional) section [DOCKER] containing docker image name.

see example [config.ini](config.ini)

## API

```bash

usage: findontime [-h] -i IN_DIR -o OUT_DIR [-s SLEEP] [-n TAG] [--config CONFIG] [--max_size MAX_SIZE] [--merge] [--downsize] [--upload {last,all,none}] [--connect {docker,ssh}] [--keep_names] [--monitor] [--televir]

Process fastq files.

optional arguments:
  -h, --help            show this help message and exit
  -i IN_DIR, --in_dir IN_DIR
                        Input directory
  -o OUT_DIR, --out_dir OUT_DIR
                        Output directory
  -s SLEEP, --sleep SLEEP
                        Sleep time between checks in monitor mode (default 600 seconds)
  -n TAG, --tag TAG     name tag, if given, will be added to the output file names
  --config CONFIG       config file
  --max_size MAX_SIZE   max size of the output file, in kilobytes (default 400000 kbytes)
  --merge               merge files
  --downsize            downsize fastq files to max_size
  --upload {last,all,none}
                        file upload strategy (default: last)
  --connect {docker,ssh}
                        file upload strategy (default: docker)
  --keep_names          keep original file names
  --monitor             monitor directory until killed
  --televir             deploy televir pathogen identification on each sample

```

### REQUIREMENTS

- **python 3.6** or higher
- dataclasses==0.6
- natsort==8.3.1
- pandas==1.5.3
- paramiko==3.1.0
- pip==21.2.3
- setuptools==57.4.0
- xopen==1.7.0

### INSTALLATION

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install findontime
```

### USAGE

_(from simple to more advanced usage situations)_

- Example 1. **Merge fastq files from an ONT run.**

```bash
findontime -i input_directory -o output_directory --tag suffix --max_size 100000000 --merge --upload none

```

_NOTE: In this simpler usage case, the fastq.gz files will only be only merged, i.e., they will not be automatically uploaded to the INSaFLU-TELEVIR platform. In case you want to concatenate all ONT same-sample files (file_0.fastq.gz, file_1.fastq.gz, etc), make sure you set up a "max_size" (e.g., 100000000 kbytes) enough to ensure that the merged file compiles all partial files._

- Example 2. **Merge fastq files generated during a ONT run at every 10 min (600 seconds).**

```bash
findontime -i input_directory -o output_directory --tag suffix -s 600 --max_size 100000000 --monitor --merge --upload none

```

_NOTE: In this simpler usage case, the fastq.gz files will only be only merged, i.e., they will not be automatically uploaded to the INSaFLU-TELEVIR platform. In case you want to concatenate all ONT same-sample files (file_0.fastq.gz, file_1.fastq.gz, etc), make sure you set up a "max_size" (e.g., 100000000 kbytes) enough to ensure that the merged file compiles all partial files._

- Example 3. **Merge fastq files generated during a ONT run at every 10 min (600 seconds), downsize the merged file to 400 MB (ready to be uploaded to INSaFLU-TELEVIR).**

```bash
findontime -i input_directory -o output_directory --tag suffix -s 600 --max_size 400000 --monitor --merge --upload none --downsize

```

_NOTE: In this case, the fastq.gz files will only be concatenated (i.e., they will not be automatically uploaded to the INSaFLU-TELEVIR platform), but the merged files will be downsized to the user-defined "max_sixe" (e.g., 400000 kbytes). This usage is useful to prepare files ready to be uploaded to the online [INSaFLU-TELEVIR platform](https://insaflu.insa.pt/), which is currently limited to an upload max size per file of 400 MB._

- Example 4. **Merge fastq files generated during a ONT run at every 10 min (600 seconds) and upload them to INSaFLU-TELEVIR**

```bash
findontime -i input_directory -o output_directory --tag suffix -s 600 --max_size 400000 --monitor --connect docker --merge --upload last --connect docker

```

_NOTE: In this case, the fastq.gz files will be concatenated and automatically uploaded to the INSaFLU-TELEVIR platform. The merged files will be downsized to the user-defined "max_sixe" (e.g., 400000 kbytes), which must be fitted to the maximum upload file size defined in your local INSaFLU_TELEVIR installation_

- Example 5. **Merge fastq files generated during a ONT run at every 10 min (600 seconds), upload them to INSaFLU-TELEVIR and run a virus detection project**

```bash
findontime -i input_directory -o output_directory --tag suffix -s 600 --max_size 400000 --monitor --connect docker --merge --upload last –-televir

```

_NOTE: In this case, the fastq.gz files will be concatenated, automatically uploaded to the INSaFLU-TELEVIR platform and run under a virus detection (TELEVIR) project. The merged files will be downsized to the user-defined "max_sixe" (e.g., 400000 kbytes), which must be fitted to the maximum upload file size defined in your local INSaFLU_TELEVIR installation_

### TESTING

Running pytest in the root directory will run all tests that do not interact with INSaFLU-TELEVIR. In order to test the upload and metagenomics functionalities, the user needs to provide a valid config file to a local docker installation, and to pass the `--docker` flag to pytest:

```bash

pytest --docker --config-file config.ini

```

### MAIN OUTPUT

> **Note:** The output directory structure is maintained.

- **fastq.gz** files containing all reads from the previous files.
- **log.txt** file containing the concatenation process.
- **metadata** individual metadata files for each fastq file uploaded.
- **results.tsv** file containing the main results of the TELEVIR pathogen detection. One file per project.

## Maintainers

- [**@santosjgnd**](https://github.com/SantosJGND)
- [**@insaflu**](https://github.com/insapathogenomics)
