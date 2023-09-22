git clone https://github.com/INSaFLU/findONTime.git

cd findONTime

python -m venv .poetry_venv
source .poetry_venv/bin/activate

python -m pip install poetry findontime

poetry run findontime -i ../../DATA/METAGENOMICS/mpox_fastq_run1/ -o findontime_test -s 5 --max_size 600000 --upload all --connect ssh --keep_names --monitor --televir --merge --tag findONTime_demo