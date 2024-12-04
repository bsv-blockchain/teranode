#!/bin/sh
# exec go install
# Start Jupyter Notebook as root (required to listen on all IPs and potentially perform root operations)
exec /venv/bin/jupyter notebook --ip=0.0.0.0 --allow-root --NotebookApp.token='' --NotebookApp.password=''
