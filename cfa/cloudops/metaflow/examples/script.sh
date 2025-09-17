#!/bin/bash
python -m cProfile -o /app/cliques/output.prof /app/bk_algorithm.py -n $1
python /app/upload_files.py
