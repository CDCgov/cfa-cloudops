#!/bin/bash
python -m cProfile -o /app/cliques/output.prof bk_algorithm.py -n $1
python upload_files.py
