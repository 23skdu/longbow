#!/bin/bash
# Comprehensive remote benchmark on Ancalagon
ssh ancalagon "cd REPOS/longbow && nohup venv/bin/python3 scripts/full_matrix_runner.py cuda ancalagon > ancalagon_run.log 2>&1 &"
echo "Remote benchmark started on Ancalagon. Tail ancalagon_run.log on the server to monitor."
