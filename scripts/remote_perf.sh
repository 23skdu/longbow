#!/bin/bash
ssh ancalagon "cd REPOS/longbow && git fetch && git reset --hard origin/main && make clean build && bash scripts/remote_runner.sh"
