#!/bin/env sh

export LD_LIBRARY_PATH=$(realpath ../target/debug/deps):${LD_LIBRARY_PATH}
export LIBRARY_PATH=$(realpath ../target/debug/deps):${LIBRARY_PATH}
