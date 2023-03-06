#!/bin/env sh

export LD_LIBRARY_PATH=$(realpath ../target/release/deps):${LD_LIBRARY_PATH}
export LIBRARY_PATH=$(realpath ../target/release/deps):${LIBRARY_PATH}
