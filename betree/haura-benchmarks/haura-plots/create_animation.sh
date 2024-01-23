#!/bin/env bash

pushd $1

if [ -e plot_timestep_000.png ]
then
    ffmpeg -framerate 2 -i plot_timestep_%03d.png -c:v libx264 -pix_fmt yuv420p plot_timestep.mp4
fi

popd
