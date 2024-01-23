# haura-plots

This directory contains some python scripts to visualize the benchmark results
of the provided scenarios.

## Install

You require `python3` and `poetry` (https://python-poetry.org/docs/) to run
these scripts.

Install poetry if not already present:

``` sh
# Fedora, RHEL, ...
$ sudo dnf install poetry
# Ubuntu
$ sudo apt update
$ sudo apt install python3-poetry
# Alpine
$ apk update
$ apk add poetry
# Or checkout their webpage https://python-poetry.org/docs/#installation
```

If poetry is up and running install the required depedencies:

``` sh
$ poetry install
```

### Animations

When creating animated plots like the object distribution for policies `ffmpeg` is required with the `libx264` codec. You can check your `ffmpeg` codecs with

``` sh
$ ffmpeg -codecs | grep libx264
```

## Usage

``` sh
$ poetry run plots <PATH_TO_YOUR_RESULT>
```
