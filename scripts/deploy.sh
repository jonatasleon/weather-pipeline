#!/bin/bash

set -euo pipefail


# Ensure that git is clean
if ! git diff-index --quiet HEAD --; then
    echo "Git is not clean. Please commit or stash your changes."
    exit 1
fi

uvx prefect-cloud deploy flows/weather_flow.py:main \
    --from jonatasleon/weather-pipeline \
    --with-requirements requirements.txt \
    --name weather-flow \
    --cron "0 */3 * * *"
