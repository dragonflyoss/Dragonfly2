#!/bin/bash

LINT_DIR=/data
MARKDOWNLINT_IMAGE=avtodev/markdown-lint:v1

docker run --rm \
    -v "$(pwd):${LINT_DIR}:ro" \
    ${MARKDOWNLINT_IMAGE} \
    -c "${LINT_DIR}/.markdownlint.yml" /data/**/*.md \
    -i "${LINT_DIR}/CHANGELOG.md" \
    -i "${LINT_DIR}/build" \
    -i "${LINT_DIR}/deploy/helm-charts" \
    -i "${LINT_DIR}/manager/console" \
