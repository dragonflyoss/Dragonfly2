#!/bin/bash

LINT_DIR=/data
MARKDOWNLINT_IMAGE=avtodev/markdown-lint:v1

docker run --rm \
    -v "$(PWD):${LINT_DIR}:ro" \
    ${MARKDOWNLINT_IMAGE} \
    -c "${LINT_DIR}/.markdownlint.yml" /data/**/*.md \
    -i "${LINT_DIR}/CHANGELOG.md" \
    -i "${LINT_DIR}/docs/en/api-reference/api-reference.md" \
    -i "${LINT_DIR}/docs/en/api-reference" \
    -i "${LINT_DIR}/deploy/helm-charts" \
    -i "${LINT_DIR}/docs/zh-CN/api-reference" \
    -i "${LINT_DIR}/manager/console" \
    -i "${LINT_DIR}/docs/en/cli-reference/dfget.1.md"
