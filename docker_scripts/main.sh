#!/bin/sh
set -o errexit
set -o nounset

echo "Starting main.sh"
echo "Creating virtual environment"
python3 -m venv --system-site-packages --symlinks --upgrade ${OSPARC_VENV_DIR}
${OSPARC_VENV_DIR}/bin/pip install -qU pip wheel setuptools


[ ! -z "${OSPARC_REQUIREMENTS_TXT}" ] && ${OSPARC_VENV_DIR}/bin/pip install -qr ${OSPARC_REQUIREMENTS_TXT}

echo "Executing code ${OSPARC_USER_ENTRYPOINT_PATH}"
cd ${OSPARC_USER_ENTRYPOINT_DIR}
${OSPARC_VENV_DIR}/bin/python3 ${OSPARC_USER_ENTRYPOINT_PATH}
echo "Stopping main.sh"
