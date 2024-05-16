#!/bin/bash

set -euo pipefail
IFS=$'\n\t'
INFO="INFO: [$(basename "$0")] "

echo "$INFO" "Starting container for map ..."

HOST_USERID=$(stat -c %u "${INPUT_FOLDER}")
HOST_GROUPID=$(stat -c %g "${INPUT_FOLDER}")
CONTAINER_GROUPNAME=$(getent group | grep "${HOST_GROUPID}" | cut --delimiter=: --fields=1 || echo "")

OSPARC_USER='osparcuser'

if [ "$HOST_USERID" -eq 0 ]; then
	echo "Warning: Folder mounted owned by root user... adding $OSPARC_USER to root..."
	addgroup "$OSPARC_USER" root
else
	echo "Folder mounted owned by user $HOST_USERID:$HOST_GROUPID-'$CONTAINER_GROUPNAME'..."
	# take host's credentials in $OSPARC_USER
	if [ -z "$CONTAINER_GROUPNAME" ]; then
		echo "Creating new group my$OSPARC_USER"
		CONTAINER_GROUPNAME=my$OSPARC_USER
		addgroup --gid "$HOST_GROUPID" "$CONTAINER_GROUPNAME"
	else
		echo "group already exists"
	fi

	echo "adding $OSPARC_USER to group $CONTAINER_GROUPNAME..."
	usermod --append --groups "$CONTAINER_GROUPNAME" "$OSPARC_USER"

	echo "changing owner ship of state directory /home/${OSPARC_USER}/work/workspace"
	chown --recursive "$OSPARC_USER" "/home/${OSPARC_USER}/work/workspace"
	echo "changing owner ship of state directory ${INPUT_FOLDER}"
	chown --recursive "$OSPARC_USER" "${INPUT_FOLDER}"
	echo "changing owner ship of state directory ${OUTPUT_FOLDER}"
	chown --recursive "$OSPARC_USER" "${OUTPUT_FOLDER}"
fi

exec gosu "$OSPARC_USER" /docker/dakota.bash
