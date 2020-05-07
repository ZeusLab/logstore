#!/bin/sh
# If the user has supplied only arguments append them to `agent` command
if [ "${1#-}" != "$1" ]; then
    set -- hermes "$@"
fi
exec "$@"