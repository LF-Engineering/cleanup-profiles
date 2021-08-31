#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: you need to specify env: test|prod"
  exit 1
fi
endpoint="`cat ./secrets/DB_ENDPOINT.${1}.secret`"
DB_ENDPOINT="${endpoint}" ./cleanup
