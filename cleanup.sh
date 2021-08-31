#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: you need to specify env: test|prod"
  exit 1
fi
endpoint="`cat ./secrets/DB_ENDPOINT.${1}.secret`"
if [ -z "${endpoint}" ]
then
  echo "$0: cannot get DB endpoint for ${1}"
  exit 2
fi
url="`cat ./secrets/AFFILIATION_API_URL.${1}.secret`"
if [ -z "${url}" ]
then
  echo "$0: cannot get API url for ${1}"
  exit 3
fi
auth="`cat ./secrets/AUTH0_DATA.${1}.secret`"
if [ -z "${auth}" ]
then
  echo "$0: cannot get auth0 data for ${1}"
  exit 4
fi
DB_ENDPOINT="${endpoint}" API_URL="${url}" AUTH0_DATA="${auth}" ./cleanup
