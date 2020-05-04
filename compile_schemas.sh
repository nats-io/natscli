#!/bin/bash

# uses https://www.npmjs.com/package/json-dereference-cli to dereference the source JSON schemas
# resolving all definitions and producing flat json schema files that's easy to load remotely and
# validate as they are standalone single files.
#
# required json-dereference and jq in your path

set -e

assert_directory_must_exist() {
    if [ ! -d $1 ]
    then
      echo "ERROR: ${1} does not exist, use fully qualified paths"
      exit 1
    fi
}

dereference_directory () {
  local pwd=$(pwd)
  local source_dir="${pwd}/$1"
  local target_dir="${pwd}/$2"

  assert_directory_must_exist "${source_dir}"
  cd "${source_dir}"

  assert_directory_must_exist "${target_dir}"

  local source_list=$(find .)

  for file in $source_list;do
    if [ -d $file ];then
      mkdir -p "${target_dir}/${file}"
    fi

    if [ $(basename "${file}") == "definitions.json" ];then
      continue
    fi

    if [[ "${file: -5}" == ".json" ]];then
      json-dereference -s "${file}" -o "${target_dir}/${file}.temp.json"
      jq < "${target_dir}/${file}.temp.json" > "${target_dir}/${file}"
      rm -f "${target_dir}/${file}.temp.json"
    fi
  done
}

dereference_directory "schema_source" "schemas"

