#!/bin/bash

if [ "$1" = "server" ] || [ "$JSM_MODE" = "server" ];
then
  shift

  if [ -f /leafnode.creds ];
  then
    exec /nats-server -js -c /ngs-server.conf $*
  else
    exec /nats-server -js $*
  fi
else
  if [ -f /user.creds ];
  then
    export NATS_CREDS=/user.creds
  fi

  exec sh -l
fi
