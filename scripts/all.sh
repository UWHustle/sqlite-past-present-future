#!/bin/bash

(
  cd ssb || exit
  ./ssb.sh
)

(
  cd tatp || exit
  ./tatp.sh
)

(
  cd blob || exit
  ./blob.sh
)
