#!/bin/bash

OUTPUT="$(grep read "$1" | awk '{sum = sum+$3}END{print sum}')"
echo "Blocks read   :  ${OUTPUT}"

OUTPUT="$(grep write "$1" | awk '{sum = sum+$3}END{print sum}')"
echo "Blocks written:  ${OUTPUT}"

OUTPUT="$(awk '{print $1}' "$1"| uniq -c | grep read | wc -l)"
echo "Read ops      :  ${OUTPUT}"

OUTPUT1="$(awk '{print $1}' "$1"| uniq -c | grep write | wc -l)"
echo "Write ops     :  ${OUTPUT1}"
echo "---------------"
echo "Total ops     :  $((OUTPUT + OUTPUT1))"

