#!/bin/bash

OUTPUT="$(awk '{print $1}' "$1"| uniq -c | grep read | wc -l)"
echo "Blocks read   :  ${OUTPUT}"

OUTPUT="$(awk '{print $1}' "$1"| uniq -c | grep write | wc -l)"
echo "Blocks written:  ${OUTPUT}"

OUTPUT="$(grep read "$1" | awk '{sum = sum+$3}END{print sum}')"
echo "Read ops      :  ${OUTPUT}"

OUTPUT1="$(grep write "$1" | awk '{sum = sum+$3}END{print sum}')"
echo "Write ops     :  ${OUTPUT1}"
echo "---------------"
echo "Total ops     :  $((OUTPUT + OUTPUT1))"

