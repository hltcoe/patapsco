#!/bin/bash

if [[ $1 != "--embedding" ]] ; then
    exit 1
fi
if [[ $2 != "mbert" ]] ; then
    exit 1
fi

echo "1 Q0 bbb 1 0.8 ranker" > $7
echo "1 Q0 aaa 2 0.3 ranker" >> $7

echo "Success"
