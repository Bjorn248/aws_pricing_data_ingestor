#!/bin/bash

set -ex

pipenv requirements > requirements.txt

pipenv run pip install -r requirements.txt -t build

cp ./lambda_import.py ./build
cp ./pricing_import.py ./build

pushd build

zip -r ../lambda_package.zip ./

popd

rm ./requirements.txt

rm -rf build
