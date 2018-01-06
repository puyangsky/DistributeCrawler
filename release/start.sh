#!/bin/bash
set -e

base_dir=$(cd `dirname $0`; cd ..; pwd)
docker_id="$1"
data_dir=${base_dir}/data/data-${docker_id}
mkdir -p ${data_dir}
docker run -it -d --name crawler-${docker_id} -v ${data_dir}:/usr/src/app/data/ --restart always crawler python ./crawler.py
echo "start "crawler-${docker_id}
