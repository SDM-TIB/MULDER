#!/usr/bin/env bash

python3.5 /MULDER/EndpointService.py -c /data/config.json &
python3.5 /MULDER/rabbitmq_receiver.py
