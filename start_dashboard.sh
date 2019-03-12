#!/bin/bash
service_url=http://17.133.209.41:8080/
docker run -p 80:80 -e SERVICE_URL=$service_url apachepulsar/pulsar-dashboard

