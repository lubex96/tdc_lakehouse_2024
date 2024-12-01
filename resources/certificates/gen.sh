#!/bin/bash

# Create CA
openssl genrsa 2048 > ca.key
openssl req -new -x509 -nodes -days 36500 -key ca.key -config ca-req.conf -extensions v3_ca > ca.crt

for i in client*.conf; do
	[ -f "$i" ] || break

	# Remove suffix -req.conf
	i=${i%"-req.conf"}

	openssl req -newkey rsa:2048 -nodes -days 36500 -keyout ${i}.key -config ${i}-req.conf -extensions v3_req > ${i}-req.pem
	openssl x509 -req -in ${i}-req.pem -days 36500 -CA ca.crt -CAkey ca.key -set_serial 01 -extfile ${i}-req.conf -extensions v3_req > ${i}.crt
done
