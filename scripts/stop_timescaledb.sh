#!/bin/bash

PG_VER=${PG_VER:-10}
sudo -u postgres /usr/lib/postgresql/${PG_VER}/bin/pg_ctl -U postgres -D /etc/postgresql/${PG_VER}/main stop
