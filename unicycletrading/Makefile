#comment
.PHONY: config java

all: java config

config:
	cd config; make

fx:
	cd config/fx/ib; make

equities:
	cd config/equities/ib; make

futures:
	cd config/futures/ib; make

update: svn_update
	make

svn_update:
	svn update

java:
	cd IBJts/java; make
