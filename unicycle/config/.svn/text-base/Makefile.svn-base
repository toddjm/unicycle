VALID_EXE := $(UNICYCLE_HOME)/bin/global_valid_ts.py

ASSETS = \
	equities \
	fx \
	futures

ASSETS_UPDATES = \
	equities_update \
	fx_update \
	futures_update

.PHONY: equities fx futures update

all: ts.upd $(ASSETS)

ts.upd: $(VALID_EXE) $(UNICYCLE_HOME)/bin/valid_ts.py equities/ib/equities.cfg fx/ib/fx.cfg futures/ib/futures.cfg
	$(VALID_EXE) equities/ib/equities.cfg fx/ib/fx.cfg futures/ib/futures.cfg
	touch $@

svn_update: 
	svn update

fx:
	cd fx/ib; make

equities:
	cd equities/ib; make

futures:
	cd futures/ib; make

update: svn_update $(ASSETS_UPDATES)

fx_update:
	cd fx/ib; make update

equities_update:
	cd equities/ib; make update

futures_update:
	cd futures/ib; make update
