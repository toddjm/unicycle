VALID_EXE := $(UNICYCLE_HOME)/bin/global_valid_ts.py

ASSETS = \
	equities \
	fx \
	futures \
        indices

ASSETS_UPDATES = \
	equities_update \
	fx_update \
	futures_update \
        indices_update

.PHONY: equities fx futures indices update

all: ts.upd $(ASSETS)

ts.upd: $(VALID_EXE) $(UNICYCLE_HOME)/bin/valid_ts.py equities/ib/equities.cfg fx/ib/fx.cfg futures/ib/futures.cfg indices/ib/indices.cfg
	$(VALID_EXE) equities/ib/equities.cfg fx/ib/fx.cfg futures/ib/futures.cfg indices/ib/indices.cfg
	touch $@

svn_update: 
	svn update

fx:
	cd fx/ib; make

equities:
	cd equities/ib; make

futures:
	cd futures/ib; make

indices:
	cd indices/ib; make

update: svn_update $(ASSETS_UPDATES)

fx_update:
	cd fx/ib; make update

equities_update:
	cd equities/ib; make update

futures_update:
	cd futures/ib; make update

indices_update:
	cd indices/ib; make update
