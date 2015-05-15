VALID_EXE := $(UNICYCLE_HOME)/bin/valid_ts.py
CONFIG_TS := indices.cfg

all: ts.upd indices.upd

update: ts.upd indices.upd

ts.upd: $(VALID_EXE) $(CONFIG_TS)
	cd ../..; make ts.upd
	$(VALID_EXE) $(CONFIG_TS)
	touch $@

indices.upd: $(UNICYCLE_HOME)/bin/update_indices.py indices.txt exchanges.txt
	$(UNICYCLE_HOME)/bin/update_indices.py unicycle.cfg indices indices.txt exchanges.txt
	touch $@
