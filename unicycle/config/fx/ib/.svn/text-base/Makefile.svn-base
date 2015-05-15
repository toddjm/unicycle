VALID_EXE := $(UNICYCLE_HOME)/bin/valid_ts.py
CONFIG_TS := fx.cfg

all: ts.upd fx.upd

update: ts.upd fx.upd

ts.upd: $(VALID_EXE) $(CONFIG_TS)
	cd ../..; make ts.upd
	$(VALID_EXE) $(CONFIG_TS)
	touch $@

fx.upd: $(UNICYCLE_HOME)/bin/update_fx.py fx.txt exchanges.txt
	$(UNICYCLE_HOME)/bin/update_fx.py unicycle.cfg fx fx.txt exchanges.txt
	touch $@
