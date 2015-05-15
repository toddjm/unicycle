VALID_EXE := $(UNICYCLE_HOME)/bin/valid_ts.py
CONFIG_TS := equities.cfg

all: ts.upd

update: ts.upd

ts.upd: $(VALID_EXE) $(CONFIG_TS)
	cd ../..; make ts.upd
	$(VALID_EXE) $(CONFIG_TS)
	touch $@
