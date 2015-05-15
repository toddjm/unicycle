VALID_EXE := $(UNICYCLE_HOME)/bin/valid_ts.py
CONFIG_TS := futures.cfg

all: ts.upd date_codes.upd roll_dates.upd futures.upd

update: ts.upd date_codes.upd roll_dates.upd futures.upd

ts.upd: $(VALID_EXE) $(CONFIG_TS)
	cd ../..; make ts.upd
	$(VALID_EXE) $(CONFIG_TS)
	touch $@

date_codes.upd: $(UNICYCLE_HOME)/bin/update_futures_date_codes.py futures_date_codes.txt
	$(UNICYCLE_HOME)/bin/update_futures_date_codes.py futures_date_codes futures_date_codes.txt
	touch $@

roll_dates.upd: $(UNICYCLE_HOME)/bin/update_futures_roll_dates.py roll_dates.txt
	$(UNICYCLE_HOME)/bin/update_futures_roll_dates.py unicycle.cfg futures_roll_dates roll_dates.txt
	touch $@

futures.upd: $(UNICYCLE_HOME)/bin/update_futures.py futures.txt exchanges.txt
	$(UNICYCLE_HOME)/bin/update_futures.py unicycle.cfg futures futures.txt exchanges.txt
	touch $@
