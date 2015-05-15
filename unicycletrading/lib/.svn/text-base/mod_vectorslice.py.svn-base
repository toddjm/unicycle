"""vectorslice

"""


class new():

    dbh = None
    left_table = None
    table = None
    where = None
    verbose = None

    tblname = None
    total_cnt = None
    valid_cnt = None

    def __init__(self,
                 dbh=None,
                 left_table=None,
                 table=None,
                 where="1=1",
                 symbol=None,
                 verbose=False):
        self.dbh = dbh
        self.left_table = left_table
        self.table = table
        self.where = where
        self.verbose = verbose
        self.symbol = symbol

        if (left_table != None):
            self.build_table()

    def get_left_table(self):
        return self.left_table

    def get_create_table_sql(self):
        return """
        CREATE TEMPORARY TABLE """ + self.get_tblname() + """ (
        `ts` TIMESTAMP NOT NULL DEFAULT 0,`valid` TINYINT(1) DEFAULT 0,
        PRIMARY KEY (`ts`), INDEX `valid_idx` (`valid`)) ENGINE=MyISAM
        """

    def get_sql_list(self):
        sql = list()
        if not self.has_right_table():
            sql.append(self.get_create_table_sql() +
              "SELECT ts, 1 AS valid FROM " +
              self.get_left_table() + " WHERE " + self.where)
        else:
            terms = list()
            if self.dbh.field_exists(self.get_left_table(), "valid"):
                terms.append("l.valid")
            terms.append("(r.ts IS NOT NULL)")
            terms.append(
              "(" +
              self.where +
              ")")
            valid_term = " AND ".join(terms)

            if (not self.symbol == None and self.symbol.is_rolled_futures()):
                roll_start_index = self.symbol.get_rolled_futures().get_ticker_tables().index(self.table)
                dt_initial = self.symbol.get_rolled_futures().get_relative_roll_dates()[0]
                tbl = self.table
                for roll_index in range(1,
                  len(self.symbol.get_rolled_futures().get_relative_roll_dates())):
                    dt_final = self.symbol.get_rolled_futures().get_relative_roll_dates()[roll_index]
                    if (roll_index == 1):
                        sql.append(
                          self.get_create_table_sql() +
                          "SELECT l.ts, (" +
                          valid_term +
                          ") AS valid FROM " +
                          self.get_left_table() +
                          " l LEFT JOIN " +
                          tbl +
                          " r USING (ts) where r.ts > DATE('" +
                          dt_initial +
                          "') AND r.ts <= DATE('" +
                          dt_final +
                          "')")
                    else:
                        sql.append(
                          "INSERT INTO " +
                          self.get_tblname() +
                          " SELECT l.ts, (" +
                          valid_term +
                          ") AS valid FROM " +
                          self.get_left_table() +
                          " l LEFT JOIN " +
                          tbl +
                          " r USING (ts) where r.ts > DATE('" +
                          dt_initial +
                          "') AND r.ts <= DATE('" +
                          dt_final +
                          "')")
                    dt_initial = dt_final
                    if (roll_start_index + roll_index) < len(self.symbol.get_rolled_futures().get_ticker_tables()):
                        tbl = self.symbol.get_rolled_futures().get_ticker_tables()[roll_start_index + roll_index]

            else:
                sql.append(
                  self.get_create_table_sql() +
                  "SELECT l.ts, (" +
                  valid_term +
                  ") AS valid FROM " +
                  self.get_left_table() +
                  " l LEFT JOIN " +
                  self.table +
                  " r USING (ts)")

        return sql

    def has_right_table(self):
        return (self.table != None)

    def build_table(self):
        for sql in self.get_sql_list():
            if (self.verbose):
                print sql
            self.dbh.execute(sql)
        if (self.verbose):
            self.results()

    def results(self):
        print "total_samples:\t", self.get_total_cnt()
        print "valid_sample_count:\t", self.get_valid_cnt()

    def print_table(self,
                    count=None,
                    offset=0):
        limit_str = "" if count == None else "LIMIT " + str(count) + " OFFSET " + str(offset)
        self.dbh.execute(
          "select * from " +
           self.get_tblname() +
           " " +
           limit_str)

        index = offset
        for rec in self.dbh.cursor.fetchall():
            print "[%05d] %s\t%d" % (
              index,
              rec[0],
              rec[1])
            index += 1

    def set_tblname(self,
                    tblname):
        self.tblname = tblname

    def get_tblname(self):
        if (self.tblname == None):
            self.tblname = "%s.%s" % (
              self.dbh.get_db_name(),
              self.dbh.get_random_name())
        return self.tblname

    def get_valid_cnt(self):
        if (self.valid_cnt == None):
            self.valid_cnt = self.dbh.get_one(
                               "select count(*) from " +
                               self.get_tblname() +
                               " where valid=1")
        return self.valid_cnt

    def get_total_cnt(self):
        if (self.total_cnt == None):
            self.total_cnt = self.dbh.get_one(
                               "select count(*) from " +
                               self.get_tblname())
        return self.total_cnt
