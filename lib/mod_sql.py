"""sql

"""

import atexit
import MySQLdb
import string
from uuid import uuid1
import warnings

dayofweek_map = dict({'Sunday': 1,
                      'Monday': 2,
                      'Tuesday': 3,
                      'Wednesday': 4,
                      'Thursday': 5,
                      'Friday': 6,
                      'Saturday': 7})

time_zone_map = dict({'CDT': "CST6CDT",
                      'EDT': "EST5EDT",
                      'MDT': "MST7MDT"})


def get_random_name():
    """get random name."""
    return "a" + str(uuid1()).replace('-', '')


def get_dayofweek_index(day):
    """Return ODBC standard index from day"""
    return dayofweek_map[day] if day in dayofweek_map else None


def get_like_pattern(value):
    """get like pattern."""
    return value.replace('_', '\_')


class connection:

    host = None
    user = None
    passwd = None

    alt_dbh = None
    alt_cursor = None
    db_name = None
    dict_cursor = None

    def __init__(self, db="", host="", user="", passwd=""):
        self.db_name = db
        self.host = host
        self.user = user
        self.passwd = passwd

        self.dbh = MySQLdb.connect(host=host, user=user, passwd=passwd,
                                   db=db, local_infile=1)
        self.cursor = self.dbh.cursor()

        atexit.register(self.on_exit)

    def on_exit(self):
        if (self.dbh.open):
            self.cursor.close()
            self.dbh.close()

    def get_db_name(self):
        return self.db_name

    def drop_database(self, db_name):
        warnings.filterwarnings('ignore', '.*drop database*')
        self.cursor.execute("DROP DATABASE IF EXISTS " + db_name)
        warnings.resetwarnings()

    def drop_table(self, tblname):
        warnings.filterwarnings('ignore', '.*Unknown table.*')
        self.cursor.execute("DROP TABLE IF EXISTS " + tblname)
        warnings.resetwarnings()

    def drop_tmp_table(self, tblname):
        warnings.filterwarnings('ignore', '.*Unknown table.*')
        self.cursor.execute("DROP TEMPORARY TABLE IF EXISTS " + tblname)
        warnings.resetwarnings()

#    def get_like_pattern(self, str):
#        return str.replace('_', '\_')

    def db_exists(self, name):
        """Returns True if database name exists."""
        self.cursor.execute("SHOW DATABASES LIKE '%s'" %
#                            self.get_like_pattern(name))
                            get_like_pattern(name))
        return (self.cursor.fetchone() != None)

    def table_exists(self, name, db_name=None):
        """Returns True if table exists assuming a db has been selected."""
        self.cursor.execute("SHOW TABLES FROM %s LIKE '%s'" %
                            (self.db_name if (db_name == None)
#                            else db_name, self.get_like_pattern(name)))
                            else db_name, get_like_pattern(name)))
        return (self.cursor.fetchone() != None)

    def field_exists(self, table, name):
        """Returns True if field exists in table assuming a db has
           been selected."""
        self.cursor.execute("SHOW COLUMNS FROM %s LIKE '%s'" %
#                            (table, self.get_like_pattern(name)))
                            (table, get_like_pattern(name)))
        return (self.cursor.fetchone() != None)

    def select_db(self, db_name):
        """Select database by name."""
        self.db_name = db_name
        self.dbh.select_db(db_name)

    def execute(self, sql):
        """SQL execute query."""
        self.cursor.execute(sql)

    def get_alt_cursor(self):
        if self.alt_cursor == None:
            self.alt_dbh = MySQLdb.connect(host=self.host, user=self.user,
                                           passwd=self.passwd, db=self.db_name,
                                           local_infile=1)
            self.alt_cursor = self.alt_dbh.cursor()
        return self.alt_cursor == None

    def get_status_variable(self, var):
        """Return value of MySQL system variable referenced by name."""
        self.get_alt_cursor().execute("SHOW GLOBAL STATUS LIKE '%s'" %
#                                      self.get_like_pattern(var))
                                      get_like_pattern(var))
        return self.get_alt_cursor().fetchone()[1]

    def get_variable(self, var):
        """Return value of MySQL system variable referenced by name."""
        self.cursor.execute("select @@global." + var)
        return self.cursor.fetchone()[0]

#    def get_dayofweek_index(self, day=None):
#        """Return ODBC standard index from day"""
#        return dayofweek_map[day] if day in dayofweek_map else None

    def set_session_time_zone(self, zone=None):
        """Set time zone for session."""
        self.cursor.execute("set time_zone = '" +
                            self.get_time_zone(zone) + "'")

    def get_temp_table_from_list(self, vals, coldef):
#        tblname = self.get_random_name()
        tblname = get_random_name()
        self.execute("CREATE TEMPORARY TABLE %s "
                     "(`id` %s, PRIMARY KEY (`id`)) "
                     "ENGINE=MyISAM" % (tblname, coldef))
        for val in vals:
            self.execute("INSERT INTO %s VALUES ('%s')" % (tblname, val))
        return tblname

#    def get_random_name(self):
#        """get random name."""
#        # preprend "a" to create valid MySQL names
#        return "a" + str(uuid1()).replace('-', '')

    def get_time_zone(self, zone=None):
        """Return re-mapped MySQL time zone."""
        val = self.get_variable("system_time_zone") if (zone == None) else zone
        return time_zone_map[val] if val in time_zone_map else val

    def get_unix_timestamp(self, time_str=None, date_str=None):
        dstr = "1970-01-01" if (date_str == None) else date_str
        tstr = "00:00:00" if (time_str == None) else time_str
        return self.get_one("SELECT UNIX_TIMESTAMP('%s %s')" % (dstr, tstr))

    def get_time_add_seconds(self, time_str, seconds):
        return self.get_one("SELECT DATE_FORMAT(FROM_UNIXTIME"
                            "(UNIX_TIMESTAMP('1970-01-01 %s') + %d), '%s')" %
                            (time_str, seconds, '%H:%i:%s'))

    def get_converted_time(self, timestr, from_zone, to_zone):
        return self.get_one("SELECT DATE_FORMAT(CONVERT_TZ"
                            "('1970-01-01 %s', '%s', '%s'), '%s')" %
                            (timestr, self.get_time_zone(from_zone),
                             self.get_time_zone(to_zone), '%H:%i:%s'))

    def get_all(self, sql):
        self.execute(sql)
        return self.cursor.fetchall()

    def dump_all(self, sql):
        for rec in self.get_all(sql):
            print rec

    def get_one(self, sql):
        result = self.get_row(sql)
        return result[0] if result != None else None

    def get_dict_cursor(self):
        if self.dict_cursor == None:
            self.dict_cursor = self.dbh.cursor(MySQLdb.cursors.DictCursor)
        return self.dict_cursor

    def get_row_assoc(self, sql):
        self.get_dict_cursor().execute(sql)
        return self.get_dict_cursor().fetchone()

    def get_row(self, sql):
        self.execute(sql)
        return self.cursor.fetchone()

    def get_list(self, sql):
        self.execute(sql)
        vals = list()
        for val in self.cursor.fetchall():
            vals.append(val[0])
        return vals

    def get_dict(self, sql):
        self.execute(sql)
        vals = {}
        for val in self.cursor.fetchall():
            vals[val[0]] = val[1]
        return vals

    def get_dict_array(self, sql):
        self.execute(sql)
        vals = {}
        for val in self.cursor.fetchall():
            vals[val[0]] = val
        return vals

    def is_now_past_time(self, timestr):
        return self.get_one("SELECT NOW() > TIMESTAMP(CONCAT"
                            "(DATE(NOW()),' ','%s'))" % (timestr))

    def get_formatted_date_str(self, datestr):
        return self.get_one("SELECT DATE_FORMAT('" +
                            datestr + "', '%Y-%m-%d')")

    def get_date_str(self):
        return self.get_one("SELECT DATE_FORMAT(NOW(), '%Y%m%d')")

    def get_timestamp_str(self):
        return self.get_one("SELECT DATE_FORMAT(NOW(), '%Y%m%d %h:%i:%s')")

    def get_date_add(self, datestr, interval):
        if datestr is not None:
            term_1 = "'%s'" % datestr
        else:
            term_1 = "NOW()"
#        dateterm = "'%s'" % (datestr) if not datestr == None else "NOW()"
        return self.get_one("SELECT DATE_ADD(%s, INTERVAL %s)" %
                            (term_1, interval))

    def get_next_day(self, datestr):
        return self.get_one("SELECT DATE_ADD('" + datestr +
                            "', INTERVAL 1 DAY)")
