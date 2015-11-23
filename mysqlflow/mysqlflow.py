#!/usr/bin/python

###############################################################################
### imports
###############################################################################
import os
import sys
import subprocess
import util

###############################################################################
### classes
###############################################################################
class MySQLFlow:

    def __init__(self):
        self._databases = {}
        self._command = None
        self._sql = ''
        self._output = None

    def _html_escape(self, text):
        html_escape_table = [
            ("&amp;","&"),
            ('\\\\"',"&quot;"),
            ("\\\\'","&apos;"),
            ("&gt;",">"),
            ("&lt;","<"),
        ]
        for (k,v) in html_escape_table:
            text=text.replace(k,v)
        return text
        
    def _char_escape(self, text):
        char_escape_table = [
            ('(','\('),
            (')','\)'),
            ('"','\\"'),
            ("'","\\'"),
            ('\\\\_','_'),
            ('\\\\%','%'),
            ('{','\{'),
            ('}','\}'),
        ]
        text=self._html_escape(text)
        for k,v in char_escape_table:
            text=text.replace(k,v)
        return text

    def _command_line(self):
        return self._command + ' -e "{sql};"'.format(sql=self._sql)

    def _reset_sql(self):
        self._sql = ''
        
    def reset(self):
        self._reset_sql()
        return self 

    def show(self):
        # TODO: implement 'pretty print' option
        print self._command_line()
        return self

    def history(self):
        # TODO: store a printable history of commands
        pass

    def setup(self, id=0, host=None, user='root', password=None):
        self._databases[id] = {'host': host, 'user': user, 'pass': password}
        return self

    def connect(self, id=0):
        self._command = 'mysql --host {host} --user {user} --password={pass}'
        self._command = self._command.format(**self._databases[id])
        return self

    def execute(self):        
        try:
            self._output = util.check_output(self._command_line(), shell=True)
            self.reset()
        except subprocess.CalledProcessError as e:
            print e
            return False
        return True

    def results(self):
        if self._output is None or not len(self._output):
            return None
        results = [l.split('\t') for l in self._output.split('\n')]
        ordered_results = [dict(zip(results[0], r)) for r in results[1:]]
        return ordered_results    
        
    def USE(self, database):
        self._sql += "USE {db}; ".format(db=database)
        return self
    
    def SELECT(self, selection):
        if isinstance(selection, list):
            selection = ','.join(selection)
        self._sql += "SELECT {sel} ".format(sel=selection)
        return self

    def FROM(self, table):
        self._sql += "FROM {tbl} ".format(tbl=table)
        return self

    def UPDATE(self, table):
        self._sql += "UPDATE {tbl} ".format(tbl=table)
        return self

    def SET(self, setting, value = None):
        if value is not None and '=' not in setting:
            setting += ' = ' + str(value)
        spl = [s for s in setting.split('=')]
        spl = [s.strip() for s in [spl[0], '='.join(spl[1:])]]
        while spl[-1].startswith("'"):
            spl[-1] = spl[-1][1:]
        while spl[-1].endswith("'"):
            spl[-1] = spl[-1][:-1]
        if not spl[-1].isdigit():
            spl[-1] = self._char_escape(spl[-1]).join(["'","'"])
        setting = ' = '.join(spl)
        self._sql += "SET {setting} ".format(setting=setting)
        return self

    def WHERE(self, condition, value = None):
        if value and '=' not in condition:
            condition += ' = ' + value
        self._sql += "WHERE {condition} ".format(condition=condition)
        return self

    def INSERT(self):
        self._sql += "INSERT "
        return self

    def INTO(self, table, fields=None, values=None):
        fields = ', '.join(fields)
        values = ', '.join([v if v.isdigit() or v == "NULL" 
                            else v.replace("'","\\'").join(["'","'"]) 
                            for v in values]
        )
        self._sql += "INTO {t} ({f}) VALUES ({v}) ".format(
            t=table, f=fields, v=values
        )
        return self
    
