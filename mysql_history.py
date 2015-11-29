# import mysql.connector
import MySQLdb
import uuid
from collections import namedtuple
import sys
import re
import json

db_config = {
    'user': 'root',
    'passwd': 'GWr5nEbM',
    'host': '172.17.0.2',
    'db': 'sugarcrm_prod75'
}

h_table_prefix = 'h_'

def table_names(config):
    config.cursor.execute('''SELECT table_name FROM information_schema.tables
                             WHERE table_schema=%s
                             AND table_name not like %s''',
                          (config.database, '%s%%' % escape_underscore(config.h_prefix)))
    names = [i['table_name'] for i in cursor]
    includes = config.includes
    if includes:
        names = [name for name in names if any([re.match(incl, name) for incl in includes])]
    excludes = config.excludes
    if excludes:
        names = [name for name in names if not any([re.match(excl, name) for excl in excludes])]
    return names


def table_exists(config, table_name):
    return config.cursor.execute('''
        SELECT 1 FROM information_schema.tables
        WHERE table_name = %s
        AND table_schema = %s''', (table_name, config.database))


def columns(config, table_name):
    columns_query = '''SELECT column_name, column_type, character_set_name, collation_name
                        FROM information_schema.columns
                        WHERE table_schema=%s
                        AND table_name=%s'''
    config.cursor.execute(columns_query, (config.database, table_name))
    return config.cursor.fetchall()


def copy_table(config, table_from, table_to):
    print "CREATE TABLE", table_to
    cols = columns(config, table_from)
    columns_sql = [u"`%(name)s` %(type)s %(charset)s %(collation)s DEFAULT NULL" % {
        'name': col['column_name'],
        'type': col['column_type'],
        'charset': ('CHARACTER SET ' + col['character_set_name']) if col['character_set_name'] else '',
        'collation':('COLLATE ' + col['collation_name']) if col['collation_name'] else '',
    } for col in cols]

    create_sql = u'''CREATE TABLE %(schema)s.%(table_to)s (
    hst_id varchar(36) PRIMARY KEY,
    hst_modified_date datetime,
    hst_type varchar(2),
    %(columns)s
    )
    ''' % {
        'schema': config.database,
        'table_to': table_to,
        'columns': ",\n ".join(columns_sql)
    }

    config.cursor.execute(create_sql)


def update_table(config, table_from, table_to):
    cols_from = columns(config, table_from)
    cols_to = columns(config, table_to)
    from_dict = {col['column_name']: col for col in cols_from}
    to_dict = {col['column_name']: col for col in cols_to}
    new_column_names = set(from_dict.keys()).difference(set(to_dict.keys()))
    new_columns = {name: from_dict[name] for name in new_column_names}
    changed_type = []
    for name, col in from_dict.iteritems():
        if name not in to_dict:
            continue
        if col['column_type'] != to_dict[name]['column_type']:
            changed_type.append(col)

    print "UPDATING COLUMNS IN TABLE", table_to, len(new_columns), "new", len(changed_type), "changed"

    new_columns_sql = [u"ADD COLUMN `%(name)s` %(type)s %(charset)s %(collation)s DEFAULT NULL" % {
        'name': col['column_name'],
        'type': col['column_type'],
        'charset': ('CHARACTER SET ' + col['character_set_name']) if col['character_set_name'] else '',
        'collation':('COLLATE ' + col['collation_name']) if col['collation_name'] else ''
        } for name, col in new_columns.iteritems()]

    changed_columns_sql = [u"MODIFY COLUMN `%(name)s` %(type)s %(charset)s %(collation)s DEFAULT NULL" % {
        'name': col['column_name'],
        'type': col['column_type'],
        'charset': ('CHARACTER SET ' + col['character_set_name']) if col['character_set_name'] else '',
        'collation':('COLLATE ' + col['collation_name']) if col['collation_name'] else ''
        } for col in changed_type]

    for sql in new_columns_sql + changed_columns_sql:
        config.cursor.execute('''ALTER TABLE %(schema)s.%(table)s %(sql)s''' % {
            'schema': config.database,
            'table': table_to,
            'sql': sql
        })



def create_or_update_h_table(config, table_name):
    name = (config.h_prefix + table_name)[:64]
    exists = table_exists(config, name)
    if not exists:
        copy_table(config, table_name, name)
    else:
        update_table(config, table_name, name)
    return name


def drop_triggers(config, table_name):
    config.cursor.execute('''SELECT trigger_name
                             FROM information_schema.triggers
                             WHERE event_object_table=%s
                             AND trigger_schema = %s
                             AND trigger_name like %s''',
                          (table_name, config.database, 'HST\_%%'))
    names = [i['trigger_name'] for i in config.cursor]
    for name in names:
        print "Dropping trigger", name
        cursor.execute('DROP TRIGGER %s.%s' % (config.database, name))


def create_triggers(config, table_name, h_table):
    print "CREATING TRIGGERS FOR", h_table, "ON", table_name
    id = uuid.uuid4().hex
    cols = columns(config, table_name)
    col_names = [col['column_name'] for col in cols]
    values = ",".join(['NEW.' + col for col in col_names])

    #INSERT
    ins_trigger = '''
    CREATE TRIGGER HST_%(id)s
    AFTER INSERT ON %(schema)s.%(table)s FOR EACH ROW
    BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    END;
    INSERT INTO %(schema)s.%(h_table)s (hst_id, hst_modified_date, hst_type, %(columns)s)
    VALUES (UUID(), SYSDATE(), 'I', %(values)s);
    END''' % {'schema': config.database,
               'table': table_name,
               'h_table': h_table,
               'columns': ",".join(col_names),
               'values': values,
               'id': id}


    config.cursor.execute(ins_trigger)

    #UPDATE
    id = uuid.uuid4().hex
    up_trigger = '''
    CREATE TRIGGER HST_%(id)s
    AFTER UPDATE ON %(schema)s.%(table)s FOR EACH ROW
    BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    END;
    INSERT INTO %(schema)s.%(h_table)s (hst_id, hst_modified_date, hst_type, %(columns)s)
    VALUES (UUID(), SYSDATE(), 'U', %(values)s);
    END''' % {'schema': config.database,
               'table': table_name,
               'h_table': h_table,
               'columns': ",".join(col_names),
               'values': values,
               'id': id}

    config.cursor.execute(up_trigger)


    #DELETE
    id = uuid.uuid4().hex
    del_values = ",".join(['OLD.' + col for col in col_names])
    del_trigger = '''
    CREATE TRIGGER HST_%(id)s
    AFTER DELETE ON %(schema)s.%(table)s FOR EACH ROW
    BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    END;
    INSERT INTO %(schema)s.%(h_table)s (hst_id, hst_modified_date, hst_type, %(columns)s)
    VALUES (UUID(), SYSDATE(), 'D', %(values)s);
    END''' % {'schema': config.database,
               'table': table_name,
               'h_table': h_table,
               'columns': ",".join(col_names),
               'values': del_values,
               'id': id}

    config.cursor.execute(del_trigger)


def escape_underscore(text):
    return text.replace('_', '\_')


def drop_history_tables(config):
    cursor = config.cursor

    cursor.execute('''SELECT table_name
                      FROM information_schema.tables
                      WHERE table_name like %s
                      AND table_schema = %s
                   ''', ('%s%%' % escape_underscore(config.h_prefix), config.database))
    h_names = [i['table_name'] for i in cursor]
    for name in h_names:
        print "dropping ", name
        base_table = name[len(config.h_prefix):]
        drop_triggers(config, base_table)
        cursor.execute('DROP TABLE %s' % name)


Config = namedtuple('Config', ['cursor', 'database', 'h_prefix', 'includes', 'excludes'])

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print u"USAGE: %s <config.ini> [DROP]" % sys.argv[0]
        sys.exit(1)

    config = json.load(open(sys.argv[1], 'r'))

    cnx = MySQLdb.connect(host=config['host'],
                          user=config['user'],
                          passwd=config['password'],
                          db=config['database'])
    cursor = cnx.cursor(MySQLdb.cursors.DictCursor)
    app_config = Config(cursor=cursor,
                        database=config['database'],
                        h_prefix=config['history_table_prefix'],
                        includes=config.get('includes', ['.*']),
                        excludes=config.get('excludes', []))

    if len(sys.argv) > 2:
        if sys.argv[2].lower() == 'drop':
            drop_history_tables(app_config)
            sys.exit(0)

    tables = table_names(app_config)
    for table in tables:
        h_name = create_or_update_h_table(app_config, table)
        drop_triggers(app_config, table)
        create_triggers(app_config, table, h_name)

    cnx.commit()
    cursor.close()
