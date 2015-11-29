# mysql-history

Helps creating audit tables and triggers to track data changes in MySQL.

Script connects to a MySQL database and creates:
* audit tables as copies of source tables (1 for every source table) with the following columns added:
    * `hst_id INT PRIMARY KEY`
    * `hst_modified_date DATETIME`
    * `hst_type VARCHAR(2)`

* triggers on INSERT, UPDATE and DELETE on source tables
    * every trigger inserts modified data to audit table specifying `hst_type` as 'I', 'U' or 'D'

When source tables change just re-run the script and it will add new columns to audit tables and modify datatypes of existing ones if possible.

# Requirements

It requires MySQLdb and Python 2.7.x to function properly.
