# mysql-history

Automates creating audit tables and triggers to track changes in MySQL tables.

Script connects to MySQL database and creates:
* audit tables as copies of source tables (1 for every source table) with the following columns added:
    * `hst_id INT PRIMARY KEY`
    * `hst_modified_date DATETIME`
    * `hst_type VARCHAR(2)`

* triggers on INSERT, UPDATE and DELETE
    * every trigger inserts modified data to audit table specifying `hst_type` as 'I', 'U' or 'D'

It requires MySQLdb and Python 2.7.x to function properly.
