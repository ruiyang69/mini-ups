import sqlite3
from utility import *

def connect_to_db():
    C = sqlite3.connect(database="../db.sqlite3")
    return C


def drop_if_exists(C):
     with db_lock:
          cur = C.cursor()
          sql = '''
               DELETE FROM package_package;
               DROP TABLE IF EXISTS trucks; 
          '''
          cur.executescript(sql)
          C.commit()


def create_table(C):
     with db_lock:
          cur = C.cursor()
          sql = '''
               CREATE TABLE trucks(
                    truck_id INT   PRIMARY KEY  NOT NULL,
                    status  TEXT                NOT NULL,
                    amount  INT                 NOT NULL DEFAULT 0);
          '''
          cur.execute(sql)
          C.commit()


def insert_package(C, package_id, dst_x, dst_y, status, truck_id, item, owner):
     with db_lock:
          cur = C.cursor()
          sql = '''
               INSERT INTO package_package (
                    package_id, dst_x, dst_y, status, truck_id, item, owner
               ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s);
          '''
          cur.execute(sql, (package_id, dst_x, dst_y, status, item, owner))
          C.commit()


def get_package_status(C, package_id):
     with db_lock:
          cur = C.cursor()
          sql = '''
               SELECT status FROM package_package WHERE package_id = %s;
          '''
          cur.execute(sql, (package_id))
          return cur.fetchone()


def update_package_status(C, package_id, status):
     with db_lock:
          cur = C.cursor()
          sql = '''
               UPDATE package_package SET status = %s WHERE package_id = %s;
          '''
          cur.execute(sql, (status, package_id))
          C.commit()

def update_package_pos(conn, truckid, x, y):
     with db_lock:
          cur = conn.cursor()
          sql = '''UPDATE package_package SET curr_x = %s, curr_y = %s WHERE truck_id = %s AND status = 'delivering';'''
          cur.execute(sql, (x, y, truckid,))
          conn.commit()
    return


def get_package_dst(C, package_id):
     with db_lock:
          cur = C.cursor()
          sql = '''
               SELECT dst_x, dst_y FROM package_package WHERE package_id = %s;
          '''
          cur.execute(sql, (package_id))
          return cur.fetchone()


def update_package_dst(C, package_id, dst_x, dst_y):
     with db_lock:
          cur = C.cursor()
          sql = '''
               UPDATE package_package SET dst_x = %s, dst_y = %s WHERE package_id = %s;
          '''
          cur.execute(sql, (dst_x, dst_y, package_id))
          C.commit()


def insert_truck(C, truck_id, status):
     with db_lock:
          cur = C.cursor()
          sql = '''
               INSERT INTO trucks (truck_id, status) VALUES (?,?);
          '''
          cur.execute(sql, (truck_id, status))
          C.commit()


def get_truck_status(C, truck_id):
     with db_lock:
          cur = C.cursor()
          sql = '''
               SELECT status FROM trucks WHERE truck_id = %s;
          '''
          cur.execute(sql, (truck_id))
          return cur.fetchone()


def update_truck_status(C, truck_id, status):
     with db_lock:
          cur = C.cursor()
          sql = '''
               UPDATE trucks SET status = %s WHERE truck_id = %s;
          '''
          cur.execute(sql, (status, truck_id))
          C.commit()


