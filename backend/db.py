import sqlite3
import threading
db_lock = threading.Lock()

def connect_to_db():
    C = sqlite3.connect(database="../db.sqlite3", check_same_thread=False)
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
                    amount  INT                 NOT NULL DEFAULT 0,
                    whnum   INT                 NOT NULL DEFAULT 0);
          '''
          cur.execute(sql)
          C.commit()


def insert_package(C, package_id, dst_x, dst_y, status, truck_id, item, owner):
     with db_lock:
          cur = C.cursor()
          sql = '''
               INSERT INTO package_package (
                    package_id, dst_x, dst_y, status, truck_id, item, owner_id
               ) VALUES(?, ?, ?, ?, ?, ?, ?);
          '''
          cur.execute(sql, (package_id, dst_x, dst_y, status, truck_id, item, owner))
          C.commit()


def get_package_status(C, package_id):
     with db_lock:
          cur = C.cursor()
          sql = '''
               SELECT status FROM package_package WHERE package_id = ?;
          '''
          cur.execute(sql, (package_id))
          return cur.fetchone()


def update_package_status(C, package_id, status):
     with db_lock:
          cur = C.cursor()
          sql = '''
               UPDATE package_package SET status = ? WHERE package_id = ?;
          '''
          cur.execute(sql, (status, package_id))
          C.commit()

def update_package_pos(conn, truckid, x, y):
     with db_lock:
          cur = conn.cursor()
          sql = '''UPDATE package_package SET cur_x = ?, cur_y = ? WHERE truck_id = ? AND status = 'delivering';'''
          cur.execute(sql, (x, y, truckid,))
          conn.commit()


def get_package_dst(C, package_id):
     with db_lock:
          cur = C.cursor()
          sql = '''
               SELECT dst_x, dst_y FROM package_package WHERE package_id = ?;
          '''
          cur.execute(sql, (package_id))
          return cur.fetchone()


def update_package_dst(C, package_id, dst_x, dst_y):
     with db_lock:
          cur = C.cursor()
          sql = '''
               UPDATE package_package SET dst_x = ?, dst_y = ? WHERE package_id = ?;
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
               SELECT status FROM trucks WHERE truck_id = ?;
          '''
          cur.execute(sql, (truck_id))
          return cur.fetchone()


def update_truck_status(C, truck_id, status):
     with db_lock:
          cur = C.cursor()
          sql = '''
               UPDATE trucks SET status = ? WHERE truck_id = ?;
          '''
          cur.execute(sql, (status, truck_id))
          C.commit()


def find_free_truck(C):
     with db_lock:
          cur = C.cursor()
          sql = '''SELECT truck_id FROM trucks WHERE status = 'idle' 
                    OR status = 'delivering' OR status = 'arrive warehouse';
          '''
          cur.execute(sql)
          row = cur.fetchone()
     if not row:
          return -1
     else:
          return row[0]


def get_truck_whnum(C, truck_id):
     with db_lock:
          cur = C.cursor()
          sql = '''SELECT whnum FROM trucks WHERE truck_id = ?;'''
          cur.execute(sql, (truck_id))
          return cur.fetchone()