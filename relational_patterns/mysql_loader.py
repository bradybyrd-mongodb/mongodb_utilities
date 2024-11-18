import sys
import os
import csv
from collections import OrderedDict
from collections import defaultdict
import json
import datetime
from decimal import Decimal
import random
import time
import re
import multiprocessing
import pprint
from bson.objectid import ObjectId
from bson.json_util import dumps
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(base_dir))
import process_csv_model as csvmod
from bbutil import Util
from id_generator import Id_generator
import mysql.connector
from faker import Faker

fake = Faker()

settings_file = "relations_settings.json"

def load_social_db():
    '''
    Pattern
        Load users
        extracdt users into array
        load topics - get array
        load user_topic
        load posts
        load posts rating
        load post_comment
    '''
    cur_process = multiprocessing.current_process()
    bb.message_box(f"({cur_process.name}) Loading Synth Data in SQL", "title")
    myconn = mysql_connection("social_db")
    settings = bb.read_json(settings_file)
    batches = settings["batches"]
    batch_size = settings["batch_size"]
    job_info = settings["mydata"]
    start_time = datetime.datetime.now()
    # Create Users - return - [user_id]
    users = create_users(myconn, job_info)
    topics = create_topics(myconn, job_info, users)
    create_user_topics(myconn, job_info, users, topics)
    create_messages(myconn, job_info, users)

    # feed user activity with each of these
    create_posts(myconn, job_info, topics, users) # - includes comments, ratings
    myconn.close()
    bb.logit("# ------------- COMPLETE --------------------- #")

def create_users(conn, info):
    cursor=conn.cursor(buffered=True)
    stats = info["user"]
    numtodo = stats["size"]
    bb.logit("Users - Starting")
    user_ids = []
    users = []
    for k in range(numtodo):
        user = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "password": fake.password(),
            "email": fake.unique.email(),
            "image": 'image/user.png'  # Default image
        }
        users.append(user)

    for user in users:
        cursor.execute("""
            INSERT INTO user (first_name, last_name, password, email, image)
            VALUES (%s, %s, %s, %s, %s)
            """, (user['first_name'], user['last_name'], user['password'], user['email'], user['image']))
        cur_id = cursor.lastrowid
        user_ids.append(cur_id)  # Get the last inserted user_id
    conn.commit()
    cursor.close()
    bb.logit(f"Users - complete {numtodo}")
    return user_ids

def create_topics(conn, info, user_ids):
    cursor=conn.cursor(buffered=True)
    stats = info["topic"]
    numtodo = stats["size"]
    bb.logit("Topics - starting")
    topic_ids = []
    topics = []
    for k in range(numtodo):
        topic = {
            "name": fake.bs(),
            "create_date": fake.past_datetime(start_date="-30d"),
            "creator_id": random.choice(user_ids)
        }
        topics.append(topic)

    for topic in topics:
        cursor.execute("""
            INSERT INTO topic (name, creator_id, create_date)
            VALUES (%s, %s, %s)
            """, (topic['name'], topic['creator_id'], topic['create_date']))
        cur_id = cursor.lastrowid
        topic_ids.append(cur_id)  # Get the last inserted user_id
    conn.commit()
    cursor.close()
    bb.logit(f"Topics - completed {numtodo}")
    return topic_ids

def create_user_topics(conn, info, user_ids, topic_ids):
    cursor=conn.cursor(buffered=True)
    stats = info["user"]
    numtodo = stats["size"]
    bb.logit("User_topcs - starting")
    user_topics = []
    for uid in user_ids:
        for k in range(5):
            topic = {
                "topic_id": random.choice(topic_ids),
                "user_id": uid
            }
            user_topics.append(topic)
        
    for topic in user_topics:
        cursor.execute("""
            INSERT INTO user_topic (user_id, topic_id)
            VALUES (%s, %s)
            """, (topic['user_id'], topic['topic_id']))
        log_user_activity(cursor, topic['user_id'], topic["topic_id"], f'Topic: subscribed to topic_id: {topic["topic_id"]}')
    conn.commit()
    cursor.close()
    bb.logit(f"user_topics - completed - {numtodo}")
    return topic_ids

def create_messages(conn, info, user_ids):
    cursor=conn.cursor(buffered=True)
    stats = info["user"]
    numtodo = stats["size"]
    bb.logit("Messages - Starting")
    recs = []
    for uid in user_ids:
        for k in range(5):
            rec = {
                "from_user": random.choice(user_ids),
                "to_user": uid,
                "message" : fake.sentence(),
                "chat_time": fake.past_datetime(start_date="-10d")
            }
            recs.append(rec)
        
    for rec in recs:
        cursor.execute("""
            INSERT INTO message (from_user, to_user, message, chat_time)
            VALUES (%s, %s, %s, %s)
            """, (rec['from_user'], rec['to_user'], rec['message'], rec['chat_time']))
        log_user_activity(cursor, rec['from_user'], rec['to_user'], f'Message: to user_id: {rec["to_user"]}')
    conn.commit()
    cursor.close()
    bb.logit(f'Messages: Completed')
    return

def start_in_the_middle():
    myconn = mysql_connection("social_db")
    settings = bb.read_json(settings_file)
    job_info = settings["mydata"]
    cursor=myconn.cursor(buffered=True)
    stats = job_info["user"]
    numtodo = stats["size"]
    bb.logit("Users - Starting")
    users = []
    topics = []
    cursor.execute("select user_id from user")
    records = cursor.fetchall()
    for row in records:
        users.append(row[0])
    cursor.execute("select topic_id from topic")
    records = cursor.fetchall()
    bb.logit("Topics - Starting")
    for row in records:
        topics.append(row[0])
    cursor.close()
    #create_messages(myconn, job_info, users)
    create_posts(myconn,job_info,users,topics)
    myconn.close()


    
def create_posts(conn, info, user_ids, topic_ids):
    stats = info["post"]
    cursor=conn.cursor(buffered=True)
    numtodo = stats["size"]
    bb.logit(f'Posts: Starting')
    recs = []
    for uid in user_ids:
        for k in range(5):
            rec = {
                "topic_id": random.choice(topic_ids),
                "user_id": random.choice(user_ids),
                "body": fake.sentence(),
                "post_time": fake.past_datetime(start_date="-30d"),
                "visibility" : 'all'
            }
            recs.append(rec)
    cnt = 0   
    for rec in recs:
        if cnt % 50 == 0:
            bb.logit(f'Post - {cnt}')
        cursor.execute("""
            INSERT INTO post (topic_id, user_id, body, post_time, visibility)
            VALUES (%s, %s, %s, %s, %s)
            """, (rec['topic_id'], rec['user_id'], rec["body"], rec["post_time"], rec['visibility']))
        cur_id = cursor.lastrowid
        log_user_activity(cursor, rec['user_id'], cur_id, f'POST: posted to topic_id: {rec["topic_id"]}')
        add_ratings(conn, user_ids, cur_id)
        add_comments(conn, user_ids, cur_id)
        cnt += 1
    conn.commit()
    cursor.close()
    bb.logit(f'Posts: Completed')
    return

def add_ratings(conn, user_ids, post_id):
    for k in range(3):
        u_id = random.choice(user_ids)
        cursor=conn.cursor(buffered=True)
        cursor.execute("""
            INSERT INTO post_rating (post_id, user_id, rating, timestamp)
            VALUES (%s, %s, %s, %s)
            """, (post_id, u_id, random.choice(("thumbs_up","thumbs_down","heart","poop")), fake.past_datetime(start_date="-10d")))
        cur_id = cursor.lastrowid
        log_user_activity(cursor, u_id, cur_id, f'Post_rating: rated post_id: {post_id}')
    conn.commit()
    cursor.close()

def add_comments(conn, user_ids, post_id):
    for k in range(random.randint(1,5)):
        u_id = random.choice(user_ids)
        cursor=conn.cursor(buffered=True)
        cursor.execute("""
            INSERT INTO post_comment (post_id, user_id, body, post_time)
            VALUES (%s, %s, %s, %s)
            """, (post_id, u_id, fake.sentence(), fake.past_datetime(start_date="-10d")))
        cur_id = cursor.lastrowid
        log_user_activity(cursor, u_id, cur_id, f'Post_comment: post_id: {post_id}')
    conn.commit()
    cursor.close()

def log_user_activity(cursor, user, x_id, msg):
    cursor.execute("""
            INSERT INTO user_activity (user_id, target_id, activity, timestamp)
            VALUES (%s, %s, %s, %s)
            """, (user, x_id, msg, fake.past_datetime(start_date="-10d")))

def mysql_test():
    conn = mysql_connection("social_db")
    if not conn.is_connected():
        print("Failed to connect")
        sys.exit(1)
    cursor=conn.cursor(buffered=True)
    try:
        res = cursor.execute("select * from user")
        row = cursor.fetchone()
        print("Row is: {0}".format(row[1]))
    except Exception as e:
        print(f"Error: {e}")

def mysql_execute(conn, cur, sql):
    if not conn.is_connected():
        print("Failed to connect - trying")
        conn = mysql_connection("social_db")
        cur=conn.cursor(buffered=True)
    try:
        res = cursor.execute(sql)
    except Exception as e:
        print(f"Error: {e}")


def mysql_connection(sdb = 'none'):
    # cur = mydb.cursor()
    # cur.execute("select * from Customer")
    # result = cursor.fetchall()
    type = "mysql"
    shost = settings[type]["host"]
    susername = settings[type]["username"]
    spwd = settings[type]["password"]
    if "secret" in spwd:
        spwd = os.environ.get("_MYPWD_")
    sport = settings[type]["port"]
    if sdb == 'none':
        sdb = settings[type]["database"]
    # ,password=spwd
    conn = mysql.connector.connect(
        user=susername,
        password=spwd,
        host=shost,
        database=sdb,
        port=sport,
        auth_plugin='mysql_clear_password'
        )
    return conn


# ------------------------------------------------------------------#
#     MAIN
# ------------------------------------------------------------------#
if __name__ == "__main__":
    bb = Util()
    ARGS = bb.process_args(sys.argv)
    settings = bb.read_json(settings_file)
    base_counter = settings["base_counter"]
    if "action" not in ARGS:
        print("Send action= argument")
        sys.exit(1)
    elif ARGS["action"] == "load_sql_data":
        load_social_db()
    elif ARGS["action"] == "test":
        mysql_test()
    elif ARGS["action"] == "load_middle":
        start_in_the_middle()
    
"""
# ---------------------------------------------------- #

Create Database:
    python3 load_sql.py action=execute_ddl task=create
    python3 load_sql.py action=load_pg_data
"""
