from datetime import datetime, timedelta
import random
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    user="root",
    password="root"
)
cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS support_db")
cursor.execute("USE support_db")

cursor.execute("DROP TABLE IF EXISTS calls")
cursor.execute("DROP TABLE IF EXISTS employees")

cursor.execute("""
CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    full_name VARCHAR(255),
    team_role VARCHAR(100),
    hire_date DATE
)
""")

cursor.execute("""
CREATE TABLE calls (
    call_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT,
    call_time DATETIME,
    phone VARCHAR(20),
    direction ENUM('inbound', 'outbound'),
    status ENUM('completed', 'missed', 'busy'),
    FOREIGN KEY (employee_id) REFERENCES employees(id)
)
""")

roles = ['Support Specialist', 'Senior Support', 'Team Lead', 'Customer Success']
employees = []
for i in range(1, 51):
    name = f"Employee {i}"
    role = random.choice(roles)
    h_date = (datetime.now() - timedelta(days=random.randint(30, 1000))).date()
    employees.append((name, role, h_date))

cursor.executemany(
    "INSERT INTO employees (full_name, team_role, hire_date) VALUES (%s, %s, %s)",
    employees
)

directions = ['inbound', 'outbound']
statuses = ['completed', 'missed', 'busy']
phones = ['+380671112233', '+380509998877', '+380934445566']

calls = []
for _ in range(200):
    emp_id = random.randint(1, 50)
    time = datetime.now() - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 1440))
    calls.append((
        emp_id,
        time.strftime('%Y-%m-%d %H:%M:%S'),
        random.choice(phones),
        random.choice(directions),
        random.choice(statuses)
    ))

cursor.executemany(
    "INSERT INTO calls (employee_id, call_time, phone, direction, status) VALUES (%s, %s, %s, %s, %s)",
    calls
)

conn.commit()
cursor.close()
conn.close()

import json
import os

json_dir = 'include/telephony_data'
os.makedirs(json_dir, exist_ok=True)

for f in os.listdir(json_dir):
    os.remove(os.path.join(json_dir, f))

for i in range(1, 201):
    call_id = i
    data = {
        "call_id": call_id,
        "duration_sec": random.randint(10, 600),
        "short_description": random.choice(["Support inquiry", "Billing issue", "Technical glitch", "General feedback"])
    }

    with open(f"{json_dir}/call_{call_id}.json", "w") as f:
        json.dump(data, f)
