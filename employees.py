
""" Assume you have a pool of employees - each with a set number of available hours to do the jobs assigned to them - whenever a new job arrives - a job that has expected hours associated with it - you want to pick the minimum number of employees who can do the given job and create a trigger so that when the job is expected to finish - you check the status of the job and if it is indeed finished then you update the employee status (make them eligible for next jobs) otherwise in a scenario where no more additional hours available for the employee - then you add a new employee to the job. If a job arrives for which no employee is currently available, you keep the job in a queue and start such a job as soon as possible. Please upload your code file here or simply upload a file with a link to your GitHub account """

import mysql.connector
import os
import sys
import time
import threading
from multiprocessing import Process
from datetime import datetime


# connecting to mysql
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password=""
)

# create database
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS mydatabase")

#Creating employees table
mycursor.execute("USE mydatabase;")
mycursor.execute("CREATE TABLE IF NOT EXISTS employees(id INT AUTO_INCREMENT PRIMARY KEY, hours INT,"
                 " job_id INT);")

#Creating jobs table
mycursor.execute("CREATE TABLE IF NOT EXISTS jobs(job_id INT AUTO_INCREMENT PRIMARY KEY, ex_hours INT,"
                 " start_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,ex_finish TIMESTAMP NULL, "
                 "assigned BIT(1) DEFAULT 0, finished BIT(1) DEFAULT 0);")

#Creating queue table
mycursor.execute("CREATE TABLE IF NOT EXISTS queue(job_id INT PRIMARY KEY, ex_hours INT,"
                 " start_date TIMESTAMP NULL );")


#Create test employee


#method ro add employee with 8 hours of availability
def add_employee():
  mycursor.execute("INSERT INTO employees(hours, job_id) VALUES (8,0)")
  mydb.commit()

#method to get the job from user
def get_job():
  ex_hours = 0
  # run as long as there are no job hours or the input is invalid
  while(ex_hours==0):
    ex_hours = input("please enter the expected hours for the job as a non-zero integer: \n")
    if(ex_hours.isnumeric()):
      ex_hours = int(ex_hours)
      mycursor.execute("INSERT INTO jobs(ex_hours, assigned, finished) VALUES (%s, %s, %s)",(ex_hours, 0, 0))
      mydb.commit()
    else:
      ex_hours=0


# getting the available employees
def get_avl_emps():
  query = ("SELECT * FROM employees WHERE employees.job_id=0;")
  mycursor.execute(query)
  emps = []
  for row in mycursor:
      emps.append(row)
  # sort employees by working hours so we can assign the minimum number of employees to the job
  emps = sorted(emps, key=lambda x: x[1], reverse=True)
  return(emps)

# getting the unfinished jobs
def get_avl_jobs():
  query = ("SELECT * FROM jobs WHERE jobs.assigned=0;")
  mycursor.execute(query)
  jbs = []
  for row in mycursor:
      jbs.append(row)
  return(jbs)

# getting all the employees
def get_all_emps():
  query = ("SELECT * FROM employees")
  mycursor.execute(query)
  emps = []
  for row in mycursor:
      emps.append(row)
  # sort employees by working hours so we can assign the minimum number of employees to the job
  emps = sorted(emps, key=lambda x: x[1], reverse=True)
  return(emps)

# getting all the jobs
def get_all_jobs():
  query = ("SELECT * FROM jobs ;")
  mycursor.execute(query)
  jbs = []
  for row in mycursor:
      jbs.append(row)
  return(jbs)

# get the content of queue
def get_all_queue():
  query = ("SELECT * FROM queue ;")
  mycursor.execute(query)
  jbs = []
  for row in mycursor:
      jbs.append(row)
  return(jbs)

# assign jobs to minimum number of employees
def emp_assign():
  avl_emps = get_avl_emps()
  avl_jobs = get_avl_jobs()
  # iterating over the number of jobs
  for j in range(len(avl_jobs)):
    # check if we have enough hours for a job
    if (sum(row[1] for row in avl_emps) > avl_jobs[j][1]):
      hours_needed = avl_jobs[j][1]
      employees_num = 0
      emp_hours = 0
      # getting the number of employees to finish the job
      while (emp_hours < hours_needed):
        emp_hours += avl_emps[employees_num][1]
        employees_num += 1
      # assigning the selected employees to the job
      for i in range(employees_num):
        mycursor.execute("UPDATE employees SET job_id = %s WHERE employees.id=%s", (avl_jobs[j][0], avl_emps[i][0]))
        mydb.commit()
      # trigerring expected finish for job and set assigned to 1
      mycursor.execute("UPDATE jobs SET assigned = %s WHERE jobs.job_id=%s", (1, avl_jobs[j][0]))
      mydb.commit()
      mycursor.execute("UPDATE jobs SET ex_finish = NOW() + INTERVAL %s HOUR WHERE jobs.job_id=%s",
                       (avl_jobs[j][1], avl_jobs[j][0]))
      mydb.commit()
      print("   job {} assigned to the employee/s \n".format(avl_jobs[j][0]))
    else:
      print("   not enough employees for job {} so we will ad an employee with 8 hours of work \n".format(avl_jobs[j][0]))
      add_employee()

#updating tables
def update():

  #check if we have any new job
  while (get_avl_jobs()):
    #check if we have any avalable employees
    if(get_avl_emps()):
      emp_assign()
    # if we do not have any available employees
    else:
      print("There are not enough employees so we add the job to the queue")
      avl_job = get_avl_jobs()
      # adding the job to the queue
      mycursor.execute("INSERT INTO queue ( job_id, ex_hours, start_date) VALUES (%s, %s, %s);", (avl_job[0][0], avl_job[0][1], avl_job[0][2]))
      mydb.commit()
      mycursor.execute("DELETE FROM jobs WHERE job_id = %s ;", (avl_job[0][0],))
      mydb.commit()
      break
  else:
    pass


#method to check the status of the job and implement changes when it finishes
def check_jobs():
  # check if there are any jobs
  if(get_all_jobs()):
    # getting the current time
    now = datetime.now()
    all_jobs = get_all_jobs()
    all_emps = get_all_emps()
    # check if the job is finished
    for i in range(len(all_jobs)):
      #check if the job status is unfinished and the expected finish time is past
      if ( all_jobs[i][5] == 0 and all_jobs[i][3] < now):
        # update the job as finished
        mycursor.execute("UPDATE jobs SET finished = 1 WHERE job_id = %s", (all_jobs[i][0],))
        mydb.commit()
        print("   job {} is finished".format(all_jobs[i][0]))

        #return the coroosponding employees to the available status
        for j in range(len(all_emps)):
          if(all_emps[j][2] == all_jobs[i][0]):
            mycursor.execute("UPDATE employees SET job_id = 0 WHERE id = %s", (all_emps[j][0],))
            mydb.commit()

  # check if there are any available employees:
  while(get_avl_emps()):
    # Checking if there are jobs waiting in the queue
    if(get_all_queue()):
      queue = get_all_queue()
      # Insert the top of queue into jobs
      mycursor.execute("INSERT INTO jobs(job_id, ex_hours, start_date, assigned,"
                       " finished) VALUES (%s, %s, %s, %s, %s)",(queue[0][0], queue[0][1], queue[0][2], 0, 0))
      mydb.commit()
      # deleting the corrosponding job from queue
      mycursor.execute("DELETE FROM queue WHERE job_id = %s ;", (queue[0][0],))
      mydb.commit()
      # update the database based on the new job
      update()
    # if there are no jobs in the queue break the loop
    else:
      break







#code that runs every period
def period(seconds):
  WAIT_TIME_SECONDS = seconds

  ticker = threading.Event()
  while not ticker.wait(WAIT_TIME_SECONDS):
    check_jobs()

# handle the current state of code
def current_state():
  #checking every hour to update the tables if a job is finished
  seconds = 3600
  period_thread = Process(target=period, args=(seconds, ))
  period_thread.start()

  # Create 3 test employees with 8 hours of work if they don't exist
  if(get_all_emps() == []):
    for i in range(3):
      add_employee()

  inp = 0
  # asking the user what they want to do
  while(inp != 2):
    inp = input("Please press 1 to add a job \n2 to terminate the program: \n")
    if(inp.isnumeric()):
      inp = int(inp)
      if (inp == 1):
        # get the job information from user and update the database
        get_job()
        update()
    else:
      inp = 0
  else:
    period_thread.terminate()

if __name__ == "__main__":
  # run the program
  current_state()

  #close the program
  mydb.close()
  print('ok')
  sys.exit()
