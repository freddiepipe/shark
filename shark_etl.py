#!/usr/bin/env python
# coding: utf-8

# ## Using pyodbc to connect to sql 

# In[46]:


import pyodbc
import csv
from datetime import datetime, timedelta


# In[47]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()


# In[48]:


sharkfile = r'c:\data\GSAF5.csv'


# In[49]:


attack_dates = []
isfatal = []
case_number = []
country = []
activity = []
age = []
gender = []
with open(sharkfile) as f:
    reader = csv.DictReader(f)
    for row in reader:
        case_number.append(row['Case Number'])
        attack_dates.append(row['Date'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])
 


# In[50]:


data = zip(attack_dates, case_number, country, activity, age, gender, isfatal)


# In[51]:


#q  = 'select * from freddie.shark'
q = 'insert into freddie.shark (attack_date, case_number, country, activity, age, gender, isfatal) values (?, ?, ?, ?, ?, ?, ?)'
p = ['2019-10-31', 'dummy124', 'england', 'snorkelling', 21, 'm', 1]


# In[52]:


cur.execute('truncate table freddie.shark')


# In[43]:


for d in data:
    try:
        cur.execute(q, d)
        conn.commit()
    except:
        conn.rollback()


# In[ ]:




