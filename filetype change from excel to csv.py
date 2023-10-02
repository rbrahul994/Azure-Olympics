#!/usr/bin/env python
# coding: utf-8

# In[23]:


from os import listdir
from os.path import isfile, join

mypath= r"C:\Users\rbrah\Documents\data-engineer\Olympics_data\raw_data\archive(5)"
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

print(onlyfiles)


# In[29]:


import openpyxl
import csv
import pandas as pd
import os

for file in onlyfiles:
    excel = openpyxl.load_workbook(os.path.join(mypath, file))
    sheet = excel.active
  
    # writer object is created
    col = csv.writer(open(os.path.join(mypath, file.split(".")[0 ]+ ".csv"), 'w', newline=""))
  
    # writing the data in csv file
    for r in sheet.rows:
        col.writerow([cell.value for cell in r])
  


# In[ ]:




