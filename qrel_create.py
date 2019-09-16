
# coding: utf-8

# In[1]:


import pymysql.cursors


# In[8]:


conn = pymysql.connect(host = "localhost", user = "root", password = "1234", db  = "qrels", 
                       cursorclass = pymysql.cursors.DictCursor)
try:
    with conn.cursor() as cursor:
        sql = "Select * FROM url_ratings"
        cursor.execute(sql)
        result = cursor.fetchall()
        
    with open("qrel_priya", "w") as file:
        for row in result:
#             print(row)
            file.write(str(row['queryid']) + " " + str(row['accessor_id']) + " " + row['urlname'] + " " + str(row["grade"]) + "\n")
finally:
    conn.close()

