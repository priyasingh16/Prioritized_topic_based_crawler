
# coding: utf-8

# In[53]:


from datetime import datetime
import json
from elasticsearch import Elasticsearch
from math import log
from statistics import median


# In[36]:


es = Elasticsearch()

body = {
  "size": 1000,
  "query": {
      "query_string": {
          "query": "independence war causes"
      }
  }
}
res_q1 = es.search(index="crawled_data_final", doc_type="doc", body=body)


body = {
  "size": 1000,
  "query": {
      "query_string": {
          "query": "founding father"
      }
  }
}
res_q2 = es.search(index="crawled_data_final", doc_type="doc", body=body)


# In[38]:


len(res_q2['hits']['hits'])


# In[39]:


with open("vertical_search_rank", "w") as file:
    rank = 0
    for hit in (res_q1['hits']['hits']):
        rank = rank + 1
        file.write("1 Q0 " +  hit['_id'] + " " + str(rank) + " " + str(hit['_score']) + " Exp \n")
    rank = 0    
    for hit in (res_q2['hits']['hits']):
        rank = rank + 1
        file.write("2 Q0 " +  hit['_id'] + " " + str(rank) + " " + str(hit['_score']) + " Exp \n")


# In[56]:


final_merge = {}
with open("qrel_priya") as file:
    lines = file.read().split("\n")[:-1]
    for line in lines:
        line = line.split()
        if line[0] not in final_merge:
            final_merge[line[0]] = {}
        final_merge[line[0]][line[2]] = int(line[3])


# In[57]:


final_merge_parth = {}
with open("qrel_parth") as file:
    lines = file.read().split("\n")[:-1]
    for line in lines:
        line = line.split()
        if line[0] not in final_merge_parth:
            final_merge_parth[line[0]] = {}
        final_merge_parth[line[0]][line[2]] = int(line[3])


# In[58]:


final = {}


# In[62]:


with open("final_qrel", "w") as file:
    for query in final_merge:
        for url in final_merge[query]:
            if url in final_merge_parth[query]:
                med = median([final_merge[query][url], final_merge_parth[query][url]])
                file.write(query + " 0 " + url + " " + str(int(med)) + "\n")

