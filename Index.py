
# coding: utf-8

# In[1]:


import json
import glob
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from urllib.parse import urlparse


# In[2]:


paths = glob.glob("files\\*.json")


# In[3]:


inlinks_priya = {}


# In[4]:


with open("inlinks_priya.json") as f:
    inlinks_priya = json.load(f)


# In[5]:


inlinks_parth = {}


# In[6]:


with open("inlinks_parth.json") as f:
    inlinks_parth = json.load(f)


# In[8]:


len(inlinks_parth), len(inlinks_priya)


# In[7]:


# mapping = {
#   "doc": {
#     "properties": {
#       "DOCNO": {
#         "type": "keyword",
#         "store": True
#       },
#       "TEXT": {
#         "type": "text",
#         "store": True,
#         "fielddata": True,
#         "term_vector": "with_positions_offsets_payloads"
#       },
#       "HTML" :{"type": "text"},
#       "INLINK" :{"type": "text"},
#       "OUTLINK" :{"type": "text"},
#       "AUTHOR" :{"type":"text"},
#       "TITLE" :{"type":"text"},
#         "DEPTH" : {"type:"integer"}
#     }
#   }
# }


# In[8]:


# body = {
#     "settings": {
#         "index": {
#           "store": {
#             "type": "fs"
#           },
#           "number_of_shards": 1,
#           "number_of_replicas": 1
#         }    
#     },
#     "mappings": mapping
# }


# In[9]:


es = Elasticsearch()


# In[12]:


# path


# In[10]:


# es.indices.create(index = "crawled_data_test", body = body)


# In[12]:


id_scheme_removed = []


# In[21]:


x = 0
for path in paths[10000:]:
    with open(path) as file:
#         print(path)
        data = json.load(file)
#         print(data.keys())

        data_enter = {}        
        data_enter["DOCNO"] = data['url']
        


        
        parsedUrl = urlparse(data["url"].lower())
        
        if(parsedUrl.netloc + parsedUrl.path) in id_scheme_removed:
            data_enter["TEXT"] = " "
        if(parsedUrl.netloc + parsedUrl.path) not in id_scheme_removed:
            data_enter["TEXT"] = data['cleantext']
            id_scheme_removed.append(parsedUrl.netloc + parsedUrl.path)

        data_enter["TEXT"] = data['cleantext']
        data_enter["RAW_HTML"] = ""
        data_enter["INLINK"] = []
        data_enter["TITLE"] = data['title']
        data_enter["DEPTH"] = data['depth']


        
        if data['url'] in inlinks_priya:
            data_enter["INLINK"] = data_enter["INLINK"] + inlinks_priya[data['url']]
        
        if data['url'] in inlinks_parth:
            data_enter["INLINK"] = data_enter["INLINK"] + inlinks_parth[data['url']]
            
        data_enter["INLINK"] = list(set(data_enter["INLINK"]))
        data_enter["OUTLINK"] = list(set(data["outlinks"]))
        data_enter["AUTHOR"] = ["Priya"]
        
        response = es.get(index = "crawled_data_final", doc_type='doc', id=data_enter["DOCNO"], ignore=404)
        
        if response['found'] == True:
            if response['_source']["TEXT"] == " ":
                data_enter["TEXT"] = " "
                print(data_enter["DOCNO"])
            data_enter["INLINK"] = list(set(response["_source"]["INLINK"] + data_enter["INLINK"]))
            data_enter["OUTLINK"] = list(set(response["_source"]["OUTLINK"] + data_enter["OUTLINK"]))
            data_enter["AUTHOR"] = list(set(response["_source"]["AUTHOR"] + data_enter["AUTHOR"]))
        
#         print(data_enter.keys())        
        es.index(index = "crawled_data_final", doc_type = "doc", body = data_enter, id = data_enter["DOCNO"])
            


# In[47]:


#         if(len(data_enter["DOCNO"]) > 512):
#             data_enter["DOCNO"] = data_enter["DOCNO"][0:512]

