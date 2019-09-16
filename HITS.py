
# coding: utf-8

# In[1]:


import elasticsearch
import elasticsearch_dsl
import json
from urllib.parse import urlparse
from math import log2, pow
import numpy as np
import random
import math


# In[2]:


es = elasticsearch.Elasticsearch()
es_response = elasticsearch.helpers.scan(
    es,
    index='crawled_data_final',
    doc_type='doc',
    query={"query": { "match_all" : {}}}
)


# In[3]:


total_data = {}


# In[4]:


for link in es_response:
    total_data[link['_id']] = link['_source']


# In[5]:


len(total_data)


# In[6]:


for link in total_data:
    print(total_data[link].keys())
    break


# In[7]:


web_graph = {}
for link in total_data:
    if link not in web_graph:
        web_graph[link] = {
            "inlinks": [],
            "outlinks": []
        }
    modified_outlinks = []
    for outlink in total_data[link]['OUTLINK']:
        if outlink in total_data:
            modified_outlinks.append(outlink)
        else:
            modified_outlinks.append(urlparse(outlink).scheme + "://" + urlparse(outlink).netloc)

    web_graph[link]['outlinks'] = list(set(modified_outlinks))
    for inlink in web_graph[link]['outlinks']:
        if inlink not in web_graph:
            web_graph[inlink] = {
                "inlinks": [],
                "outlinks": []
            }
        web_graph[inlink]['inlinks'].append(link)
        web_graph[inlink]['inlinks'] = list(set(web_graph[inlink]['inlinks']))


# In[8]:


len(web_graph)


# In[9]:


# https://elasticsearch-py.readthedocs.io/en/master/api.html


# In[32]:


body = {
    "query" : {
        "match" : {
            "TEXT" : "Founding Fathers"
        }
    }
}


hits = es.search(index = 'crawled_data_final', doc_type = 'doc', body = body, size = 1000)


# In[11]:


hits.keys()


# In[12]:


hits['hits'].keys()


# In[13]:


# hits['hits']['hits'][0]


# In[33]:


root_set = []
for url in hits['hits']['hits']:
    root_set.append(url['_id'])


# In[34]:


len(root_set)


# In[35]:


temp_root_set = []
for url in root_set:
    temp_root_set.append(url)


# In[36]:


root_set = []
for url in temp_root_set:
    root_set.append(url)


# In[37]:


base_set = []
base_set += root_set
i = 0
d = 200
while(i < 3 and len(set(base_set)) < 10000):
    for link in root_set:
        temp_set = []
        base_set += web_graph[link]['outlinks']
        temp_set = temp_set + web_graph[link]['inlinks']
        temp_set = list(set(temp_set))
        if len(temp_set) > 200:
            newtempset = []
            r = random.sample(range(0, len(temp_set)), 200)
            for a in r:
                newtempset.append(temp_set[a])
            base_set += newtempset
        else:
            base_set += temp_set
            
        if i > 0 and len(set(base_set)) > 10000:
            break
    root_set += base_set
    root_set = list(set(root_set))
    print(i)
    i = i +  1


# In[38]:


len(set(base_set))


# In[39]:


url_id = {}
id_url = {}
id = 0
for link in root_set:
    url_id[link] = id
    id_url[id] = link
    id += 1


# In[40]:


adj_list = np.zeros((len(root_set), len(root_set)))


# In[22]:


# type(url_id['https://de.wikipedia.org/wiki/Library_of_Congress'])


# In[23]:


# http://pi.math.cornell.edu/~mec/Winter2009/RalucaRemus/Lecture4/lecture4.html


# In[41]:


i = 0
for url in root_set:
#     print(url)
    id = url_id[url]
    for links in web_graph[url]['outlinks']:
        if links not in root_set or links == url:
            continue
        outlinkid = url_id[links]
        adj_list[id][outlinkid] = 1
        
    for links in web_graph[url]['inlinks']:
        if links not in root_set or links == url:
            continue
        inlinkid = url_id[links]
        adj_list[inlinkid][id] = 1
    i = i + 1
    if(i % 100 == 0):
        print(i)
    
        
        


# In[42]:


hubs = np.ones(len(root_set))
authority = np.ones(len(root_set))


# In[43]:


hubs = hubs / math.sqrt(sum(np.square(hubs)))
authority = authority / math.sqrt(sum(np.square(authority)))


# In[44]:


epsilon = 1.0e-6
for i in range(100):
    new_authority = adj_list.T.dot(hubs)
    new_hubs = adj_list.dot(authority)
    
    new_hubs = new_hubs / math.sqrt(sum(np.square(new_hubs)))
    new_authority = new_authority / math.sqrt(sum(np.square(new_authority)))
    
    
    if (np.abs(new_authority - authority) > epsilon).any() or (np.abs(new_hubs - hubs) > epsilon).any() :
        hubs = new_hubs
        authority = new_authority
    else:
        print(i)
        break
        
    


# In[45]:


with open("authority_score.txt", "w") as file:
    for i in authority.argsort()[::-1][0:500]:
        file.write(id_url[i]+"   "+str(authority[i])+"\n")
    
with open("hubs_score.txt", "w") as file:
    for i in hubs.argsort()[::-1][0:500]:
        file.write(id_url[i]+"   "+str(hubs[i])+"\n")


    


# In[46]:


for i in authority.argsort()[::-1][0:500]:
    print(id_url[i])
    


# In[47]:


for i in hubs.argsort()[::-1][0:500]:
    print(id_url[i])
    

