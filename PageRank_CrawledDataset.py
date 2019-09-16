
# coding: utf-8

# In[20]:


import elasticsearch
import elasticsearch_dsl
import json
from urllib.parse import urlparse
from math import log2, pow


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


# In[7]:


for link in total_data:
    print(total_data[link].keys())
    break


# In[32]:


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


# In[33]:


len(web_graph)


# In[34]:


sink_nodes = []
for link in web_graph:
    if len(web_graph[link]['outlinks']) == 0:
        sink_nodes.append(link)


# In[35]:


def perplexity(PR):
    entropy  = 0
    for link in PR:
        entropy += (PR[link] * log2(1 / PR[link]))
    return pow(2, entropy)


# In[36]:


N = len(web_graph)
PR = {}

for link in web_graph:
    PR[link] = 1/N


# In[37]:


perplexity(PR)


# In[38]:


d = 0.85


# In[39]:


perplexity_list = []


# In[40]:


while True:
    sinkPR = 0
    newPR = {}
    for p in sink_nodes:
        sinkPR += PR[p]
    for p in web_graph:
        newPR[p] = (1-d)/N
        newPR[p] += d * sinkPR / N
        
        for q in web_graph[p]["inlinks"]:
            newPR[p] += d * PR[q] / len(web_graph[q]["outlinks"])
    for p in PR:
        PR[p] = newPR[p]
    
    plex = perplexity(PR)
    print(plex)
    perplexity_list.append(int(plex % 10))
    
    if len(perplexity_list) == 4:
        if perplexity_list[0] == perplexity_list[1] and perplexity_list[1] == perplexity_list[2] and perplexity_list[2] == perplexity_list[3]:
            break
        else:
            del perplexity_list[0]
            
    


# In[41]:


sorted(PR.items(), key=lambda kv:kv[1])[::-1][0:50]

