
# coding: utf-8

# In[1]:


from math import log2, pow


# In[2]:


inlinkfile = open('C:\\Users\\priyu\\Desktop\\NEU\\Information Retrieval\\Assignments\\Dataset\\wt2g_inlinks\\wt2g_inlinks.txt', "r")
inlinkfile = inlinkfile.read()


# In[3]:


web_graph = {}
for line in inlinkfile.split('\n')[:-1]:
    line = line.split(" ")[:-1]
    if len(line) == 0:
        continue
    if line[0] not in web_graph:
        web_graph[line[0]] = {
            "inlinks": [],
            "outlinks": []
        }
    web_graph[line[0]]['inlinks'] = line[1:]
    for out in line[1:]:
        if out not in web_graph:
            web_graph[out] = {
                "inlinks": [],
                "outlinks": []
            }
        web_graph[out]['outlinks'].append(line[0])
    


# In[4]:


sink_nodes = []
for link in web_graph:
    if len(web_graph[link]['outlinks']) == 0:
        sink_nodes.append(link)


# In[5]:


#  sink_nodes


# In[6]:


def perplexity(PR):
    entropy  = 0
    for link in PR:
        entropy += (PR[link] * log2(1 / PR[link]))
    return pow(2, entropy)


# In[7]:


N = len(web_graph)
PR = {}

for link in web_graph:
    PR[link] = 1/N


# In[8]:


d = 0.85


# In[9]:


perplexity_list = []


# In[10]:


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
            
    


# In[11]:


sorted(PR.items(), key=lambda kv:kv[1])[::-1][0:50]

