
# coding: utf-8

# In[1]:


from math import log10
import numpy as np
import matplotlib.pyplot as plt


# In[2]:


# with open("sorted_tfidf", "r") as file:
#     rankedListf = file.read()


# In[3]:


with open("vertical_search_rank", "r") as file:
    rankedListf = file.read()


# In[4]:


rankedList = {}
ranks = rankedListf.split('\n')[:-1]
for r in ranks:
    r = r.replace("  ", " ")
    r = r.split(" ")
    if r[0] not in rankedList.keys():
        rankedList[r[0]] = {}
    rankedList[r[0]][r[2]] = float(r[4])
    


# In[5]:


# with open("C:\\Users\\priyu\\Desktop\\NEU\\Information Retrieval\\Assignments\\Dataset\\AP89_DATA\\AP_DATA\\qrels.adhoc.51-100.AP89.txt", "r") as file:
#     qrelf = file.read()


# In[6]:


with open("final_qrel", "r") as file:
    qrelf = file.read()


# In[7]:


qrelList = {}
qrels = qrelf.split('\n')[:-1]
for q in qrels:
    q = q.split(" ")
#     print(q)
    if q[0] not in qrelList.keys():
        qrelList[q[0]] = {}
    qrelList[q[0]][q[2]] = int(q[3])
    


# In[9]:


avg_precision_list = [0 for i in range(1000)]
avg_recall_list = [0 for i in range(1000)]
avg_f1score_list = [0 for i in range(1000)]
avg_ndcg = 0
avg_dcg = 0
avg_r_precision = 0
avg_avg_precision = 0

total = 0

option = "-q"

for r in rankedList:
    total = total + 1
    
    sortedRank = sorted(rankedList[r], key=rankedList[r].get)[::-1]
    total_rel_qrel = 0
    for doc in qrelList[r]:
        if qrelList[r][doc] > 0:
            total_rel_qrel = total_rel_qrel + 1
#     print(total_rel_qrel)      
    retrieved = 0
    relevant_retrieved = 0
    sum_precision = 0
    
    precision = []
    recall = []
    f1 = []
    relevant_matrix = []

    
    for doc in sortedRank:
        retrieved += 1
        
        grade = 0
        rr = 0
        
        if doc in qrelList[r]:
            if int(qrelList[r][doc]) > 0:
                grade = 1
                rr = int(qrelList[r][doc])
        relevant_matrix.append(rr)
        
        if grade != 0:
            sum_precision = sum_precision + (grade * (1 + relevant_retrieved) / retrieved)
            relevant_retrieved += 1
            
        precision_score = relevant_retrieved / retrieved
        recall_score = relevant_retrieved / total_rel_qrel
        
        f1score = 0
         
        if (precision_score + recall_score) != 0:
            f1score = 2 * precision_score * recall_score / (precision_score + recall_score)
        
        precision.append(precision_score)
        recall.append(recall_score)
        f1.append(f1score)
        
    avg_precision = sum_precision / total_rel_qrel
    final_recall = relevant_retrieved / total_rel_qrel
        
    for i in range(len(precision)):
        avg_precision_list[i] += precision[i]
        avg_recall_list[i] += recall[i]
        avg_f1score_list[i] += f1[i]
    
    
    
    relevant_retrieved = relevant_retrieved - 1
    
    if total_rel_qrel > retrieved:
        r_prec = float(relevant_retrieved / total_rel_qrel)
    else:
        integer_relevant = int(total_rel_qrel)
        fraction_relevant = total_rel_qrel - integer_relevant
        
        if fraction_relevant > 0:
            r_prec = (1 - fraction_relevant) * precision[integer_relevant] + (fraction_relevant * precision[integer_relevant + 1])
        else:
            r_prec = precision[integer_relevant]
#     print(r, r_prec)
#     print(relevant_retrieved)
    
    dcg = relevant_matrix[0]
    
    for i in range(1,len(relevant_matrix)):
        dcg = dcg + relevant_matrix[i] / log10(i+1)
    
    relevant_matrix = sorted(relevant_matrix)[::-1]
    
    
    dcg_sorted = relevant_matrix[0]
    
    for i in range(1,len(relevant_matrix)):
        dcg_sorted = dcg_sorted + relevant_matrix[i] / log10(i+1)
    
    ndcg = dcg / dcg_sorted
#     print(dcg, ndcg)
    
    
    if option == "-q":
        print("Query no :",r)
        print("Total relevant:",total_rel_qrel )
        print("Total relevant retrieved:",relevant_retrieved)
        print("\n")
        print("K , Precision , recall , f1")
        K = [5,10, 20, 50, 100]
        for k in K:
            print(k,",",round(precision[k],4),",",round(recall[k],4),",",round(f1[k],4))
        print("\n")
        print("Average Precision: ", avg_precision)
        print("r-precision: ", r_prec)
        print("discounted cumilative gain: ", dcg)
        print("normalized discounted cumilative gain: ", ndcg)
        
        print(len(recall))

        
        plt.plot(recall, precision)
        plt.show()

        
        print("------------------------------------")
    
    avg_ndcg = avg_ndcg + ndcg
    avg_dcg = avg_dcg + dcg
    avg_r_precision = avg_r_precision + r_prec
    avg_avg_precision = avg_avg_precision + avg_precision

avg_ndcg = avg_ndcg / total
avg_dcg = avg_dcg / total
avg_r_precision = avg_r_precision / total
avg_avg_precision = avg_avg_precision / total


avg_precision_list = np.array(avg_precision_list) / total
avg_recall_list = np.array(avg_recall_list) / total
avg_f1score_list = np.array(avg_f1score_list) / total

print("K , Precision , recall , f1")
K = [5, 10, 20, 50, 100]
for k in K:
    print(k,",",round(avg_precision_list[k],4),",", round(avg_recall_list[k],4),",", round(avg_f1score_list[k],4))

    
print("\n")
print("Average precision over all queries:", avg_avg_precision)    
print("Average r-precision over all queries:", avg_r_precision)
print("Average discounted cumulative over all queries:", avg_dcg)
print("Average normalised cumulative over all queries:", avg_ndcg)    



#     break

