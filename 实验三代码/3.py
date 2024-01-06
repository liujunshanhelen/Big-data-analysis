import os

with open('C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\聚类数据.txt', encoding='utf-8') as file_obj:
    lines_0 = file_obj.readlines()
with open('C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\KMeans_new_center_path\\part-r-00000', encoding='utf-8') as file_obj:
    lines_1 = file_obj.readlines()
print("Finish Read In")
print(len(lines_0))
print(len(lines_1))
dict_2 = {}
for line in lines_1:
    hash_str = hash(line[2:])
    print(hash_str)
    dict_2[hash_str] = line[:1]
with open('./1.txt','a') as f:
    n = 0
    for line in lines_0:
        # n = n+1
        # if n%100==0:
        #     print(n)
        hash_str = hash(line)
        if hash_str in dict_2:
            f.write(dict_2[hash_str]+'\n')
        else:
            raise Exception("Error")



