import os

with open('C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\测试数据.txt', encoding='utf-8') as file_obj:
    lines_0 = file_obj.readlines()
with open('C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\Classified_Result3\\part-r-00000', encoding='utf-8') as file_obj:
    lines_1 = file_obj.readlines()
print("Finish Read In")
print(len(lines_0))
print(len(lines_1))
dict_2 = {}
for line in lines_1:
    #print(line[0:-4])
    hash_str = hash(line[0:-4])
   # print(hash_str)
    dict_2[hash_str] = line[-3:-2]
   # print(dict_2[hash_str])
with open('./2.txt','a') as f:
    for line in lines_0:
        hash_str = hash(line[0:-1])
        i=line[0:-1]
        if hash_str in dict_2:
            f.write(dict_2[hash_str]+'\n')
        else:
            raise Exception("Error")



