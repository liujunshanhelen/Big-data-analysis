import os

with open('C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\聚类数据.txt', encoding='utf-8') as file_obj:
    lines_0 = file_obj.readlines()
with open('C:\\Users\\LiuJunshan\\Desktop\\大数据分析实验一\\2\\实验2数据\\KMeans_new_center_path\\part-r-00000', encoding='utf-8') as file_obj:
    lines_1 = file_obj.readlines()
for line in lines_0:
    line_0=str(line)
    str_list0 = line_0.split()
    for line1 in lines_1:
        line_1=str(line1)
        str_list1 = line_1.split()
        if(str_list0[0]==str_list1[1]):
            print(str_list1[0])
            with open('./1.txt','a') as f:
                f.write(str_list1[0]+'\n')
                f.close()
file_obj.close()





