import pandas as pd
from py2neo import Node, Relationship, Graph, NodeMatcher, RelationshipMatcher

def main():

    join_str = '_'
    #设置特征名拼接符
    count1 = len(open("featnames.txt", 'r').readlines())
    featnames = open("featnames.txt", "r")
    feature_data = pd.DataFrame(columns=['num', 'fea_name', 'fea_data'])
    fea_count = []

    #遍历所有节点属性
    for i in range(count1):
        contents = featnames.readline()
        contents_list = contents.split(';')
        if len(contents_list) == 2:
            first = contents_list[0].split(' ')
            second = contents_list[1].split(' ')
            feature_data = feature_data.append({'num': int(first[0]), 'fea_name': first[1], 'fea_data': int(second[2])},
                                               ignore_index=True)

            if first[1] not in fea_count:
                fea_count.append(first[1])

        elif len(contents_list) == 3:
            first = contents_list[0].split(' ')
            third = contents_list[2].split(' ')
            feature_data = feature_data.append({'num': int(first[0]), 'fea_name': first[1] + join_str + contents_list[1],
                                                'fea_data': int(third[2])}, ignore_index=True)

            if (first[1] + join_str + contents_list[1]) not in fea_count:
                fea_count.append(first[1] + join_str + contents_list[1])

        elif len(contents_list) == 4:
            first = contents_list[0].split(' ')
            fourth = contents_list[3].split(' ')
            feature_data = feature_data.append({'num': int(first[0]), 'fea_name': first[1] + join_str + contents_list[1] + join_str + contents_list[2],
                                                'fea_data': int(fourth[2])}, ignore_index=True)

            if (first[1] + join_str + contents_list[1] + join_str + contents_list[2]) not in fea_count:
                fea_count.append(first[1] + join_str + contents_list[1] + join_str + contents_list[2])

    count2 = len(open("feat.txt", 'r').readlines())
    feaboolean = open("feat.txt", "r")
   # graph = Graph('http://localhost:7474/', username='neo4j', password='123456')
    graph = Graph("http://localhost:7474", auth=("neo4j", "123456"))
    #记录圈子属性
    node_circle_list = []
    for i in range(count2):
        node_circle = []
        node_circle_list.append(node_circle)

    circles = open("circles.txt",'r')
    count4 = len(open("circles.txt",'r').readlines())

    for i in range(count4):
        contents = circles.readline()
        contents_list = contents.split('\t')
        content_len = len(contents_list)
        for j in range(1, content_len):
            node_circle_list[int(contents_list[j])-1].append(contents_list[0])

    #节点集合，在之后的边创建中会用到
    node_list = []

    #对节点进行映射，同时创建节点，好处是值为空的属性会被自动删除
    for i in range(count2):
        fea_dict = {key: None for key in fea_count}
        contents = feaboolean.readline()
        contents_list = contents.split(' ')
        for j in range(1, len(contents_list)):
            if contents_list[j-1] == '1':
                fea_dict[feature_data.loc[j-1, "fea_name"]] = feature_data.loc[j-1, "fea_data"]
        myNode = Node("Person", birthday=fea_dict['birthday'], education_classes_id=fea_dict['education_classes_id'],
                      education_concentration_id=fea_dict['education_concentration_id'],
                      education_degree_id=fea_dict['education_degree_id'],education_school_id=fea_dict['education_school_id'],
                      education_type=fea_dict['education_type'],
                      education_with_id=fea_dict['education_with_id'],education_year_id=fea_dict['education_year_id'],
                      first_name=fea_dict['first_name'],gender=fea_dict['gender'],
                      hometown_id=fea_dict['hometown_id'],languages_id=fea_dict['languages_id'],
                      last_name=fea_dict['last_name'],locale=fea_dict['locale'],location_id=fea_dict['location_id'],
                      work_employer_id=fea_dict['work_employer_id'],work_end_date=fea_dict['work_end_date'],
                      work_location_id=fea_dict['work_location_id'],work_position_id=fea_dict['work_position_id'],
                      work_start_date=fea_dict['work_start_date'],work_with_id=fea_dict['work_with_id'],
                      circle = node_circle_list[i])

        node_list.append(myNode)
        graph.create(myNode)

    edges = open("edges.txt", "r")
    count3 = len(open("edges.txt", 'r').readlines())

    for i in range(count3):
        contents = edges.readline()
        contents_list = contents.split(' ')
        first = int(contents_list[0])
        second = int(contents_list[1])
        relationship = Relationship(node_list[first-1],"Be_Friend_With",node_list[second-1])
        relationship["undirected"] = True
        graph.create(relationship)

    #导入中心点信息
    center_data = open("egofeat.txt",'r').read()
    count5 = len(open("feat.txt", 'r').readlines())
    feaboolean = open("feat.txt", "r")
    center_node = center_data.split(' ')


    #寻找中心点是否包含在节点列表里
    for i in range(count5):
        contents = feaboolean.readline()
        contents_list = contents.split(' ')
        flag = True
        for j in range(1, len(contents_list)):
            if int(contents_list[j]) != int(center_node[j-1]):
                flag = False
                break
        if flag == True:
            center_id = i
            print("The center_id is ",i)
            break

    fea_dict = {key: None for key in fea_count}
    for j in range(len(center_node)):
        if center_node[j] == '1':
            fea_dict[feature_data.loc[j, "fea_name"]] = feature_data.loc[j, "fea_data"]

    CenterNode = Node("Person", birthday=fea_dict['birthday'], education_classes_id=fea_dict['education_classes_id'],
                  education_concentration_id=fea_dict['education_concentration_id'],
                  education_degree_id=fea_dict['education_degree_id'],
                  education_school_id=fea_dict['education_school_id'],
                  education_type=fea_dict['education_type'],
                  education_with_id=fea_dict['education_with_id'], education_year_id=fea_dict['education_year_id'],
                  first_name=fea_dict['first_name'], gender=fea_dict['gender'],
                  hometown_id=fea_dict['hometown_id'], languages_id=fea_dict['languages_id'],
                  last_name=fea_dict['last_name'], locale=fea_dict['locale'], location_id=fea_dict['location_id'],
                  work_employer_id=fea_dict['work_employer_id'], work_end_date=fea_dict['work_end_date'],
                  work_location_id=fea_dict['work_location_id'], work_position_id=fea_dict['work_position_id'],
                  work_start_date=fea_dict['work_start_date'], work_with_id=fea_dict['work_with_id'])

    graph.create(CenterNode)

    for i in range(count2):
        relationship = Relationship(node_list[i], "Be_Friend_With", CenterNode)
        relationship["undirected"] = True
        graph.create(relationship)

main()
