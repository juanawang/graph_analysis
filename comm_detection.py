#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from py2neo import Graph as pyGraph
from igraph import Graph as IGraph
from retrying import retry
from config import logger
import ConfigParser

def read_conf(conf_file):
    cf = ConfigParser.ConfigParser()
    cf.read(conf_file)
    
    db_conf = dict()
    db_conf["uri"] = cf.get("db","db_uri")
    db_conf["user"] = cf.get("db","db_user")
    db_conf["password"] = cf.get("db","db_password")
    db_conf["cypher"] = cf.get("cypher",'cypher_statement')
    
    label_index=dict()
    label_index[':User'] = cf.get("db_indexes","User")
    label_index[':Device'] = cf.get("db_indexes","Device")
    label_index[':Mobile'] = cf.get("db_indexes","Mobile")
    label_index[':Idcard'] = cf.get("db_indexes","Idcard")
    label_index[':Bankcard'] = cf.get("db_indexes","Bankcard")
    
    return db_conf,label_index

@retry(stop_max_attempt_number=2)
def get_graph(user,pwd,bolt=False,**kwarg):
    if not bolt:
        graph=pyGraph(auth=(user,pwd))
    else:
        if kwarg.get('uri',None) is None:
            logger.error('missing parameter uri')
            return None
        graph=pyGraph(kwarg['uri'], bolt=True,auth=(user, pwd))
    return graph

@retry(stop_max_attempt_number=2)
def fetch_data(cypher,graph):
    if graph is not None:
        return graph.run(cypher)

def get_db_max_commid(graph):
    if graph is not None:
        max_commid = graph.run("match (u) return max(u.community) as maxcid").data()[0]['maxcid']
        return -1 if max_commid is None else max_commid

def get_db_max_sgid(graph):
    if graph is not None:
        max_sgid = graph.run("match (u) return max(u.subgraph_id) as maxsgid").data()[0]['maxsgid']
        return -1 if max_sgid is None else max_sgid

def decompose_graph(ig):
    return ig.clusters().subgraphs()

# 对subgraph应用社区发现算法，组装包含community信息的nodes
def compose_sub_nodes_comm(subgraph,label_index,comm_num):
    communities = subgraph.community_multilevel()
    membership = communities.membership
    nodes = [{"index":label_index[str(node.labels)],label_index[str(node.labels)]:node[label_index[str(node.labels)]]} for node in subgraph.vs["name"]]
    for node in nodes:
        idx = next((index for (index, d) in enumerate(subgraph.vs['name']) if d[node['index']] == node[node['index']]), None)
        node["community"] = membership[idx]+comm_num
    return nodes, len(communities)
        
def compose_nodes_comm(ig,label_index,comm_num):
    nodes = []
    for sg in decompose_graph(ig):
        sub_nodes,sub_comm_num = compose_sub_nodes_comm(sg,label_index,comm_num)
        nodes.extend(sub_nodes)
        comm_num = comm_num + sub_comm_num
    nodes_user = filter(lambda x:x['index']=='uid', nodes)
    nodes_device = filter(lambda x:x['index']=='mac_code', nodes)
    nodes_mobile = filter(lambda x:x['index']=='mobile', nodes)
    nodes_idcard = filter(lambda x:x['index']=='identityid', nodes)
    nodes_bankcard = filter(lambda x:x['index']=='cardno', nodes)
    return nodes_user,nodes_device,nodes_mobile,nodes_idcard,nodes_bankcard

# 对subgraph中的节点，组装包含subgraph_id信息的nodes
def compose_sub_nodes_sg(subgraph,label_index,sg_num):
    sub_nodes = [{"index":label_index[str(node.labels)],label_index[str(node.labels)]:node[label_index[str(node.labels)]]} for node in subgraph.vs["name"]]
    for node in sub_nodes:
        node["subgraph_id"] = sg_num
    return sub_nodes

def compose_nodes_sg(ig,label_index,sg_num):
    nodes = []
    for sg in decompose_graph(ig):
        sub_nodes = compose_sub_nodes_sg(sg, label_index, sg_num)
        nodes.extend(sub_nodes)
        sg_num = sg_num + 1
    nodes_user = filter(lambda x:x['index']=='uid', nodes)
    nodes_device = filter(lambda x:x['index']=='mac_code', nodes)
    nodes_mobile = filter(lambda x:x['index']=='mobile', nodes)
    nodes_idcard = filter(lambda x:x['index']=='identityid', nodes)
    nodes_bankcard = filter(lambda x:x['index']=='cardno', nodes)
    return nodes_user,nodes_device,nodes_mobile,nodes_idcard,nodes_bankcard
       
#信息写回图谱
@retry(stop_max_attempt_number=2)
def writeback_prop(graph,property_name,**kwarg):
    if not isinstance(property_name,str):
        logger.error("invalid parameter: 'property_name' should be str")
        return
        
    write_query_users = "UNWIND {nodes} AS n MATCH (c:User) WHERE c.uid = n.uid set c." + property_name + " = n." + property_name

    write_query_devices = "UNWIND {nodes} AS n MATCH (c:Device) WHERE c.mac_code = n.mac_code set c." + property_name + " = n." + property_name

    write_query_mobiles = "UNWIND {nodes} AS n MATCH (c:Mobile) WHERE c.mobile = n.mobile set c." + property_name + " = n." + property_name

    write_query_idcards = "UNWIND {nodes} AS n MATCH (c:Idcard) WHERE c.identityid = n.identityid set c." + property_name + " = n." + property_name

    write_query_bankcards = "UNWIND {nodes} AS n MATCH (c:Bankcard) WHERE c.cardno = n.cardno set c." + property_name + " = n." + property_name
    
    logger.info("writing user {}...".format(property_name))
    graph.run(write_query_users, nodes=kwarg.get('nodes_user'))

    logger.info("writing device {}...".format(property_name))
    graph.run(write_query_devices, nodes=kwarg.get('nodes_device'))

    logger.info("writing mobile {}...".format(property_name))
    graph.run(write_query_mobiles, nodes=kwarg.get('nodes_mobile'))

    logger.info("writing idcard {}...".format(property_name))
    graph.run(write_query_idcards, nodes=kwarg.get('nodes_idcard'))

    logger.info("writing bankcard {}...".format(property_name))
    graph.run(write_query_bankcards, nodes=kwarg.get('nodes_bankcard'))
    
def comm_detect_and_writeback(ig,graph,label_index):
    comm_num = get_db_max_commid(graph) + 1
    logger.info('composing nodes(community info)...')
    nodes_user,nodes_device,nodes_mobile,nodes_idcard,nodes_bankcard = compose_nodes_comm(ig,label_index,comm_num)
    writeback_prop(graph,"community",nodes_user=nodes_user,nodes_device=nodes_device,nodes_idcard=nodes_idcard,nodes_bankcard=nodes_bankcard)

def sg_decompose_and_writeback(ig,graph,label_index):
    sg_num = get_db_max_sgid(graph) + 1
    logger.info('composing nodes(subgraph info)...')
    nodes_user,nodes_device,nodes_mobile,nodes_idcard,nodes_bankcard = compose_nodes_sg(ig,label_index,sg_num)
    writeback_prop(graph,"subgraph_id",nodes_user=nodes_user,nodes_mobile=nodes_mobile,nodes_device=nodes_device,nodes_idcard=nodes_idcard,nodes_bankcard=nodes_bankcard)
    
