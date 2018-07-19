python版本：2.7

外部依赖包：
py2neo
python-igraph
retrying
ConfigParser

配置文件：
db.conf配置数据库基本信息及cypher查询语句
注：cypher语句需返回节点对，才能将查询结果导入igraph对象

运行：
python main_process [-b] [-c] [-s] （服务器上用python2）
-b：（服务器上运行无需指定此参数）使用bolt方式连接数据库
-c：社区检测，社区编号写回图谱
-s：连通子图分解，子图编号写回图谱（图谱中子图已分好，无需指定此参数）