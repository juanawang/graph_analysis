from config import logger
from comm_detection import *
import sys, getopt

def print_usage():
    print '''main_process.py [-b] [-c] [-s]
                -b: using bolt connection
                -c: community detection
                -s: subgraph decomposition'''
                
def main(argv):
    bolt = False
    comm = False
    sg = False
    try:
        opts,args = getopt.getopt(argv, "hbcs")
    except getopt.GetoptError:
        print_usage()
        sys.exit(-1)
        
    for opt,arg in opts:
        if opt == "-h":
            print_usage()
            sys.exit()
        elif opt == "-b":
            bolt = True
        elif opt == "-c":
            comm = True
        elif opt == "-s":
            sg = True
    
    if not comm and not sg:
        print_usage()
        sys.exit()

    db_conf,label_index = read_conf("conf/db.conf")
    if bolt:
        graph= get_graph(db_conf["user"],db_conf["password"],bolt=bolt,uri=db_conf["uri"])
    else:
        graph= get_graph(db_conf["user"],db_conf["password"])
    
    logger.info("fetching data...")
    result=fetch_data(db_conf["cypher"],graph)
    ig = IGraph.TupleList(result)

    if comm:
        comm_detect_and_writeback(ig,graph,label_index)
    if sg:
        sg_decompose_and_writeback(ig,graph,label_index)
    
if __name__=='__main__':
    main(sys.argv[1:])