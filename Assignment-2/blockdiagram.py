from graphviz import Digraph
import json
dot = Digraph(comment='The Round Table')

jsonfile="outputpeer_adversary_interarrival:2_floodpercent:1_127.0.0.1:8003.json"

# dot.node('A', 'King Arthur')
# dot.node('B', 'Sir Bedevere the Wise')
# dot.node('L', 'Sir Lancelot the Brave')

# dot.edges(['AB', 'AL'])
# dot.edge('B', 'L', constraint='false')

with open(jsonfile) as json_file: 
            data = json.load(json_file)
            data=data.items()
            y=[]
            for x,v in data:
                key=str(x)
                # if v["previousHash"]!="null":
                inn=str(v["index"])+"\n"+key
                if v["index"]==1:
                    inn="genesis\n"+inn
                dot.node(key,inn)
                if v["previousHash"]=="null":
                    # dot.edge("genesis",key)
                    pass
                else:
                    dot.edge(v["previousHash"],key)
            # dot.edges(y)

dot.render(view=True)  # doctest: +SKIP
