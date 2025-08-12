from rdflib import Graph,URIRef,Namespace,BNode,Literal
from rdflib.namespace import XSD,RDF
import pandas as pd
import argparse
from collections import defaultdict
from datetime import datetime
import json

def LoadGraph(directory):
    graph = Graph()
    print("Started loading graph...")
    graph.parse(directory, format="turtle",publicID="https://example.org/")
    print("Graph loaded successfully.")
    return graph

def CreateSensorSet(graph):
    sensor_set = set()
    sensor_set.clear()
    get_sensor_query = ''' 
PREFIX sosa: <http://www.w3.org/ns/sosa/>

SELECT DISTINCT ?sensor
WHERE {
  ?s sosa:madeBySensor ?sensor .
}
'''
    print('Started identifying unique senors')
    for sensor in graph.query(get_sensor_query):
        #print(sensor.sensor)
        sensor_set.add(str(sensor[0]))
    
    print('Sensors identified successfully')
    return sensor_set

def CreateTSS(sensor_set,graph):
    prefix_tss = Namespace('https://w3id.org/tss#')
    prefix_ex  = Namespace('http://example.org/')
    prefix_sosa  = Namespace('http://www.w3.org/ns/sosa/')
    final_graph = Graph()
    final_graph.bind('tss',prefix_tss)
    final_graph.bind('ex',prefix_ex)
    final_graph.bind('sosa',prefix_sosa)
    print('Started creating final graph')
    for sensor in sensor_set:
            tss_points = []
            tss_query = f'''
            PREFIX sosa: <http://www.w3.org/ns/sosa/>

            SELECT ?READING ?TIME ?OBSERVATION ?observedProperty
            WHERE {{
                ?OBSERVATION a sosa:Observation ;
                sosa:resultTime ?TIME;
                sosa:hasSimpleResult ?READING;
                sosa:observedProperty ?observedProperty;
                sosa:madeBySensor <{sensor}>.

            }}

            ORDER BY ?TIME
            '''
            results = graph.query(tss_query)
            for row in results:
                #print(sensor, ' value:' ,row.READING, ' time:', row.TIME, ' ID:' ,row.OBSERVATION, ' Observed property: ',row.observedProperty)

                
                data = {
                    'time': row.TIME,
                    'value': row.READING,
                    'id': row.OBSERVATION,
                    #'observedProperty' :  row.observedProperty
                }
                
                tss_points.append(data)
            json_object = json.dumps(tss_points) #serialize json object to a string
            
            #Create new graph 
            subject  = prefix_ex[f"snippet/{str(tss_points[0]['time'])[:9]}"] #this is the proper subject and should replace "URIRef(sensor)"
            #point_template = BNode(f'${URIRef(sensor)}') #blank node for the PointTemplate. the sensor uri reference is added here to make sure that temporary node has unique id
            #print(point_template) #for testing 
            temporary_node = BNode() #temporary node is created for each sensor at a time to ensure its uniqueness.

            final_graph.add((URIRef(sensor),RDF.type,prefix_tss.Snippet)) #temp
            final_graph.add((URIRef(sensor),prefix_tss.points,Literal(json_object, datatype=RDF.JSON))) #the json array with time, value, id.
            final_graph.add((URIRef(sensor),prefix_tss["from"],tss_points[0]['time'])) #from is a reserved word, hence worked around it this way
            final_graph.add((URIRef(sensor),prefix_tss.to,tss_points[-1]['time'])) #from is a reserved word, hence worked around it this way
            final_graph.add((URIRef(sensor),prefix_tss.pointType,prefix_sosa.Observation)) 

            #tss context json object
            context_obj = {
            "@context": {
                "id": "@id",
                "time": {
                    "@id": "http://www.w3.org/ns/sosa/resultTime",
                    "@type": "http://www.w3.org/2001/XMLSchema#dateTime"
                },
                "value": {
                    "@id": "http://www.w3.org/ns/sosa/resultTime",
                    "@type": "http://www.w3.org/2001/XMLSchema#integer"
                }
            }
        }
            context_obj = json.dumps(context_obj) #serialize json object to a string
            #final_graph.add((URIRef(sensor),prefix_tss.context,Literal(context_obj, datatype=RDF.JSON))) #this part is static. It is decided to remove it from each instance. It will be added later on a higher level.
            
            #temporary node part
            final_graph.add((URIRef(sensor),prefix_tss.about,temporary_node))
            final_graph.add((temporary_node, RDF.type, prefix_tss.PointTemplate))
            final_graph.add((temporary_node,prefix_sosa.madeBySensor,URIRef(sensor)))
            final_graph.add((temporary_node,prefix_sosa.observedProperty,row.observedProperty))


            '''
            print(json_object)
            print('start time: ', tss_points[0]['time']) #since readings are already sorted, first object holds start time
            print('end time: ', tss_points[-1]['time']) #while last object always holds last time
            print('-----------------------------------------------------------------------------------------------------------------------')
            '''
    print('Graph created successfully')
    return final_graph

def SaveGraph(directory,final_graph):
    print('Started writing file to disk')
    final_graph.serialize(destination=directory, format="turtle")
    print('File written successfully')

def main():
    parser = argparse.ArgumentParser(description='Process sensor graph files.')
    parser.add_argument('-i', '--input', required=True, help='Input Turtle file path')
    parser.add_argument('-o', '--output', required=True, help='Output Turtle file path')
    args = parser.parse_args()

    print("Program started!")
    Original_graph  = LoadGraph(args.input)
    Sensor_set = CreateSensorSet(Original_graph)
    Final_graph = CreateTSS(Sensor_set,Original_graph)
    SaveGraph(args.output,Final_graph)

    
if __name__ == "__main__":
    main()