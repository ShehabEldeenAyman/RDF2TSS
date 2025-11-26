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
    get_sensor_query = ''' 
PREFIX sosa: <http://www.w3.org/ns/sosa/>

SELECT DISTINCT ?sensor
WHERE {
  ?s sosa:madeBySensor ?sensor .
}
'''
    print('Started identifying unique sensors')
    # store actual RDF terms (URIRef or Literal), not stringified values
    for sensor in graph.query(get_sensor_query):
        sensor_term = sensor[0]   # this is an rdflib term (URIRef or Literal)
        sensor_set.add(sensor_term)

    print('Sensors identified successfully')
    return sensor_set



def CreateTSS(sensor_set, graph):
    prefix_tss = Namespace('https://w3id.org/tss#')
    prefix_ex  = Namespace('http://example.org/')
    prefix_sosa  = Namespace('http://www.w3.org/ns/sosa/')
    final_graph = Graph()
    final_graph.bind('tss', prefix_tss)
    final_graph.bind('ex', prefix_ex)
    final_graph.bind('sosa', prefix_sosa)
    print('Started creating final graph')

    for sensor in sensor_set:
        # use sensor.n3() so SPARQL receives a proper token: <uri> or "literal"^^xsd:...
        sensor_token = sensor.n3() if hasattr(sensor, 'n3') else f"<{str(sensor)}>"

        
        tss_query = f'''
        PREFIX sosa: <http://www.w3.org/ns/sosa/>

        SELECT ?READING ?TIME ?OBSERVATION ?observedProperty
        WHERE {{
            ?OBSERVATION a sosa:Observation ;
                         sosa:resultTime ?TIME ;
                         sosa:hasSimpleResult ?READING ;
                         sosa:observedProperty ?observedProperty ;
                         sosa:madeBySensor {sensor_token} .
        }}
        ORDER BY ?TIME
        '''
        results = graph.query(tss_query)

        date_time_points = set ()


        for row in results:
            #this is where we will add our new code
            #dt_temp = row.TIME.toPython()
            date_only_temp = row.TIME.toPython().date()
            date_time_points.add(date_only_temp)

        print("Unique dates identified:", len(date_time_points))
        for dtp in date_time_points: 

            sensor_byDate_query = f'''
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX sosa: <http://www.w3.org/ns/sosa/>

            SELECT ?OBSERVATION ?TIME ?READING ?observedProperty
            WHERE {{
                ?OBSERVATION sosa:resultTime ?TIME ;
                             sosa:hasSimpleResult ?READING ;
                             sosa:observedProperty ?observedProperty ;
                             sosa:madeBySensor {sensor_token} .
                
                 FILTER( STRSTARTS(STR(?TIME), "{dtp}") )
            }}
            ORDER BY ?TIME
            '''
            sensor_byDate_results = graph.query(sensor_byDate_query)
            print("Number of rows returned:", len(list(sensor_byDate_results)))

            tss_points = []
            for R in sensor_byDate_results:
            # each row values are rdflib terms; keep them
                data = {
                    'time': str(R.TIME),
                    'value': str(R.READING),
                    'id': str(R.OBSERVATION),
                    'observedProperty': str(R.observedProperty)
                }
               
                tss_points.append(data)
            json_object = json.dumps(tss_points)

            if isinstance(sensor, URIRef):
                subject = sensor
            else:
            # for a Literal sensor (e.g. "24002042"), create a stable URI in ex: namespace
                safe_id = str(sensor).replace(' ', '_')
                subject = prefix_ex[f"sensor/{safe_id}"]

                temporary_node = BNode()
                temp_subj_node = BNode()
                

                final_graph.add((temp_subj_node, RDF.type, prefix_tss.Snippet))
                # store the JSON timeseries as a literal (you had RDF.JSON before; keep as plain literal or use a string)
                final_graph.add((temp_subj_node, prefix_tss.points, Literal(json_object)))
                # Add from / to times using the original RDF time Literals
                final_graph.add((temp_subj_node, prefix_tss["from"], Literal(tss_points[0]['time'],datatype=XSD.dateTime)))
                final_graph.add((temp_subj_node, prefix_tss.to, Literal(tss_points[-1]['time'],datatype=XSD.dateTime)))
                final_graph.add((temp_subj_node, prefix_tss.pointType, prefix_sosa.Observation))
                final_graph.add((temp_subj_node, prefix_tss.about, temporary_node))
                final_graph.add((temporary_node, RDF.type, prefix_tss.PointTemplate))
                # madeBySensor should point to original sensor term if it was a URI, otherwise to an identifier URI
                if isinstance(sensor, URIRef):
                    final_graph.add((temporary_node, prefix_sosa.madeBySensor, sensor))
                else:
                    # point to an identifier URI we just created
                    final_graph.add((temporary_node, prefix_sosa.madeBySensor, subject))
                # use observedProperty from the first point (assumes same observedProperty for sensor)
                final_graph.add((temporary_node, prefix_sosa.observedProperty, row.observedProperty))

        # if not tss_points:
        #     print(f'Warning: no observations found for sensor {sensor_token}; skipping.')
        #     continue  # avoid indexing into empty list

        # json_object = json.dumps([
        #     {
        #         'time': str(p['time']),    # convert to string for JSON
        #         'value': str(p['value']),
        #         'id': str(p['id'])
        #     }
        #     for p in tss_points
        # ])

        # # choose an output subject: use the real URI if sensor is URIRef,
        # # otherwise mint an example URI for that sensor value
        # if isinstance(sensor, URIRef):
        #     subject = sensor
        # else:
        #     # for a Literal sensor (e.g. "24002042"), create a stable URI in ex: namespace
        #     safe_id = str(sensor).replace(' ', '_')
        #     subject = prefix_ex[f"sensor/{safe_id}"]

        # temporary_node = BNode()

        # final_graph.add((subject, RDF.type, prefix_tss.Snippet))
        # # store the JSON timeseries as a literal (you had RDF.JSON before; keep as plain literal or use a string)
        # final_graph.add((subject, prefix_tss.points, Literal(json_object)))
        # # Add from / to times using the original RDF time Literals
        # final_graph.add((subject, prefix_tss["from"], tss_points[0]['time']))
        # final_graph.add((subject, prefix_tss.to, tss_points[-1]['time']))
        # final_graph.add((subject, prefix_tss.pointType, prefix_sosa.Observation))

        # # tss context omitted as in your original comment

        # final_graph.add((subject, prefix_tss.about, temporary_node))
        # final_graph.add((temporary_node, RDF.type, prefix_tss.PointTemplate))
        # # madeBySensor should point to original sensor term if it was a URI, otherwise to an identifier URI
        # if isinstance(sensor, URIRef):
        #     final_graph.add((temporary_node, prefix_sosa.madeBySensor, sensor))
        # else:
        #     # point to an identifier URI we just created
        #     final_graph.add((temporary_node, prefix_sosa.madeBySensor, subject))
        # # use observedProperty from the first point (assumes same observedProperty for sensor)
        # final_graph.add((temporary_node, prefix_sosa.observedProperty, tss_points[0]['observedProperty']))

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