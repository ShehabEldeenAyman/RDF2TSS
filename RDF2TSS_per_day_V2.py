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
    prefix_sosa = Namespace('http://www.w3.org/ns/sosa/')
    base_snippet_ns = Namespace("https://example.org/tss/snippet/")

    final_graph = Graph()
    final_graph.bind('tss', prefix_tss)
    final_graph.bind('ex', prefix_ex)
    final_graph.bind('sosa', prefix_sosa)

    print("Creating TSS graph...")

    # One SPARQL query per sensor (HUGE SPEEDUP)
    base_query = """
    PREFIX sosa: <http://www.w3.org/ns/sosa/>

    SELECT ?OBSERVATION ?TIME ?READING ?observedProperty
    WHERE {
        ?OBSERVATION a sosa:Observation ;
                     sosa:resultTime ?TIME ;
                     sosa:hasSimpleResult ?READING ;
                     sosa:observedProperty ?observedProperty ;
                     sosa:madeBySensor %s .
    }
    ORDER BY ?TIME
    """

    for sensor in sensor_set:
        sensor_token = sensor.n3()

        q = base_query % sensor_token
        results = list(graph.query(q))

        if not results:
            continue

        # Group by date in Python (FAST)
        grouped = defaultdict(list)
        for row in results:
            t = row.TIME.toPython()
            grouped[t.date()].append(row)

        # Identify sensor subject (URI or mint one)
        if isinstance(sensor, URIRef):
            sensor_uri = sensor
        else:
            safe_id = str(sensor).replace(" ", "_")
            sensor_uri = prefix_ex[f"sensor/{safe_id}"]

        # Build TSS blocks
        for date_key, rows in grouped.items():

            # Convert rows into JSON list
            tss_points = []
            for r in rows:
                tss_points.append({
                    "time": str(r.TIME),
                    "value": str(r.READING),
                    "id": str(r.OBSERVATION),
                    "observedProperty": str(r.observedProperty)
                })

            json_object = json.dumps(tss_points)

            # Create nodes
            #snippet = BNode() #this should be changed. 
            template = BNode()

############replacing blank node snippet############
            first_obs = str(rows[0].OBSERVATION)
            first_time = str(rows[0].TIME)
            # Sanitize time for URI use
            safe_time = first_time.replace(":", "").replace("-", "").replace("T", "").replace("Z", "")
            # Example: "2025-08-18T00:00:00" → "20250818000000"
            # Sanitize observation ID
            safe_obs = first_obs.replace(":", "%3A").replace("/", "%2F")
            # Final snippet URI
            snippet = URIRef(base_snippet_ns[f"{safe_id}_{safe_time}"])
########################
            # Add Snippet
            final_graph.add((snippet, RDF.type, prefix_tss.Snippet))
            final_graph.add((snippet, prefix_tss.points, Literal(json_object)))
            final_graph.add((snippet, prefix_tss["from"], Literal(str(rows[0].TIME), datatype=XSD.dateTime)))
            final_graph.add((snippet, prefix_tss.to, Literal(str(rows[-1].TIME), datatype=XSD.dateTime)))
            final_graph.add((snippet, prefix_tss.pointType, prefix_sosa.Observation))

            # Link to template
            final_graph.add((snippet, prefix_tss.about, template))
            final_graph.add((template, RDF.type, prefix_tss.PointTemplate))

            # Assign sensor
            if isinstance(sensor, URIRef):
                final_graph.add((template, prefix_sosa.madeBySensor, sensor))
            else:
                final_graph.add((template, prefix_sosa.madeBySensor, sensor_uri))

            # observedProperty (use first row)
            final_graph.add((template, prefix_sosa.observedProperty, rows[0].observedProperty))

    print("TSS graph created.")
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