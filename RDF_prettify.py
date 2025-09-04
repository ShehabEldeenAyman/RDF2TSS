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
    SaveGraph(args.output,Original_graph)

if __name__ == "__main__":
    main()    