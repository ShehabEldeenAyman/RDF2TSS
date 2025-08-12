Certainly! Here's a concise README documentation for your script explaining how it transforms RDF data into time series snippets:

---

# RDF to Time Series Snippets Converter

This Python script processes RDF sensor observation data and transforms it into time series snippets in RDF format. It is designed to work with sensor data modeled using the [SOS](https://www.w3.org/TR/vocab-ssn/) (Sensor, Observation, Sample, and Actuator) ontology and produces a compact representation suitable for time series analysis.

## Overview

The script performs the following steps:

1. **Load RDF Graph**
   Loads an input RDF graph from a Turtle file containing sensor observations and related metadata.

2. **Identify Unique Sensors**
   Extracts all distinct sensors (`sosa:madeBySensor`) referenced in the observations.

3. **Extract Time Series Data per Sensor**
   For each sensor, it queries all observations, retrieving:

   * Observation time (`sosa:resultTime`)
   * Observed value (`sosa:hasSimpleResult`)
   * Observation ID
   * Observed property

   These are collected and ordered by observation time to form the time series data points.

4. **Create Time Series Snippet Graph**
   For each sensor, a new RDF graph snippet is created containing:

   * The sensor as a `tss:Snippet` instance
   * A JSON array of observation data points serialized as a literal (`tss:points`)
   * Start and end timestamps of the time series (`tss:from` and `tss:to`)
   * A template node (`tss:PointTemplate`) describing the structure of the points, linking to the sensor and observed property

5. **Save Final RDF Graph**
   The resulting graph with time series snippets for all sensors is serialized back to Turtle format for downstream use.

## Usage

Run the script via command line:

```bash
python RDF2TSS.py -i sample.ttl -o output_tss.ttl
```

* `-i / --input`: Path to the input Turtle RDF file containing sensor observations.
* `-o / --output`: Path where the transformed time series snippet RDF Turtle file will be saved.

## Output

The output RDF graph contains:

* `tss:Snippet` instances for each sensor
* JSON serialized observation points with timestamps and values
* Metadata linking observations to sensors and observed properties
* Time bounds of the snippet (`tss:from`, `tss:to`)

## Dependencies

* Python 3.x
* `rdflib`
* `pandas` (imported but currently not used in the script)

## Notes

* The script assumes input RDF uses the SOSA ontology for observations.
* The JSON-LD context for the time series points is prepared but currently commented out to be added later at a higher level.
* The `tss` namespace refers to a custom vocabulary for time series snippets.


