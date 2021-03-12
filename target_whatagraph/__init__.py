#!/usr/bin/env python3
import os
import json
import argparse
import logging
import requests
import gluestick as gs

logger = logging.getLogger("target-whatagraph")
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_json(path):
    with open(path) as f:
        return json.load(f)


def parse_args():
    '''Parse standard command-line args.
    Parses the command-line arguments mentioned in the SPEC and the
    BEST_PRACTICES documents:
    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -p,--properties Properties file: DEPRECATED, please use --catalog instead
    --catalog       Catalog file
    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config',
        help='Config file',
        required=True)

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)

    return args


def load_data(config):
    # Read the passed CSV
    entities = gs.read_csv_folder(config['input_path'])
    return entities


def get_metadata(config, metatype, name):
    """
    Gets list of existing metrics/dimensions in whatagraph. Used to avoid recreating them.
    """
    access_token = config['access_token']

    response = requests.get(
        f"https://api.whatagraph.com/v1/integration-{metatype}",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
    )

    raw_data = response.json()
    data = raw_data.get("data", [])
    matching_meta = next((x for x in data if x["name"] == name), None)

    return matching_meta


def create_metric(config, metric_name):
    """
    Create metric in whatagraph
    """
    access_token = config['access_token']

    response = requests.post(
        f"https://api.whatagraph.com/v1/integration-metrics",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        data=json.dumps({
            "name": metric_name,
            "external_id": metric_name,
            "type": "int",
            "accumulator": "sum",
            "negative_ratio": False
        })
    )

    logger.debug(response.text)


def create_dimension(config, dimension_name):
    """
    Create dimension in whatagraph
    """
    access_token = config['access_token']

    response = requests.post(
        f"https://api.whatagraph.com/v1/integration-dimensions",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        data=json.dumps({
            "name": dimension_name,
            "external_id": dimension_name,
            "type": "string"
        })
    )

    logger.debug(response.text)


def post_data(config, data):
    """
    Posts all data from whatagraph: formats into JSON then pushes to API
    """
    for entity_name in data:
        entity = data[entity_name]
        types = entity.dtypes
        cols = entity.columns

        # Configure the meta
        for col in cols:
            col_type = types[col]
            logger.debug(f"Verifying metadata for {col} in whatagraph")

            if col_type == "int64":
                logger.debug(f"Checking metric {col}")
                meta = get_metadata(config, "metrics", col)

                if meta is None:
                    logger.debug(f"Metric does not exist yet. Creating...")
                    create_metric(config, col)
            elif col != "date":
                logger.debug(f"Checking dimension {col}")
                meta = get_metadata(config, "dimensions", col)

                if meta is None:
                    logger.debug(f"Dimension does not exist yet. Creating...")
                    create_dimension(config, col)


        # Format the data
        access_token = config['access_token']
        formatted_rows = []

        for index, row in entity.iterrows():
            entry = {}

            for col in cols:
                entry[col] = row[col]

            formatted_rows.append(entry)

        logger.debug(f"Exporting {json.dumps(formatted_rows)}")

        # Send to whatagraph
        response = requests.post(
            f"https://api.whatagraph.com/v1/integration-source-data",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json"
            },
            data=json.dumps({
                "data": formatted_rows
            })
        )

        logger.debug(response.text)
        logger.debug(f"Exported {entity_name}")


def purge_data(config):
    """
    Used for testing to purge old data points
    """
    access_token = config['access_token']

    response = requests.get(
        f"https://api.whatagraph.com/v1/integration-source-data",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }
    )

    raw_data = response.json()
    data = raw_data.get("data", [])

    for d in data:
        response = requests.delete(
            f"https://api.whatagraph.com/v1/integration-source-data/{d['id']}",
            headers={
                "Authorization": f"Bearer {access_token}"
            }
        )


def upload(config):
    """
    Uploads CSV data in input_path to whatagraph
    """
    # Load CSVs to post
    data = load_data(config)
    logger.debug(f"Exporting {list(data.keys())}...")

    # Post CSV data to Databox
    post_data(config, data)


def main():
    # Parse command line arguments
    args = parse_args()

    # Upload the 
    upload(args.config)


if __name__ == "__main__":
    main()
