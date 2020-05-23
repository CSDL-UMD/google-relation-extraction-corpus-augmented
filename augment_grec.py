#!/usr/bin/python3

'''
Script to Augment the Raw Google Relation Extraction Corpus (https://ai.googleblog.com/2013/04/50000-lessons-on-how-to-read-relation.html)
'''

import json
import os
from urllib import parse, request, error
import random
import string
import unicodedata
import argparse
import datetime
import logging
import requests
import xml.etree.ElementTree as ET


# Argument parser
def arg_parse(arg_list=None):
    now = datetime.datetime.now().strftime("%b-%d-%y")
    parser = argparse.ArgumentParser(
        description="Augment Google Relation Corpus")
    # .ndjson -> json
    parser.add_argument(
        '--jsonify',
            '-js',
        dest='jsonify',
        help='Fix JSON formatting of raw GREC corpus',
        action='store_true',
        default=False
    )
    # Convert Unicode to Ascii
    parser.add_argument(
        '--ascii',
        '-a',
        dest='uni_to_ascii',
        help='Convert Unicode in Snippets to ASCII',
        action='store_true',
        default=False
    )
    # Add UIDs
    parser.add_argument(
        '--unique-ids',
        '-uid',
        dest='uid',
        help='Add Unique Identifiers',
        action='store_true',
        default=False
    )
    # Add Majority Vote
    parser.add_argument(
        '--majority-vote',
        '-mv',
        dest='majority_vote',
        help='Add a Majority Vote Entry',
        action='store_true',
        default=False
    )
    # Pull Google KG Entries
    parser.add_argument(
        '--google-kg',
        '-gkg',
        dest='google_kg',
        help='Replace Freebase IDs for subject and object with string from Google KG API ',
        action='store_true',
        default=False
    )
    # Pull DBPedia References
    parser.add_argument(
        '--dbpedia',
        '-db',
        dest='dbpedia',
        help='Find DBpedia reference URIs',
        action='store_true',
        default=False
    )
    # Source Directory
    parser.add_argument(
        '--in-dir',
        '-id',
        dest='source_dir',
        help='Input Directory Path, default ./',
        type=str,
        default='./'
    )
    # Save Directory
    parser.add_argument(
        '--out-dir',
        '-od',
        dest='save_dir',
        help='Output Directory Path, default ./',
        type=str,
        default='./'
    )
    # Save Filename Tag
    parser.add_argument(
        '--output',
        '-o',
        dest='output_tag',
        help='Tag to append to Output Files, default <filename>-augment_MM-DD-YY.json',
        type=str,
        default='-augment' + "_" + now
    )
    # Parses and returns args
    if arg_list:
        return parser.parse_args(args=arg_list)
    else:
        return parser.parse_args()

# Google KG API Extraction

def get_entity(id, path_to_key, subject=None):
    api_key = open(path_to_key).read()
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
    params = {
        'ids': id,
        'key': api_key
    }
    url = service_url + '?' + parse.urlencode(params)

    try:
        response = json.loads(request.urlopen(url).read())
        print(response)
        return response['itemListElement'][0]['result']['name']
    except:
        logging.error("Freebase ID not found in Google Knowledge Graph API")
        if subject is not None:
            logging.info('Using Wikipedia evidence URL for subject extraction')
            try:
                return ' '.join(subject.split('/')[-1].split('_')).replace('.', '').replace(',', '')
            except:
                logging.error("Wikipedia URL extraction failed.")
                raise
        else:
            raise

def get_json(json_file, json_bool):
    """Returns contents of json_file as json object

    Arguments:
        json_file {python file object} -- A file object containing a json
        json_bool {Boolean} -- Fixes .ndjson formatting
    """
    if json_bool:
        return json.loads("[" + json_file.read().replace("}\n{", "}, {").replace('\\', '') + "]")
    else:
        return json.loads(json_file.read())


def get_relation_type(json_filename):
    if "education" in json_filename:
        return "education"
    elif "institution" in json_filename:
        return "institution"
    elif "date_of_birth" in json_filename or 'dob' in json_filename:
        return "dob"
    elif "place_of_birth" in json_filename or 'pob' in json_filename:
        return "pob"
    elif "place_of_death" in json_filename or 'pod' in json_filename:
        return "pod"
    else:
        return "Error"


def generate_id(relation, size=10):
    """Generate random id

    Arguments:
        relation {str} -- relation tag (i.e education, pob, etc)
        size {int} -- Size of random ID
    """
    hash = ''.join([random.choice(string.ascii_letters + string.digits)
                   for n in range(size)])
    relation_tag = relation if len(relation) < 4 else relation[0]
    return relation_tag + '_' + hash  # i.e pob_3jf8jnd8


def tally_votes(relation):
    """Finds majority vote of raters in GRC

    Arguments:
        relation {json_object} -- Object representing single relation sample
    """
    yes_votes = 0
    no_votes = 0
    skip_votes = 0
    for vote in relation['judgments']:
        if vote['judgment'] == 'yes':
            yes_votes += 1
        if vote['judgment'] == 'no':
            no_votes += 1
        if vote['judgment'] == 'skip':
            skip_votes += 1
    if (yes_votes >= no_votes) and (yes_votes >= skip_votes):
        return 'yes'
    elif (no_votes >= yes_votes) and (no_votes >= skip_votes):
        return 'no'
    else:
        return 'skip'

def find_dbpedia(entity):
    try:
        response = requests.get(f"http://lookup.dbpedia.org/api/search.asmx/KeywordSearch?QueryString='{entity}'")
        parsed = ET.fromstring(response.content)
        return parsed[0][1].text
    except:
        raise

def main():

    ### Initialize Logger ###
    logging.basicConfig(
        format="%(asctime)s;%(levelname)s;%(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO
    )

    #########################

    ### Args ###
    args = arg_parse()
    jsonify = args.jsonify
    uid = args.uid
    majority_vote = args.majority_vote
    source_dir = args.source_dir
    save_dir = args.save_dir
    uni_to_ascii = args.uni_to_ascii
    dbpedia = args.dbpedia
    output_tag = args.output_tag

    google_kg = args.google_kg
    google_api_key = '../google_api'  # Path to Google API Key

    ############

    logging.info("Beginning Google Relation Extraction Corpus Augmentation")

    if not os.path.exists(source_dir):
        logging.error(source_dir + " not found.")
        return

    # Generate list of paths to all .json files in <source_dir>
    json_files = [str(source_dir + x) for x in os.listdir(source_dir) if ".json" in x ]

    # Iterate through every file
    for j_file in json_files:
        with open(j_file) as f:

            logging.info("Processing " + j_file)

            relations = get_json(f, jsonify)
            relation_type = get_relation_type(j_file)

            # Iterate through all relations in file, applying selected modification
            for relation in relations:
                if uid:
                    relation['UID'] = generate_id(relation_type)
                if uni_to_ascii:
                    relation['evidences'][0]['snippet'] = unicodedata.normalize('NFKD', relation['evidences'][0]['snippet']).encode('ascii', 'ignore').decode('ascii')
                if majority_vote:
                    relation['maj_vote'] = tally_votes(relation)
                if google_kg:
                    evidence = relation['evidences'][0]['url']
                    relation['sub_id'] = relation['sub']
                    relation['obj_id'] = relation['obj']
                    try:
                        relation['sub'] = get_entity(
                            relation['sub_id'], google_api_key, subject=evidence)
                    except:
                        logging.error(relation['UID'] + \
                                      "failed to fetch subject")
                        relation['sub'] = "needs_entry"
                    if relation_type is not 'dob':
                        try:
                            relation['obj'] = get_entity(
                                relation['obj_id'], google_api_key)
                        except:
                            logging.error(
                                relation['UID'] + "failed to fetch object")
                            relation['obj'] = "needs_entry"
                if dbpedia:
                    try:
                        relation['dbpedia_sub'] = find_dbpedia(relation['sub'])
                    except:
                        pass
                    try:
                        relation['dbpedia_obj'] = find_dbpedia(relation['obj'])
                    except:
                        pass
            out_filename = relation_type + output_tag + '.json'
            out_filepath = save_dir + out_filename

            if not os.path.exists(save_dir):
                os.makedirs(save_dir)
            with open(out_filepath, 'w+') as out:
                json.dump(relations, out, indent=2)

            logging.info("Finished processing " + j_file)

    logging.info("Finished Augmenting Corpus")


if __name__ == '__main__':
    main()
