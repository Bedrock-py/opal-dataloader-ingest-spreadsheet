#****************************************************************
#
# Copyright (c) 2015, Georgia Tech Research Institute
# All rights reserved.
#
# This unpublished material is the property of the Georgia Tech
# Research Institute and is protected under copyright law.
# The methods and techniques described herein are considered
# trade secrets and/or confidential. Reproduction or distribution,
# in whole or in part, is forbidden except by the express written
# permission of the Georgia Tech Research Institute.
#****************************************************************/

import csv
import pandas as pd
import os

# add a new field to the maps input (used for csv loading)
def add_field(maps, key, values, type):
    #if data is of string type, convert to ids
    if type == 'String':
        maps[key] = {}
        maps[key]['indexToLabel'] = list(set(values))
        maps[key]['values'] = [maps[key]['indexToLabel'].index(x) + 1 for x in values]
    # just add the values directly do the array
    else:
        maps[key] = [str(x) for x in values]

# update field to the maps input (used for csv loading)
def update_field(maps, key, values, type, storepath):
    if type == 'String':
        with open(storepath + key + '.txt') as features:
            fieldvalues = features.read().split("\n")
            fieldvalues.pop()
        maps[key] = {}
        maps[key]['indexToLabel'] = list(set(values))
        for value in maps[key]['indexToLabel']:
            if value in fieldvalues:
                maps[key]['indexToLabel'].remove(value)
        fieldvalues.extend(maps[key]['indexToLabel'])
        maps[key]['values'] = [fieldvalues.index(x) + 1 for x in values]
    # just add the values directly to the array
    else:
        maps[key] = [str(x) for x in values]

# any fields that were added in the filtering processes are added to maps
def update_additions(maps, additions, matrixFeatures, storepath):
    if len(additions) > 0:
        for addition in additions:
            updateField(maps, addition['key'], addition['values'], addition['type'], storepath)
            matrixFeatures.append(addition['key'])

# any fields that were added in the filtering processes are added to maps
def process_additions(maps, additions, matrixFeatures):
    if len(additions) > 0:
        for addition in additions:
            addField(maps, addition['key'], addition['values'], addition['type'])
            matrixFeatures.append(addition['key'])

# write the data at the specified roopath with the specified filename
def write_output(rootpath, fileName, outputData):
    with open(rootpath + fileName + '.txt', 'w') as featuresFile:
        for element in outputData:
            featuresFile.write(str(element) + '\n')

# write the data at the specified roopath with the specified filename
def append_output(rootpath, fileName, outputData):
    with open(rootpath + fileName + '.txt', 'a') as featuresFile:
        for element in outputData:
            featuresFile.write(str(element) + '\n')

# use pandas to load the csv file into the dataframe, using a header if appropriate
def load_matrix(filepath):
    with open(filepath, 'rbU') as csvfile:
        snippet = csvfile.read(2048)
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(snippet)
    try:
        has_header = sniffer.has_header(snippet)
    except CParserError:
        has_header = True

    if has_header:
        df = pd.read_csv(filepath, error_bad_lines=False, header=None, skiprows=1)
        df.columns = get_header(filepath, '')
    else:
        df = pd.read_csv(filepath,header=None, error_bad_lines=False)
    return df

def get_header(filepath, dialect):
    sniffer = csv.Sniffer()
    with open(filepath, 'rbU') as csvfile:
        snippet = csvfile.read(2048)
        if dialect == '':
            dialect = sniffer.sniff(snippet)
        csvfile.seek(0)
        reader = csv.reader(csvfile, dialect)
        examples_lines = []
        for i,line in enumerate(reader):
            if i < 2:
                examples_lines.append(line)
            else:
                break
        # get the header
        if sniffer.has_header(snippet):
            header = examples_lines.pop(0)
        else:
            header = [str(x) for x in range(0,len(examples_lines[0]))]
    return header

# write the output files associated with each loaded file
# matrix.csv, features.txt, features_original.txt, and any non-numeric fields' mappings
def write_files(maps, matrixFeatures, matrixFeaturesOriginal, rootpath, return_data=False):
    # make directory
    if not os.path.exists(rootpath):
        os.makedirs(rootpath, 0775)
    toWrite = []
    # list of features to write to the output features.txt file
    features = []
    featuresOrig = []
    outputs = []
    # determine if a feature is numeric or has a label mapping
    for i, each in enumerate(matrixFeatures):
        if isinstance(maps[each], list):
            toWrite.append(maps[each])
        # since the feature has a label mapping, write out the label values in order for later reference
        # filename is the feature name + .txt
        else:
            try:
                toWrite.append([str(x) for x in maps[each]['values']])
            except KeyError:
                pass # ignore, since this is the mongoids field and has no values
            # write the features to output
            outputs.append(each + '.txt')
            write_output(rootpath, each, maps[each]['indexToLabel'])
        if matrixFeaturesOriginal[i] != '_id': # don't do this for mongoids
            features.append(each)
            featuresOrig.append(matrixFeaturesOriginal[i])
    # write out the list of features
    outputs.extend(['features_original.txt', 'features.txt'])
    write_output(rootpath, 'features_original', featuresOrig)
    write_output(rootpath, 'features', features)
    toReturn = []
    # convert lists to numpy arrays
    # matrix is documents x features (i.e. rows = individual items and columns = features)
    with open(rootpath + '/' + 'matrix.csv', 'w') as matrix:        
        for i in range(len(toWrite[0])):
            temp = []
            for each in toWrite:
                temp.append(each[i])
                if return_data:
                    toReturn.append(temp)
            matrix.write(','.join(temp) + '\n')
    outputs.append('matrix.csv')
    if return_data:
        return toReturn
    else:
        return outputs

def update_files(maps, matrixFeatures, matrixFeaturesOriginal, rootpath, return_data=False):
    # write output files
    toWrite = []
    # determine if a feature is numeric or has a label mapping
    for i, each in enumerate(matrixFeatures):
    # for each in maps.keys():
        if isinstance(maps[each], list):
            toWrite.append(maps[each])
        # since the feature has a label mapping, write out the label values in order for later reference
        # filename is the feature name + .txt
        else:
            try:
                toWrite.append([str(x) for x in maps[each]['values']])
            except KeyError:
                pass # ignore, since this is the mongoids field and has no values
            # write the features to output
            appendOutput(rootpath, each, maps[each]['indexToLabel'])
    toReturn = []
    # convert lists to numpy arrays
    # matrix is documents x features (i.e. rows = individual items and columns = features)
    with open(rootpath + '/' + 'matrix.csv', 'a') as matrix:        
        for i in range(len(toWrite[0])):
            temp = []
            for each in toWrite:
                temp.append(each[i])
            matrix.write(','.join(temp) + '\n')
            if return_data:
                toReturn.append(temp)
    if return_data:
        return toReturn
