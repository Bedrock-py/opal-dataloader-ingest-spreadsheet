#****************************************************************
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

from bedrock.dataloader.utils import *
from spreadsheet_utils import *
import time
import csv
import xlrd
import re
import os
import traceback
import pandas as pd
from collections import Counter
from itertools import islice

class Spreadsheet(Ingest):
    def __init__(self):
        super(Spreadsheet, self).__init__()
        self.name = 'CSV/Microsoft Excel'
        self.description = 'Loads data from CSV or Microsft Excel spreadsheets.'
        self.parameters_spec = [{ "name" : "file", "value" : ".csv,.xls,.xlsx", "type" : "file" }]
        self.NUM_EXAMPLES = 10
        self.NUM_SAMPLES = 100000
        self.NUM_UNIQUE = 10


    def explore(self, filepath):
        # check to see if csv or xls[x]
        files = os.listdir(filepath)
        if len(files) > 1: # means we have generated csv files from xls[x]
            for i,filename in enumerate(files):
                if 'xls' in filename: # find the xls[x] file
                    target = files[i]
                    break
        else:
            target = files[0]
        targetpath = filepath + '/' + target
        if 'csv' in targetpath:
            filename = re.split('/', targetpath)[-1]
            name = filename[:-4]
            try:
                schema = self.get_CSV_schema(targetpath)
            except:
                raise
                return 'Malformed CSV file.', 406
            else:
                return {name:schema}, 200
        elif 'xls' in targetpath:
            schemas = {}
            book = xlrd.open_workbook(targetpath)
            sheets = book.sheet_names()
            for sheetname in sheets:
                if sheetname + '.csv' not in os.listdir(filepath):
                    sheet = book.sheet_by_name(sheetname)
                    num_rows = sheet.nrows - 1
                    curr_row = -1
                    targetpath = os.path.join(filepath, sheetname + '.csv')
                    print "Reading XLS with %d rows and %d cols." % (sheet.nrows, sheet.ncols)
                    try:
                        with open(targetpath, 'wb') as csv_file:
                            csv_writer = csv.writer(csv_file)
                            while curr_row < num_rows:
                                curr_row += 1
                                csv_writer.writerow([self.format_excel_cell(sheet.cell_value(curr_row, col),sheet.cell_type(curr_row,col),book.datemode) for col in range(sheet.ncols)])
                    except:
                        print "Unable to convert %s to valid CSV." % (sheetname)
            for sheetname in sheets:
                if sheetname + '.csv' in os.listdir(filepath):
                    try:
                        targetpath = os.path.join(filepath, sheetname + '.csv')
                        schema = self.get_CSV_schema(targetpath, dialect='excel')
                    except:
                        raise
                        return 'Malformed CSV file.', 406
                    else:
                        schemas[sheetname] = schema

            if len(schemas) < 1:
                print schemas
                return ('Unable to extract usable CSV from the XLS[X] file.', 406)
            else:
                return schemas, 200

    # create a matrix using the provided features and file
    def ingest(self, posted_data, src):
        filepath = src['rootdir'] + 'source/' + posted_data['sourceName'] + '.csv'
        error = False
        matrices = []
        filter_outputs = []
        mat_id = getNewId()
        storepath = src['rootdir'] + mat_id + '/'
        try:
            df = load_matrix(filepath)
            maps = {}
            additions = []
            remove = []
            # handle before filters
            matrices, filters = self.apply_before_filters(posted_data, src)
            for i, feature in enumerate(posted_data['matrixFeaturesOriginal']):
                try:
                    df_feature = int(feature)
                except ValueError:
                    df_feature = feature
                try:
                    col = df[df_feature].values.tolist()
                except:
                    print "Unable to find feature", feature, "in dataframe. Available columns: ", df.columns.values.tolist()
                    raise
                # if the selected feature has any after filters, apply them
                if len(posted_data['matrixFilters'][feature]) > 0: # filters were selected
                    filt = posted_data['matrixFilters'][feature]
                    if filt['stage'] == 'after':
                        if filt['type'] == 'extract':#Having just extracts will cause program to break. Should not be allowed in genearl.
                            conf = {}
                            conf['values'] = col
                            conf['storepath'] = storepath
                            val = self.apply_filter(filt['filter_id'], filt['parameters'], conf)
                            if val != None:
                                matrices.append( val )
                            # posted_data['matrixFilters'].pop(feature, None)
                            # posted_data['matrixFeatures'].remove(feature)
                            # posted_data['matrixFeaturesOriginal'].remove(feature)
                            remove.append(feature)
                            filter_outputs.append('truth_labels.csv')
                        elif filt['type'] == 'convert':
                            col, posted_data['matrixTypes'][i] = self.apply_filter(filt['filter_id'], filt['parameters'], col)
                            add_field(maps, posted_data['matrixFeatures'][i], col, posted_data['matrixTypes'][i])
                        elif filt['type'] == 'add':
                            add_field(maps, posted_data['matrixFeatures'][i], col, posted_data['matrixTypes'][i])
                else:
                    add_field(maps, posted_data['matrixFeatures'][i], col, posted_data['matrixTypes'][i])
            # remove extracted features
            for feature in remove:
                posted_data['matrixFeatures'].remove(feature)
                posted_data['matrixFeaturesOriginal'].remove(feature)

            # if any features were added via filters, process them now
            process_additions(maps, additions, posted_data['matrixFeatures'])
            outputs = write_files(maps, posted_data['matrixFeatures'], posted_data['matrixFeaturesOriginal'], storepath)
            outputs.extend(filter_outputs)
            if not os.path.exists(storepath):
                os.makedirs(storepath, 0775)
            matrix = {
                'id': mat_id,
                'rootdir': storepath,
                'src_id': src['src_id'],
                'created': getCurrentTime(),
                'name': posted_data['matrixName'],
                'mat_type': 'csv',
                'outputs': outputs,
                'filters': posted_data['matrixFilters']
            }
            matrices.append(matrix)

        except:
            print "ERROR: error with matrix creation"
            traceback.print_exc(file=sys.stdout)
            error = True

        return error, matrices

    # used to convert Excel's internal date representation (days since Jan 0 1900) to python datetime
    # celltype mappings: http://www.lexicon.net/sjmachin/xlrd.html#xlrd.Cell-class
    def format_excel_cell(self, value,celltype,datemode):
        ret = value
        if celltype == 3:
            try:
                dateTuple = xlrd.xldate_as_tuple(value,datemode)
                ret = int(time.mktime(datetime(*dateTuple).timetuple()))
            except:
                ret = 0
        return ret

    def get_header(self, filepath, dialect):
        sniffer = csv.Sniffer()
        with open(filepath, 'rbU') as csvfile:
            # retain a sample line for use later
            sample = csvfile.readline()
            csvfile.seek(0)
            snippet = csvfile.read(2048)
            if dialect == '':
                dialect = sniffer.sniff(snippet)
            csvfile.seek(0)
            reader = csv.reader(csvfile, dialect)
            i = 0
            examples_lines = []
            for line in reader:
                if i < self.NUM_EXAMPLES:
                    examples_lines.append(line)
                    i += 1
                else:
                    break
            # get the header
            if sniffer.has_header(snippet):
                header = examples_lines.pop(0)
            else:
                header = [str(x+1) for x in range(0,len(examples_lines[0]))]
                # sometimes a floating point number like 1.4 is mistaken for two elements;
                # see if a line can be converted to a float point number, and if so
                # ensure that the number of header elements matches
                try:
                    float(sample)
                    examples_lines = []
                    csvfile.seek(0)
                    for i, line in enumerate(csvfile):
                        if i > 0 and i < self.NUM_EXAMPLES:
                            examples_lines.append([line.rstrip()])
                    header = [str(x+1) for x in range(0,len(examples_lines[0]))]
                # must not be able to convert the line to a numeric element
                except ValueError:
                    pass
            # handle single column instance when it has a header
            if len(examples_lines[0]) != len(header):
                examples_lines = []
                csvfile.seek(0)
                for i, line in enumerate(csvfile):
                    if i > 0 and i < self.NUM_EXAMPLES:
                        examples_lines.append([line.rstrip()])
        return examples_lines, header

    def get_size(self, filepath):
        with open(filepath, 'rbU') as f:
            row_count = sum(1 for row in f)
            return row_count

    # extract the schema from the dataset
    def get_CSV_schema(self, filepath, dialect=''):
        examples_lines, header = self.get_header(filepath, dialect)
        size = self.get_size(filepath)
        sampled = size > self.NUM_SAMPLES
        if header[0] == '1':
            data = pd.read_csv(filepath, header=None, nrows=self.NUM_SAMPLES)
        else:
            with open(filepath, 'rbU') as f:
                data = pd.read_csv(f, nrows=self.NUM_SAMPLES)
        numeric = data.describe()
        meta = {}
        for i, column in enumerate(data.columns):
            if column in numeric.columns:
                meta[header[i]] = {key:value for key, value in dict(numeric[column]).iteritems() if key != 'count' }
                type_ = 'Numeric'
            else:
                counts = Counter(data[column])
                if len(counts) > self.NUM_UNIQUE:
                    counts = dict(islice(counts.iteritems(), self.NUM_UNIQUE))
                meta[column] = counts
                type_ = 'String'
            meta[column]['suggestions'] = self.get_filters(type_)
            meta[column]['type'] = type_

        schema = {}
        schema['fields'] = meta
        schema['sampled'] = {'samples': self.NUM_SAMPLES, 'count': size}

        return schema
