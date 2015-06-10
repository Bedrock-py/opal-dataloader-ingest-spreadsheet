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

# from ..DetectDatatype import DetectDatatype
import os, socket, datetime, time
import csv, xlrd
import re
from dataloader.utils import *
from dataloader.opals.spreadsheet_utils import *

class Spreadsheet(Ingest):
    def __init__(self):
        super(Spreadsheet, self).__init__()
        self.name = 'CSV/Microsoft Excel'
        self.description = 'Loads data from CSV or Microsft Excel spreadsheets.'

        self.parameters_spec = [{ "name" : "file", "value" : ".csv,.xls,.xlsx", "type" : "file" }]


    def explore(self, filepath):
        #find the appropriate file

        #check to see if csv or xls[x]
        files = os.listdir(filepath)
        if len(files) > 1: #means we have generated csv files from xls[x]
            for i,filename in enumerate(files):
                if 'xls' in filename: #find the xls[x] file
                    target = files[i]
                    break
        else:
            target = files[0]

        targetpath = filepath + '/' + target

        if 'csv' in targetpath:
            filename = re.split('/', targetpath)[-1]
            name = filename[:-4]
            try:
                schema = self._getCSVSchema(targetpath)
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
                                csv_writer.writerow([self._formatExcelCell(sheet.cell_value(curr_row, col),sheet.cell_type(curr_row,col),book.datemode) for col in range(sheet.ncols)])
                    except:
                        print "Unable to convert %s to valid CSV." % (sheetname)
            
            for sheetname in sheets:
                if sheetname + '.csv' in os.listdir(filepath):
                    try:
                        targetpath = os.path.join(filepath, sheetname + '.csv')
                        schema = self._getCSVSchema(targetpath, dialect='excel')
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


    #create a matrix using the provided features and file
    def ingest(self, posted_data, src):
        filepath = src['rootdir'] + 'source/' + posted_data['sourceName'] + '.csv'
        matrix = {}
        matrix['id'] = getNewId()

        storepath = src['rootdir'] + matrix['id'] + '/'
        if not os.path.exists(storepath):
            os.makedirs(storepath, 0775)

        error = False
        matrices = []
        filter_outputs = []
        try:
            df = loadMatrix(filepath)

            maps = {}
            additions = []

            #handle before filters
            for field, filt in posted_data['matrixFilters'].items():
                if len(filt) > 0:
                    if filt['stage'] == 'before':
                        if filt['type'] == 'extract':
                            #create new matrix metadata
                            conf['mat_id'] = getNewId()
                            conf['storepath'] = src['rootdir'] + conf['mat_id'] + '/'
                            conf['src_id'] = src['src_id']
                            conf['name'] = posted_data['matrixName']
                            val = self.apply_filter(filt['filter_id'], filt['parameters'], conf)
                            if val != None:
                                matrices.append( val )
                            posted_data['matrixFilters'].pop(field, None)
                        elif filt['type'] == 'convert':
                            pass
                        elif filt['type'] == 'add':
                            pass

            for i, feature in enumerate(posted_data['matrixFeaturesOriginal']):
                # if feature in matirxFilters:
                #     filters = matrixFilters[feature]
                col = df[feature].values.tolist()
                #     col, matrixTypes[i] = self.apply_filter(filters['filter_id'], filters['parameters_spec'], col) 
                #handle before filters
                if len(posted_data['matrixFilters'][feature]) > 0: #filters were selected
                    filt = posted_data['matrixFilters'][feature]
                    if filt['stage'] == 'after':
                        if filt['type'] == 'extract':
                            conf = {}
                            conf['values'] = col
                            conf['storepath'] = storepath
                            val = self.apply_filter(filt['filter_id'], filt['parameters'], conf)
                            if val != None:
                                matrices.append( val )
                            # posted_data['matrixFilters'].pop(feature, None)
                            posted_data['matrixFeatures'].remove(feature)
                            posted_data['matrixFeaturesOriginal'].remove(feature)
                            filter_outputs.append('truth_labels.csv')
                        elif filt['type'] == 'convert':
                            col, posted_data['matrixTypes'][i] = self.apply_filter(filt['filter_id'], filt['parameters'], col) 
                            addField(maps, posted_data['matrixFeatures'][i], col, posted_data['matrixTypes'][i])
                        elif filt['type'] == 'add':
                            addField(maps, posted_data['matrixFeatures'][i], col, posted_data['matrixTypes'][i])
                else:
                    addField(maps, posted_data['matrixFeatures'][i], col, posted_data['matrixTypes'][i])

            #if any features were added via filters, process them now
            processAdditions(maps, additions, posted_data['matrixFeatures'])
            outputs = writeFiles(maps, posted_data['matrixFeatures'], posted_data['matrixFeaturesOriginal'], storepath)
            outputs.extend(filter_outputs)
                
            matrix['rootdir'] = storepath
            matrix['src_id'] = src['src_id']
            matrix['created'] = getCurrentTime()
            matrix['name'] = posted_data['matrixName']
            matrix['mat_type'] = 'csv'
            matrix['outputs'] = outputs
            matrix['filters'] = posted_data['matrixFilters']

            matrices.append(matrix)


        except:
            raise
            error = True

        return error, matrices



    # Used primarily to convert Excel's internal date representation (days since Jan 0 1900) to python datetime
    # Celltype mappings: http://www.lexicon.net/sjmachin/xlrd.html#xlrd.Cell-class
    def _formatExcelCell(self, value,celltype,datemode):
        ret = value
        if celltype == 3:
            try:
                dateTuple = xlrd.xldate_as_tuple(value,datemode)
                ret = int(time.mktime(datetime(*dateTuple).timetuple()))
            except:
                ret = 0
        return ret


    #exctract the schema from the dataset
    def _getCSVSchema(self, filepath, dialect=''):
        #open the file and get the header, if any
        sniffer = csv.Sniffer()
        with open(filepath, 'rbU') as csvfile:
            snippet = csvfile.read(2048)
            if dialect == '':
                dialect = sniffer.sniff(snippet)
            csvfile.seek(0)
            reader = csv.reader(csvfile, dialect)
            i = 0
            examples_lines = []
            for line in reader:
                if i < 10:
                    examples_lines.append(line)
                    i += 1
                else:
                    break

            #get the header
            if sniffer.has_header(snippet):
                header = examples_lines.pop(0)

            else:
                header = [str(x) for x in range(1,len(examples_lines[0]) + 1)]

        schema = []
        # for each column
        for i in range(len(examples_lines[0])):
            # represents a single column/field
            obj = {} 
            # temp = [1,3,2,4,1,3] (examples from each column)
            temp = []
            for j in range(len(examples_lines)):
                temp.append(examples_lines[j][i])

            # see if the first example is numeric
            try:
                float(temp[0])
            # must be string
            except ValueError:
                obj['type'] = ['String']

            else:
                obj['type'] = ['Numeric']
            
            #give it a label
            obj['key'] = header[i]
            obj['key_usr'] = header[i]
            #set the examples
            obj['examples'] = temp
            #set a fake range
            obj['range'] = [-1,-1]
            #run the detect algorithm
            # detect = DetectDatatype(obj)

            #get suggestions
            # obj['suggestions'] = detect.getSuggestions()
            # #get top suggestion
            # obj['suggestion'] = detect.getTopSuggestion()
            # obj['options'] = detect.possibleTypes()

            obj['suggestions'] = self.get_filters(obj['type'][0])
            obj['suggestion'] = self.get_best_filter(obj['type'][0], header[i], temp[0])
            obj['options'] = self.get_filters(obj['type'][0])

            #add to schema, then repeat for next column
            schema.append(obj)
        return schema

