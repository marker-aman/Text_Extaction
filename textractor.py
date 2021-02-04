import sys
import os
from urllib.parse import urlparse
import boto3
import time
from tdp import DocumentProcessor
# from og import OutputGenerator
# from helper import FileHelper, S3Helper

class Textractor:
    def getInputParameters(self, args):
        event = {}
        i = 0
        if(args):
            while(i < len(args)):
                if(args[i] == '--documents'):
                    event['documents'] = args[i+1]
                    i = i + 1
                if(args[i] == '--region'):
                    event['region'] = args[i+1]
                    i = i + 1
                if(args[i] == '--text'):
                    event['text'] = True
                if(args[i] == '--forms'):
                    event['forms'] = True
                if(args[i] == '--tables'):
                    event['tables'] = True
                if(args[i] == '--insights'):
                    event['insights'] = True
                if(args[i] == '--medical-insights'):
                    event['medical-insights'] = True
                if(args[i] == '--translate'):
                    event['translate'] = args[i+1]
                    i = i + 1

                i = i + 1
        return event

    def validateInput(self, args):

        event = self.getInputParameters(args)

        ips = {}

        if(not 'documents' in event):
            raise Exception("Document or path to a foler or S3 bucket containing documents is required.")

        inputDocument = event['documents']
        idl = inputDocument.lower()

        bucketName = None
        documents = []
        awsRegion = 'us-east-1'

        if(idl.startswith("s3://")):
            o = urlparse(inputDocument)
            bucketName = o.netloc
            path = o.path[1:]
            ar = S3Helper.getS3BucketRegion(bucketName)
            if(ar):
                awsRegion = ar

            if(idl.endswith("/")):
                allowedFileTypes = ["jpg", "jpeg", "png", "pdf"]
                documents = S3Helper.getFileNames(awsRegion, bucketName, path, 1, allowedFileTypes)
            else:
                documents.append(path)
        else:
            if(idl.endswith("/")):
                allowedFileTypes = ["jpg", "jpeg", "png"]
                documents = FileHelper.getFileNames(inputDocument, allowedFileTypes)
            else:
                documents.append(inputDocument)

            if('region' in event):
                awsRegion = event['region']

        ips["bucketName"] = bucketName
        ips["documents"] = documents
        ips["awsRegion"] = awsRegion
        ips["text"] = ('text' in event)
        ips["forms"] = ('forms' in event)
        ips["tables"] = ('tables' in event)
        ips["insights"] = ('insights' in event)
        ips["medical-insights"] = ('medical-insights' in event)
        if("translate" in event):
            ips["translate"] = event["translate"]
        else:
            ips["translate"] = ""

        return ips
    
    def get_cell_text(self,result, blocks_map):
        text = ''
        if 'Relationships' in result:
            for relationship in result['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        word = blocks_map[child_id]
                        if word['BlockType'] == 'WORD':
                            text += word['Text'] + ' '
                        if word['BlockType'] == 'SELECTION_ELEMENT':
                            if word['SelectionStatus'] =='SELECTED':
                                text +=  'X '    
        return text        

    def get_lookup_row_col(self,table_result, blocks_map):
        rows = {}
        for relationship in table_result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    cell = blocks_map[child_id]
                    if cell['BlockType'] == 'CELL':
                        row_index = cell['RowIndex']
                        col_index = cell['ColumnIndex']
                        if row_index not in rows:
                            rows[row_index] = {}

                        rows[row_index][col_index] = self.get_cell_text(cell, blocks_map)
        return rows

    def table_csv(self,table_result, blocks_map, table_index):
        rows = self.get_lookup_row_col(table_result, blocks_map)

        table_id = 'Table_' + str(table_index)
        
        # get cells.
        csv = 'Table: {0}\n\n'.format(table_id)

        for row_index, cols in rows.items():
            
            for col_index, text in cols.items():
                csv += '{}'.format(text) + ","
            csv += '\n'
            
        csv += '\n\n\n'
        return csv     

    def processDocument(self, ips, i, document):
        print("\nTextracting Document # {}: {}".format(i, document))
        print('=' * (len(document)+30))

        # Get document textracted
        dp = DocumentProcessor(ips["bucketName"], document, ips["awsRegion"], ips["text"], ips["forms"], ips["tables"])
        response = dp.run()
        blocks=[]
        for docs in response:
            blockList= docs['Blocks']
            for block in blockList:
                 blocks.append(block)

        blocks_map = {}
        table_blocks=[]

        for block in blocks:
            blocks_map[block['Id']] = block
            if block['BlockType'] == "TABLE":
                table_blocks.append(block)
        if len(table_blocks) <= 0:
            return "<b> NO Table FOUND </b>"

        csv = ''
        for index, table in enumerate(table_blocks):
            csv += self.table_csv(table, blocks_map, index +1)
            csv += '\n\n'

        return csv
       

    def printFormatException(self, e):
        print("Invalid input: {}".format(e))
        print("Valid format:")
        print('- python3 textractor.py --documents mydoc.jpg --text --forms --tables --region us-east-1')
        print('- python3 textractor.py --documents ./myfolder/ --text --forms --tables')
        print('- python3 textractor.py --documents s3://mybucket/mydoc.pdf --text --forms --tables')
        print('- python3 textractor.py --documents s3://mybucket/ --text --forms --tables')

    def run(self):

        ips = None
        try:
            ips = self.validateInput(sys.argv)
        except Exception as e:
            self.printFormatException(e)

        #try:
        i = 1
        totalDocuments = len(ips["documents"])

        print("\n")
        print('*' * 60)
        print("Total input documents: {}".format(totalDocuments))
        print('*' * 60)

        for document in ips["documents"]:
            doc=self.processDocument(ips, i, document)
            output_file = 'STEWARDSHIP_Report_decade_transition_Web'+ str(i)+'.csv'

            with open(output_file, "wt") as fout:
                fout.write(doc)


            remaining = len(ips["documents"])-i

            if(remaining > 0):
                print("\nRemaining documents: {}".format(remaining))

            i = i + 1

        print("\n")
        print('*' * 60)
        print("Successfully textracted documents: {}".format(totalDocuments))
        print('*' * 60)
        print("\n")

Textractor().run()
