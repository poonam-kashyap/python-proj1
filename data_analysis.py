import zipfile
import csv
import logging
import xml.etree.ElementTree as ET
import requests
import boto3


logging.basicConfig(filename='logger.log',
                    format='%(asctime)s - %(message)s', level=logging.DEBUG)


def download_file(url: str) -> bytes:
    '''Takes string url as input.
       Downloads file with this url.
       Returns response content.
    '''
    try:
        response = requests.get(url)

    except Exception:
        logging.exception(
            f'Error occurred while downloading file with url : {url}')

    return response.content


def parse_first_xml() -> str:
    '''Parses the first xml file.
       Returns the first download link of file_type DLTINS
    '''
    FIRST_URL = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021\-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'
    FIRST_XML_NAME = 'esma_registers_firds_files.xml'
    download_url: str = ...

    response_content = download_file(FIRST_URL)

    try:
        with open(FIRST_XML_NAME, 'wb') as file:
            file.write(response_content)

        tree = ET.parse(FIRST_XML_NAME)

        root = tree.getroot()

        for str_elem in root.iter('str'):

            if str_elem.attrib['name'] == 'download_link':
                download_url = str_elem.text
            elif (str_elem.attrib['name'] == 'file_type'
                  and str_elem.text == 'DLTINS'):
                logging.info(
                    f'First download link whose file_type is DLTINS : {download_url}')
                return download_url

    except Exception:
        logging.exception('Exception occured while parsing first xml file')


def parse_second_xml(url: str) -> str:
    '''Takes string url of second xml file as input.
       Parses the second xml file and converts xml to csv.
       Returns the name of csv file.
    '''
    ZIP_FILENAME = 'second_xml.zip'
    CSV_FILENAME = 'DLTINS.csv'
    CSV_HEADER = [
        'FinInstrmGnlAttrbts.Id',
        'FinInstrmGnlAttrbts.FullNm',
        'FinInstrmGnlAttrbts.ClssfctnTp',
        'FinInstrmGnlAttrbts.CmmdtyDerivInd',
        'FinInstrmGnlAttrbts.NtnlCcy',
        'Issr',
    ]
    elem_tag_text: dict = {}
    row_count: int = 0

    response_content = download_file(url)

    try:
        with open(ZIP_FILENAME, 'wb') as zip_file:
            zip_file.write(response_content)
        logging.info(f'Zip file {ZIP_FILENAME} downloaded')

        zip = zipfile.ZipFile(ZIP_FILENAME)
        zip.extractall()
        logging.info(f'{ZIP_FILENAME} file extracted')

        csv_file = open(CSV_FILENAME, 'w')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(CSV_HEADER)
        logging.info(f'Header row written to csv file {CSV_FILENAME}')

        logging.info(f'Parsing second xml file...')

        for event, elem in ET.iterparse(zip.namelist()[0], events=('start', 'end')):

            tag = str(elem.tag)

            if event == 'end':
                if tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id':
                    elem_tag_text['Id'] = elem.text
                elif tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm':
                    elem_tag_text['FullNm'] = elem.text
                elif tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp':
                    elem_tag_text['ClssfctnTp'] = elem.text
                elif tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy':
                    elem_tag_text['NtnlCcy'] = elem.text
                elif tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd':
                    elem_tag_text['CmmdtyDerivInd'] = elem.text
                elif tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr':
                    elem_tag_text['Issr'] = elem.text
                    row = [
                        elem_tag_text['Id'],
                        elem_tag_text['FullNm'],
                        elem_tag_text['ClssfctnTp'],
                        elem_tag_text['CmmdtyDerivInd'],
                        elem_tag_text['NtnlCcy'],
                        elem_tag_text['Issr'],
                    ]
                    csv_writer.writerow(row)
                    row_count += 1

        logging.info(f'{row_count} rows written to csv file {CSV_FILENAME}')
        return CSV_FILENAME

    except Exception:
        logging.exception('Exception occurred while parsing second xml file')


def upload_csv_to_s3(file_name: str):
    '''Takes string filename as input.
       Uploads file to aws s3 bucket.
    '''
    BUCKET_NAME = 'steeleye-csv-bucket'

    try:
        s3_client = boto3.client('s3')

        s3_client.upload_file(file_name, BUCKET_NAME, file_name)

    except Exception:
        logging.exception(
            'Exception occurred while uploading csv file to aws s3')


download_link = parse_first_xml()
csv_filename = parse_second_xml(download_link)
upload_csv_to_s3(csv_filename)
