#!/usr/bin/env python3

import sys
import time
import argparse
from datetime import timedelta
import requests
import singer
from singer import utils
from pyrfc3339 import parse

PARSER = argparse.ArgumentParser()
PARSER.add_argument('--config', action='store', dest='path',
                    help='Path for configuration file')
PARSER.add_argument('--state', action='store', dest='state',
                    help='Path for state file')
ARGUMENTS = PARSER.parse_args()
LOGGER = singer.logger.get_logger()

if ARGUMENTS.path is None:
    LOGGER.error('Specify configuration file folder.')
    sys.exit(1)
PATH = ARGUMENTS.path
AUTH = utils.load_json(PATH)
if ARGUMENTS.state:
    STATE = utils.load_json(ARGUMENTS.state)
else:
    STATE = {}

ACCOUNT_SCHEMA = {"type":"object",
                  "properties":
                      {"accountId":{"type":["number"]},\
                       "accountName":{"type":["string"]},\
                       "accountType":{"type":["string"]},\
                       "userRole":{"type":["string"]},\
                       "startDate": {"type":["string"]},\
                       "endDate": {"type":["string"]}\
                      }
                 }
PROGRAMMES_SCHEMA = {"type":"object",
                     "properties":
                         {"id":{"type":["number"]}, \
                         "name":{"type":["mull", "string"]},\
                         "displayUrl":{"type":["mull", "string"]},\
                         "clickThroughUrl":{"type":["mull", "string"]},\
                         "logoUrl":{"type":["mull", "string"]},\
                         "countryName":{"type":["mull", "string"]},\
                         "countryCode":{"type":["mull", "string"]},\
                         "currencyCode":{"type":["mull", "string"]},\
                         "startDate": {"type":["string"]},\
                         "endDate": {"type":["string"]}\
                         }\
                    }


TRANSACTION_SCHEMA = {"type":"object",
                      "properties":{\
                             "id": {"type":["number"]},\
                             "url": {"type":["string"]},\
                             "advertiserId": {"type":["number"]},\
                             "publisherId": {"type":["number"]},\
                             "siteName": {"type":["null", "string"]},\
                             "datasettype": {"type":["string"]},\
                             "commissionStatus": {"type":["string"]},\
                             "commissionCurrency":{"type":["string"]},\
                             "saleAmount": {"type":["null", "number"]},\
                             "saleCurrency": {"type":["string"]},\
                             "ipHash": {"type":["null", "string"]},\
                             "customerCountry": {"type":["null", "string"]},\
                             "clickRef": {"type":["null", "string"]},\
                             "clickRef2": {"type":["null", "string"]},\
                             "clickRef3": {"type":["null", "string"]},\
                             "clickRef4": {"type":["null", "string"]},\
                             "clickRef5": {"type":["null", "string"]},\
                             "clickRef6": {"type":["null", "string"]},\
                             "clickDate": {"type":["null", "string"]},\
                             "transactionDate": {"type":["null", "string"]},\
                             "validationDate": {"type":["null", "string"]},\
                             "type": {"type":["null", "string"]},\
                             "declineReason": {"type":["null", "string"]},\
                             "voucherCodeUsed": {"type":["null", "boolean"]},\
                             "voucherCode": {"type":["null", "string"]},\
                             "lapseTime": {"type":["null", "number"]},\
                             "amended": {"type":["null", "boolean"]},\
                             "amendReason": {"type":["null", "string"]},\
                             "oldSaleAmount": {"type":["null", "number"]},\
                             "oldCommissionAmount": {"type":["null", "number"]},\
                             "clickDevice": {"type":["null", "string"]},\
                             "transactionDevice": {"type":["null", "string"]},\
                             "publisherUrl": {"type":["null", "string"]},\
                             "advertiserCountry": {"type":["null", "string"]},\
                             "orderRef": {"type":["null", "string"]},\
                             "customParameters":{"type":["null", "string"]},\
                             "commissionGroupId": {"type":["null", "number"]},\
                             "amount": {"type":["null", "number"]},\
                             "commissionAmount": {"type":["null", "number"]},\
                             "commissionGroupCode": {"type":["null", "string"]},\
                             "commissionGroupName": {"type":["null", "string"]},\
                             "paidToPublisher": {"type":["null", "boolean"]},\
                             "paymentId": {"type":["null", "number"]},\
                             "transactionQueryId": {"type":["null", "number"]},\
                             "originalSaleAmount": {"type":["null", "number"]},\
                             "startDate": {"type":["string"]},\
                             "endDate": {"type":["string"]}\
                             }\
                    }


REPORT_SCHEMA = {"type": "object",
                 "properties":{\
                         "advertiserId": {"type":["number"]},\
                         "advertiserName": {"type":["string"]},\
                         "publisherId": {"type":["number"]},\
                         "publisherName": {"type":["string"]},\
                         "region": {"type":["string"]},\
                         "currency": {"type":["string"]},\
                         "impressions": {"type":["number"]},\
                         "clicks": {"type":["number"]},\
                         "pendingNo": {"type":["number"]},\
                         "pendingValue": {"type":["number"]},\
                         "pendingComm": {"type":["number"]},\
                         "confirmedNo": {"type":["number"]},\
                         "confirmedValue": {"type":["number"]},\
                         "confirmedComm": {"type":["number"]},\
                         "bonusNo": {"type":["number"]},\
                         "bonusValue": {"type":["number"]},\
                         "bonusComm": {"type":["number"]},\
                         "totalNo": {"type":["number"]},\
                         "totalValue": {"type":["number"]},\
                         "totalComm": {"type":["number"]},\
                         "declinedNo": {"type":["number"]},\
                         "declinedValue": {"type":["number"]},\
                         "declinedComm": {"type":["number"]},\
                         "startDate": {"type":["string"]},\
                         "endDate": {"type":["string"]}\
                         }\
                }

PROGRAMMES_DETAILS = {"type":"object",
                      "properties":{\
                              "id":{"type":["number"]},\
                              "name":{"type":["string"]},\
                              "displayUrl":{"type":["string"]},\
                              "clickThroughUrl":{"type":["string"]},\
                              "logoUrl": {"type":["string"]},\
                              "countryname": {"type":["string"]},\
                              "countrycode" : {"type":["string"]},\
                              "validDomains": {"type":["string"]},\
                              "currencyCode": {"type":["string"]},\
                              "averagePaymentTime": {"type":["string"]},\
                              "approvalPercentage": {"type":["number"]},\
                              "epc": {"type":["number"]},\
                              "conversionRate": {"type":["number"]},\
                              "validationDays": {"type":["number"]},\
                              "awinIndex": {"type":["number"]},\
                              "percentagemin": {"type":["number"]},\
                              "percentagemax": {"type":["number"]},\
                              "amountmin": {"type":["number"]},\
                              "amountmax": {"type":["number"]},\
                              "startDate": {"type":["string"]},\
                              "endDate": {"type":["string"]}\
                              }
                     }


AGGREGATED_CREATIVE_SCHEMA = {"type":"object",
                              "properties":
                                  {\
                                  "advertiserId":{"type":["number"]},\
                                  "advertiserName":{"type":["string"]},\
                                  "publisherId":{"type":["number"]},\
                                  "publisherName":{"type":["string"]},\
                                  "region":{"type":["string"]},\
                                  "currency":{"type":["string"]},\
                                  "impressions":{"type":["number"]},\
                                  "clicks":{"type":["number"]},\
                                  "creativeId":{"type":["number"]},\
                                  "creativeName":{"type":["string"]},\
                                  "tagName":{"type":["string"]},\
                                  "pendingNo":{"type":["number"]},\
                                  "pendingValue":{"type":["number"]},\
                                  "pendingComm":{"type":["number"]},\
                                  "confirmedNo":{"type":["number"]},\
                                  "confirmedValue":{"type":["number"]},\
                                  "confirmedComm":{"type":["number"]},\
                                  "bonusNo":{"type":["number"]},\
                                  "bonusValue":{"type":["number"]},\
                                  "bonusComm":{"type":["number"]},\
                                  "totalNo":{"type":["number"]},\
                                  "totalValue":{"type":["number"]},\
                                  "totalComm":{"type":["number"]},\
                                  "declinedNo":{"type":["number"]},\
                                  "declinedValue":{"type":["number"]},\
                                  "declinedComm":{"type":["number"]},\
                                  "startDate": {"type":["string"]},\
                                  "endDate": {"type":["string"]}\
                                  }
                             }

COMMISSIONGROUP_SCHEMA = {"type":"object",
                          "properties":{\
                                          "groupId": {"type":["number"]},\
                                          "groupCode": {"type":["string"]},\
                                          "groupName": {"type":["string"]},\
                                          "type": {"type":["string"]},\
                                          "percentage": {"type":["number"]},\
                                          "startDate": {"type":["string"]},\
                                          "endDate": {"type":["string"]}\
                                        }
                         }
PUBLISHERS = []
ADVERTISERS = []
def getaccount():
    accounts = requests.get('https://api.awin.com/accounts?accessToken=' + AUTH['accessToken'],
                            headers={"user_agent":AUTH['user_agent']})
    if accounts.status_code == 200:
        singer.write_schema("Accounts", ACCOUNT_SCHEMA, ["accountId"])
        for account in accounts.json()['accounts']:
            if account['accountType'] == 'advertiser':
                ADVERTISERS.append(account['accountId'])
            if account['accountType'] == 'publisher':
                PUBLISHERS.append(account['publisher'])
            account["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
            account["endDate"] = str(parse(STATE['last_fetched']) + \
                                           timedelta(days=AUTH['increment']))
            singer.write_record("Accounts", account)
    else:
        LOGGER.error(accounts.json()['error'])
        sys.exit(0)

def getprogrammes():
    for publisher in PUBLISHERS:
        programmes = requests.get('https://api.awin.com/publishers/' + str(publisher)+\
                                  '/programmes?accessToken=' + AUTH['accessToken'],\
                                  params=STATE['programmes'],
                                  headers={"User-Agent":AUTH['user_agent']})
        if programmes.status_code == 200:
            singer.write_schema("Programmes", PROGRAMMES_SCHEMA, ["id"])
            for program in programmes.json():
                if 'primaryRegion' in program and program['primaryRegion'] != None:
                    program['countryName'] = program['primaryRegion']['name']
                    program["countryCode"] = program['primaryRegion']['countryCode']
                    program.pop('primaryRegion')
                program["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
                program["endDate"] = str(parse(STATE['last_fetched']) + \
                                           timedelta(days=AUTH['increment']))
                singer.write_record('Programmes', program)
        else:
            LOGGER.info('Error '+ str(programmes.content).replace('\n', ' ') +\
                        ' while retriving data for publisher ' + str(publisher))
    time.sleep(10)

def getprogrammesdetails():
    singer.write_schema('ProgrammesDetails', PROGRAMMES_DETAILS, ['id'])
    for publisher in PUBLISHERS:
        for advertiser in ADVERTISERS:
            progdetails = requests.get('https://api.awin.com/publishers/' + str(publisher) +\
                                             '/programmedetails?advertiserId' +  str(advertiser),
                                       headers={"User-Agent":AUTH['user_agent']})
            if progdetails.status_code != 200:
                LOGGER.info('Error ' + str(progdetails.content).replace('\n', ' ') + \
                            ' in programmes details for Publisher:' + str(publisher) + \
                            ' and avdertiser:' + str(advertiser))
            else:
                progdetails.update(progdetails['kpi'])
                del progdetails['kpi']
                progdetails.update(progdetails['programmeInfo'])
                del progdetails['programmeInfo']
                progdetails['countryCode'] = progdetails['primaryRegion']['countryCode']
                progdetails['countryName'] = progdetails['primaryRegion']['name']
                del progdetails['primaryRegion']
                progdetails['validDomains'] = ','.join([domain['domain'] \
                                                       for domain in progdetails['validDomains']])
                progdetails['amountmin'], progdetails['amountmax'] = \
                             [(i['max'], i['min']) for i in progdetails['commissionRange'] \
                                                       if i['type'] == 'amount'][0]
                progdetails['percentagemin'], progdetails['percentagemax'] = \
                            [(i['max'], i['min']) for i in progdetails['commissionRange'] \
                                                        if i['type'] == 'percentage'][0]
                progdetails["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
                progdetails["endDate"] = str(parse(STATE['last_fetched']) + \
                                               timedelta(days=AUTH['increment']))
                singer.write_record('ProgrammesDetails', progdetails)
    time.sleep(10)

def transactiondataset(dataset, ttype):
    if 'commissionAmount' in dataset:
        commamount = dataset['commissionAmount']['amount']
        dataset['commissionCurrency'] = dataset['commissionAmount']['currency']
        dataset.pop('commissionAmount')
        dataset['commissionAmount'] = commamount
    if 'saleAmount' in dataset:
        saleamount = dataset['saleAmount']['amount']
        dataset['saleCurrency'] = dataset['saleAmount']['currency']
        dataset.pop('saleAmount')
        dataset['saleAmount'] = saleamount
    if 'clickRefs' in dataset and dataset['clickRefs'] != None:
        dataset.update(dataset.pop('clickRefs'))
    dataset['datasettype'] = ttype
    if 'customParameters' in dataset and dataset['customParameters'] != None:
        dataset['customParameters'] = str(dict([(i['key'], i['value']) \
                                               for i in dataset['customParameters']]))
    if  'transactionParts' in dataset:
        transactionparts = [parts for parts in dataset['transactionParts']]
        dataset.pop('transactionParts')
    dataset["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
    dataset["endDate"] = str(parse(STATE['last_fetched']) + timedelta(days=AUTH['increment']))
    for data in transactionparts:
        dataset.update(data)
        singer.write_record('Transactions', dataset)

def gettransactionlist():
    singer.write_schema('Transactions', TRANSACTION_SCHEMA, ['id'])
    for advertiser in ADVERTISERS:
        transaction = requests.get('https://api.awin.com/advertisers/' + str(advertiser) +
                                   '/transactions/', params=STATE['transactions'],
                                   headers={"User-Agent":AUTH['user_agent']})
        if transaction.status_code == 200:
            for row in transaction.json():
                transactiondataset(row, 'advertiser')
        else:
            LOGGER.error('Error ' + str(transaction.content).replace('\n', ' ') + \
                         ' while retriving transaction list for avdertiser:' + str(advertiser))
    time.sleep(5)
    for publisher in PUBLISHERS:
        transaction = requests.get('https://api.awin.com/publishers/' + str(publisher) +
                                   '/transactions/', params=STATE['transactions'],
                                   headers={"User-Agent":AUTH['user_agent']})
        if transaction.status_code == 200:
            for row in transaction.json():
                transactiondataset(row, 'publisher')
        else:
            LOGGER.error('Error ' + str(transaction.content).replace('\n', ' ') +
                         ' while retriving transaction list for publisher:' + str(publisher))
    time.sleep(5)

def getaggreport():
    singer.write_schema("AggReport", REPORT_SCHEMA, ["advertiserId", "publisherId", "region"])
    for advertiser in ADVERTISERS:
        reportdataset = requests.get('https://api.awin.com/advertisers/' + str(advertiser) +
                                     '/reports/publisher', params=STATE['aggregatedReport'],
                                     headers={"User-Agent":AUTH['user_agent']})
        if reportdataset.status_code == 200:
            for data in reportdataset.json():
                data["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
                data["endDate"] = str(parse(STATE['last_fetched']) + \
                                               timedelta(days=AUTH['increment']))
                singer.write_record("AggReport", data)
        else:
            LOGGER.error('Error ' + str(reportdataset.content).replace('\n', ' ') +
                         ' while retriving aggregate report for advertiser:' + str(advertiser))
    time.sleep(5)
    for publisher in PUBLISHERS:
        reportdataset = requests.get('https://api.awin.com/publishers/' + str(publisher) +
                                     '/reports/advertiser', params=STATE['aggregatedReport'],
                                     headers={"User-Agent":AUTH['user_agent']})
        if reportdataset.status_code == 200:
            for data in reportdataset.json():
                data["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
                data["endDate"] = str(parse(STATE['last_fetched']) + \
                                               timedelta(days=AUTH['increment']))
                singer.write_record("AggReport", data)
        else:
            LOGGER.error('Error ' + str(reportdataset.content).replace('\n', ' ') +
                         ' while retriving aggregate report for publisher:' + str(publisher))
    time.sleep(5)

def getaggreportcreative():
    singer.write_schema("AggReport", AGGREGATED_CREATIVE_SCHEMA, ["advertiserId", "publisherId",
                                                                  "region"])
    for advertiser in ADVERTISERS:
        reportdataset = requests.get('https://api.awin.com/advertisers/'+str(advertiser) +
                                     '/reports/creative', params=STATE['aggregatedByCreative'],
                                     headers={"User-Agent":AUTH['user_agent']})
        if reportdataset.status_code == 200:
            for data in reportdataset.json():
                data["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
                data["endDate"] = str(parse(STATE['last_fetched']) + \
                                               timedelta(days=AUTH['increment']))
                singer.write_record("AggReport", data)
        else:
            LOGGER.error("Error" + str(reportdataset.content).replace('\n', ' ') +
                         " while extracting data in report creative for advertiser: " +
                         str(advertiser))
    time.sleep(5)
    for publisher in PUBLISHERS:
        reportdataset = requests.get('https://api.awin.com/publishers/' + str(publisher) +
                                     '/reports/creative', params=STATE['aggregatedByCreative'],
                                     headers={"User-Agent":AUTH['user_agent']})
        if reportdataset.status_code == 200:
            for data in reportdataset.json():
                data["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
                data["endDate"] = str(parse(STATE['last_fetched']) + \
                                               timedelta(days=AUTH['increment']))
                singer.write_record("AggReport", data)
        else:
            LOGGER.error("Error " + str(reportdataset.content).replace('\n', ' ') +
                         " while extracting data in report creative for publisher: " +
                         str(publisher))
    time.sleep(5)

def getcommissiongroups():
    singer.write_schema('Commissiongroup', COMMISSIONGROUP_SCHEMA, ['groupId'])
    for publisher in PUBLISHERS:
        for advertiser in ADVERTISERS:
            commissiongroups = requests.get('https://api.awin.com/publishers/' + publisher +
                                            '/commissiongroups?advertiserId=' + advertiser,
                                            params=AUTH['accessToken'],
                                            headers={"User-Agent":AUTH['user_agent']})
            if commissiongroups.status_code == 200:
                for commission in commissiongroups.json():
                    commission["startDate"] = str(parse(STATE['last_fetched']) + timedelta(days=1))
                    commission["endDate"] = (parse(STATE['last_fetched']) + \
                                               timedelta(days=AUTH['increment']))
                    singer.write_record('Commissiongroup', commission)
            else:
                LOGGER.error("Error" + str(commissiongroups.content).replace('\n', ' ') +
                             " while extracting data for commission group for publisher: " +
                             str(publisher) + " and advertiser: " + str(advertiser))

def getreports():
    getaccount()
    getprogrammes()
    getprogrammesdetails()
    gettransactionlist()
    getaggreport()
    getaggreportcreative()
    getcommissiongroups()

def main():
    if not any(STATE):
        try:
            DATE = parse(AUTH['start_date'])
        except ValueError:
            LOGGER.error('start_date should be in RFC3339 format')
            sys.exit(1)
        STATE['transactions'] = AUTH['transactions']
        STATE['transactions']['startDate'] = str(DATE.isoformat())[0:19]
        STATE['transactions']['endDate'] = str((DATE + timedelta(days=AUTH['increment'])\
                                                     + timedelta(seconds=-1))\
                                                                 .isoformat())[0:19]
        STATE['transactions']['accessToken'] = AUTH['accessToken']

        STATE['aggregatedByCreative'] = AUTH['aggregatedByCreative']
        STATE['aggregatedByCreative']['startDate'] = str(DATE.isoformat())[0:10]
        STATE['aggregatedByCreative']['endDate'] = str((DATE + \
                                                        timedelta(days=(AUTH['increment']-1)))\
                                                        .isoformat())[0:10]
        STATE['aggregatedByCreative']['accessToken'] = AUTH['accessToken']

        STATE['aggregatedReport'] = AUTH['aggregatedReport']
        STATE['aggregatedReport']['startDate'] = str(DATE.isoformat())[0:10]
        STATE['aggregatedReport']['endDate'] = str((DATE + timedelta(days=(AUTH['increment']-1)))\
                                                   .isoformat())[0:10]
        STATE['aggregatedReport']['accessToken'] = AUTH['accessToken']

        STATE['programmes'] = AUTH['programmes']
        STATE['programmes']['accessToken'] = AUTH['accessToken']

        STATE['last_fetched'] = str((DATE + timedelta(days=AUTH['increment'])\
                                                     + timedelta(seconds=-1))\
                                                                 .isoformat())

    else:
        DATE = parse(STATE['last_fetched']) + timedelta(seconds=1)
        STATE['transactions']['startDate'] = str(DATE.isoformat())[0:19]
        STATE['transactions']['endDate'] = str((DATE + timedelta(days=AUTH['increment'])\
                                                     + timedelta(seconds=-1))\
                                              .isoformat())[0:19]
        STATE['transactions']['accessToken'] = AUTH['accessToken']

        STATE['aggregatedByCreative']['startDate'] = str(DATE.isoformat())[0:10]
        STATE['aggregatedByCreative']['endDate'] = str((DATE +
                                                        timedelta(days=(AUTH['increment'] -1)))\
                                                        .isoformat())[0:10]
        STATE['aggregatedByCreative']['accessToken'] = AUTH['accessToken']

        STATE['aggregatedReport']['startDate'] = str(DATE.isoformat())[0:10]
        STATE['aggregatedReport']['endDate'] = str((DATE + timedelta(days=(AUTH['increment'] - 1)))\
                                                        .isoformat())[0:10]
        STATE['aggregatedReport']['accessToken'] = AUTH['accessToken']

        STATE['programmes']['accessToken'] = AUTH['accessToken']

    getreports()
    STATE['last_fetched'] = str((DATE + timedelta(days=AUTH['increment'])\
                                                     + timedelta(seconds=-1))\
                                              .isoformat())
    del STATE['transactions']['accessToken']
    del STATE['aggregatedByCreative']['accessToken']
    del STATE['aggregatedReport']['accessToken']
    del STATE['programmes']['accessToken']

    singer.write_state(STATE)


if __name__ == "__main__":
    main()
