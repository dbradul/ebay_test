import csv
import datetime
import json
import tempfile
import time
from abc import abstractmethod

from ebaysdk.exception import ConnectionError
# from ebaysdk.finding import Connection
from ebaysdk.trading import Connection

from ebay_rest import API, DateTime, Error, Reference
from pprint import pprint as pp

# try:
#     # api = Connection(appid='JohnHibs-USWFInte-PRD-45d7a0307-87ffa29e', config_file='credentials.yaml')
#     # # response = api.execute('findItemsAdvanced', {'keywords': 'legos'})
#     # response = api.execute('getSellerTransactions')
#     #
#     # # assert(response.reply.ack == 'Success')
#     # # assert(type(response.reply.timestamp) == datetime.datetime)
#     # # assert(type(response.reply.searchResult.item) == list)
#     # #
#     # # item = response.reply.searchResult.item[0]
#     # # assert(type(item.listingInfo.endTime) == datetime.datetime)
#     # # assert(type(response.dict()) == dict)
#     # #
#     # # print(item)
#     # print('SUCCESS!!')
#
#     from ebaysdk.trading import Connection as Trading
#     from ebaysdk.trading import Connection as Trading
#
#     api = Trading(config_file='credentials.yaml')
#     # response = api.execute('GetUser', {})
#     # response = api.execute('GetSellerTransactions', {'Pagination': {'EntriesPerPage': 100, 'PageNumber': 1}})
#     response = api.execute('GetOrders', {'NumberOfDays': 1, 'CreatedTimeFrom': '2022-01-01T00:00:00.000Z', 'CreatedTimeTo': '2022-01-02T00:00:00.000Z'})
#     # response = api.execute('GetSellerEvents', {'StartTimeFrom': '2022-02-01T00:00:00.000Z', 'StartTimeTo': '2022-10-02T00:00:00.000Z'})
#     # response = api.execute('GetMyeBaySelling', {'ActiveList': {'Include': True, 'Pagination': {'EntriesPerPage': 100, 'PageNumber': 1}}})
#     # response = api.execute('GetMyeBaySelling', {'ActiveList': {'Include': True, 'Pagination': {'EntriesPerPage': 100, 'PageNumber': 1}}})
#     print(response.dict())
#     print(response.reply)
#
# except ConnectionError as e:
#     print(e)
#     print(e.response.dict())








# print("\nClass documentation:")
# print(help(API))    # Over a hundred methods are available!
# print(help(DateTime))
# print(help(Error))
# print(help(Reference))


# result = api_conn.sell_marketing_create_report_task(body=report_data)
#
# if result is not None:
#     raise RuntimeError('Failed to create report task: ' + str(result))
#
# result = api.sell_marketing_get_report_tasks()
#
# current_task = result[0]
#
# while current_task.status != 'SUCCESS':
#     if current_task.status == 'FAILED':
#         raise RuntimeError('Report task failed: ' + str(current_task))
#
#     if current_task.status == 'IN_PROGRESS':
#         print('Report task is still in progress...')
#         time.sleep(5)
#         continue



class ReportPoller(object):
    POLLING_TIMEOUT_SEC = 60 * 10 * 2
    POLLING_SLEEP_SEC = 10

    REPORT_STATUS_READY = 'SUCCESS'
    REPORT_STATUS_FAILED = 'FAILED'
    REPORT_STATUS_PENDING = 'PENDING'

    def __init__(self, api_conn):
        self.api_conn = api_conn

    @abstractmethod
    def create_report_task(self, report_data):
        raise NotImplementedError('Must implement create_report_task')

    def get_report_task_id(self, report_task_response):
        raise NotImplementedError('Must implement get_report_task_id')

    def get_report_task_status(self, report_task_id):
        raise NotImplementedError('Must implement get_report_status')

    def get_report_id(self, report_task_id):
        return report_task_id

    def get_report_data(self, report_id):
        raise NotImplementedError('Must implement get_report_data')

    def transform_report_data(self, report_data):
        return report_data

    def delete_report_task(self, report_task_id):
        pass

    def wait_for_report(self):
        time.sleep(self.POLLING_SLEEP_SEC)

    def download_report(self, report_data):
        report_task_id = ''
        try:
            report_task_response = self.create_report_task(report_data)
            report_task_id = self.get_report_task_id(report_task_response)
            report_task_status = self.get_report_task_status(report_task_id)

            while report_task_status != self.REPORT_STATUS_READY:
                if report_task_status == self.REPORT_STATUS_FAILED:
                    raise Exception('Report failed to generate')
                self.wait_for_report()
                report_task_status = self.get_report_task_status(report_task_id)

            report_id = self.get_report_id(report_task_id)
            report_data = self.get_report_data(report_id)
            transformed_report_data = self.transform_report_data(report_data)
        finally:
            if report_task_id:
                self.delete_report_task(report_task_id)

        return transformed_report_data


class AmazonAdvertisingReportPoller(ReportPoller):

    def _parse_response(self, response):
        if not response['success']:
            raise osv.except_osv("Response parse error", response['response'])
        response_obj = json.loads(response['response'])
        return response_obj

    def create_report_task(self, report_data):
        report_type = report_data['reportType']
        report_data = report_data['reportData']
        result = self.api_conn.request_report(record_type=report_type, data=report_data)
        return result

    def get_report_task_status(self, report_task_id):
        response = self.api_conn.check_report_status(report_id=report_task_id)
        response_obj = self._parse_response(response)
        return response_obj['status']

    def get_report_task_id(self, report_task_response):
        report_response_obj = self._parse_response(report_task_response)
        return report_response_obj['reportId']

    def get_report_id(self, report_task_id):
        return report_task_id

    def get_report_data(self, report_id):
        report = self.api_conn.get_report(report_id=report_id)
        if not report['success']:
            err_msg = "Report fetch failed [%s]: %s" % (report['code'], report['reason'])
            _logger.error(err_msg)
            raise osv.except_osv('Amazon Report Fetch', ("%s" % err_msg))
        return report['response']

    def transform_report_data(self, report_data):
        return [
            line for line in report_data
            if any(line[metric] for metric in self.api_conn.metrics.get('quant'))
        ]



class EBayReportPoller(ReportPoller):
    def _parse_response(self, response):
        if not response['success']:
            raise osv.except_osv("Response parse error", response['response'])
        response_obj = json.loads(response['response'])
        return response_obj

    def create_report_task(self, report_data):
        result = self.api_conn.sell_marketing_create_report_task(body=body)
        return result

    def get_report_task_id(self, report_task_response):
        report_tasks = self.api_conn.sell_marketing_get_report_tasks()
        report_tasks = list(report_tasks)
        if not report_tasks:
            raise osv.except_osv("Report task not found", "Report task not found")
        return report_tasks[0]['record']['report_task_id']

    def get_report_task_status(self, report_task_id):
        response = self.api_conn.sell_marketing_get_report_task(report_task_id=report_task_id)
        return response['report_task_status']

    def get_report_id(self, report_task_id):
        response = self.api_conn.sell_marketing_get_report_task(report_task_id=report_task_id)
        return response['report_id']

    def get_report_data(self, report_id):
        report_data = self.api_conn.sell_marketing_get_report(report_id=report_id)
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(report_data)
            temp_file.seek(0)
            with open(temp_file.name, 'r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                return list(reader)

    def transform_report_data(self, report_data):
        return report_data

    def delete_report_task(self, report_task_id):
        self.api_conn.sell_marketing_delete_report_task(report_task_id=report_task_id)


print(f"eBay's official date and time is {DateTime.to_string(DateTime.now())}.\n")

print("All valid eBay global id values, also known as site ids.")
print(Reference.get_global_id_values(), '\n')

try:
    api = API(application='production_1', user='production_1', header='US')
except Error as error:
    print(f'Error {error.number} is {error.reason}  {error.detail}.\n')
else:
    try:
        print("The five least expensive iPhone things now for sale on-eBay:")


        body = {
            "reportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "dateTo": "2022-10-03T00:00:00.000Z",
            "metricKeys": [
                "CLICKS",
                "sales",
                "ad_fees",
                "impressions",
                "impressions",
                "avg_cost_per_sale"
                ],
            "dimensions": [
                {
                    "annotationKeys": [
                        "campaign_name",
                        "campaign_start_date",
                        "campaign_end_date"
                    ],
                    "dimensionKey": "campaign_id"
                },
                {
                    "annotationKeys": [
                        "listing_title",
                        "listing_quantity_sold"
                    ],
                    "dimensionKey": "listing_id"
                }
            ],
            "dateFrom": "2022-09-26T00:00:00.000Z",
            "marketplaceId": "EBAY_US",
            "reportFormat": "TSV_GZIP",
            "campaignIds": [
                "12975917014"
            ]
        }

        poller = EBayReportPoller(api)
        report = poller.download_report(report_data=body)

        # result = api.sell_marketing_create_report_task(body=body)
        # print(result)
        #
        # result = api.sell_marketing_get_report_tasks()
        # pp(list(result))
        #
        #
        # result = api.sell_marketing_get_report_tasks()
        # pp(list(result))
        #
        # result = api.sell_marketing_get_report_task(report_task_id='48225050014')
        # pp(list(result))
        #
        # result = api.sell_marketing_get_report(report_id='48225076014')
        # pp(list(result))

        # for record in api.buy_browse_search(q='iPhone', sort='price', limit=5):
        #     if 'record' not in record:
        #         pass    # TODO Refer to non-records, they contain optimization information.
        #     else:
        #         item = record['record']
        #         print(f"item id: {item['item_id']} {item['item_web_url']}")
    except Error as error:
        print(f'Error {error.number} is {error.reason} {error.detail}.\n')
    else:
        pass









# ------------------------------------------------------------------------------------------------
def poll_report_adv(adv_conn, report_type, report_data=None):
    _TOTAL_POLLING_TIMEOUT_SEC = 60 * 10 * 2

    response = adv_conn.request_report(record_type=report_type, data=report_data)
    if not response['success']:
        raise osv.except_osv("Report request error", response['response'])
    resp_obj = json.loads(response['response'])

    attempt_num, elapsed_time = 0, 0

    while resp_obj['status'] == AdvertisingApi.Status.IN_PROGRESS:
        if elapsed_time >= _TOTAL_POLLING_TIMEOUT_SEC:
            break
        sleep_time = 2 ** attempt_num
        _logger.info("Waiting for report (%ds)..." % sleep_time)
        time.sleep(sleep_time)
        elapsed_time += sleep_time
        attempt_num += 1

        response = adv_conn.check_report_status(report_id=resp_obj['reportId'])
        resp_obj = json.loads(response['response'])

    if resp_obj['status'] == AdvertisingApi.Status.SUCCESS:
        report = adv_conn.get_report(report_id=resp_obj['reportId'])
    else: #if resp_obj['status'] == AdvertisingApi.Status.FAILURE:
        err_prefix = "'%s' report fetch failed" % report_type
        err_msg = "%s: %s/%s" % (err_prefix, resp_obj['status'], resp_obj['statusDetails'])
        _logger.error(err_msg)
        raise osv.except_osv('Amazon Report Fetch', ("%s" % err_msg))

    if not report['success']:
        err_msg = "Report fetch failed [%s]: %s" % (report['code'], report['reason'])
        _logger.error(err_msg)
        raise osv.except_osv('Amazon Report Fetch', ("%s" % err_msg))

    not_empty_report_lines = [line for line in report['response']
                              if any(line[metric] for metric in adv_conn.metrics.get('quant'))]

    return not_empty_report_lines
