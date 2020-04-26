__author__ = 'danish-wani'
import os
from collections import OrderedDict

import pandas as pd
from django.conf import settings
from django.db import connection
from django.http import StreamingHttpResponse


class ErrorLog:
    def __init__(self):
        self.error_log = OrderedDict()


class CSVReportGenerator(ErrorLog):
    def __init__(self, file_name, file_path, headers=None, customized_headers=None):
        super().__init__()
        """

        :param file_name: file name
        :param file_path: Path of the csv file
        :param headers: for list of list data
        :param customized_headers: if data is a list of dictionaries then the headers will mostly be a dictionary
                        (can be a list too) where key will be the actual keys present in the data and values will be
                        respective Display column names. And if data is [[], [], ....] then we can either pass
                        dictionary or simply a list of headers both must be in same order as of data (OPTIONAL)
                        If column name in csv needs to be different from column name in query pass a dictionary of
                        {'column_name_in_query': 'column_name_in_csv', ...}
        """
        self.file_name = file_name
        self.file_path = file_path
        self.headers = headers
        self.customized_headers = customized_headers

    def write_from_data(self, data):
        """
            Writes data to a csv created at self.file_path
        : param data:    Data to be written, format either [[], [], ...] or [{}, {}, ...]
        :return:
        """
        try:
            try:
                try:
                    data_frame = pd.DataFrame(data,
                                              columns=self.customized_headers if self.customized_headers else self.headers)
                except ValueError:
                    data_frame = pd.DataFrame(data, index=[0],
                                              columns=self.customized_headers if self.customized_headers else self.headers)
                if self.customized_headers and isinstance(self.customized_headers, dict):
                    data_frame.columns = self.customized_headers.values()
                if self.customized_headers and (isinstance(self.customized_headers, tuple) or
                                                isinstance(self.customized_headers, list)):
                    data_frame.columns = self.customized_headers

                self.write_to_csv(data_frame)

            except ValueError as e:
                self.error_log.update(dict(ValueError=str(e)))
                self.write_error()
        except Exception as e:
            self.error_log.update(dict(Exception=str(e)))
            self.write_error()

    def write_from_query(self, query, raw=True):
        """
            Writes to csv from a raw sql query or ORM
        :param query: can be either raw sql query or ORM
        :param raw: True if query is raw sql query, False if query is ORM
        :return:
        """
        try:
            try:
                if raw:
                    data_frame = pd.read_sql(query, connection,
                                             columns=self.customized_headers if self.customized_headers else self.headers)
                else:
                    data_frame = pd.read_sql(str(query.query), connection,
                                             columns=self.customized_headers if self.customized_headers else self.headers)
                if self.customized_headers and isinstance(self.customized_headers, dict):
                    data_frame.columns = self.customized_headers.values()
                if self.customized_headers and (isinstance(self.customized_headers, tuple) or
                                                isinstance(self.customized_headers, list)):
                    data_frame.columns = self.customized_headers

                self.write_to_csv(data_frame)
            except ValueError as e:
                self.error_log.update(dict(ValueError=str(e)))
                self.write_error()
        except Exception as e:
            self.error_log.update(dict(Exception=str(e)))
            self.write_error()

    def write_to_csv(self, data_frame):
        """
            Write to CSV from data frame
        :param data_frame: Data frame
        :return:
        """
        try:
            data_frame.to_csv(self.file_path, index=False)
        except Exception:
            self.file_path = 'error.csv'
            data_frame.to_csv(self.file_path, index=False)

    def write_error(self):
        """
            Writes the errors that occurred while writing data to csv
        :return:
        """
        try:
            data_frame = pd.DataFrame(self.error_log, index=[0])
            data_frame.to_csv(self.file_path, index=False)
        except Exception as e:
            self.error_log = dict(WriteError=str(e))
            data_frame = pd.DataFrame(self.error_log, index=[0])
            data_frame.to_csv(self.file_path, index=False)

    def download(self, remove_file=True):
        """
            Downloads the CSV file located at self.file_path and Downloaded named self.file_name
        :return:
        """
        try:
            file_path = open(self.file_path, mode='rb')
            result = StreamingHttpResponse(file_path, content_type='text/csv')
            result['Content-Disposition'] = 'attachment; filename={filename}'.format(filename=self.file_name)
            if remove_file:
                self.silent_remove()
            return result
        except Exception as e:
            self.error_log.update(dict(DownloadException=str(e)))
            return self.error_log

    def silent_remove(self):
        """
            Deletes the file
        :return:
        """
        try:
            os.remove(self.file_path)
        except Exception as e:
            print(e)
            self.error_log = dict(DeleteFileError=str(e))


class CSVReport(CSVReportGenerator):
    def __init__(self, file_name, file_path, headers=None, customized_headers=None):
        super().__init__(file_name=file_name, file_path=file_path, headers=headers,
                         customized_headers=customized_headers)

    def write(self, data=None, query=None):
        """
            Calls the parent class's write method to write the data to a csv
        :param data: Data to be written, format either [[], [], ...] or [{}, {}, ...]
        :param query:   Query to be written either raw sql or ORM
        :return:
        """

        if data or isinstance(data, list):
            self.write_from_data(data)
        elif query:
            self.write_from_query(query)
        else:
            pass
