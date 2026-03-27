import json, sys, boto3
from s3fs import S3FileSystem
import pandas as pd
from awsglue.utils import getResolvedOptions
from itertools import groupby
import splunk  # import the module from the s3 bucket
import pickle

# simplefilestorage to get data in s3 object in bytes
s3 = boto3.client('s3')
sfs = S3FileSystem()


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:store_auxrecords_postgres:" + account_id


def get_configs():
    args = getResolvedOptions(sys.argv,
                              ['s3_bucket', 's3_input_file', 's3_config_file', 's3_output_file', 'transaction_selector',
                               'csv_read_params', 'b1_file_header', 'default_value', 'client_id'])
    return args


class B1DataProcess:
    def __init__(self, params):
        self.s3_bucket = params['s3_bucket']
        self.input_file = params['s3_input_file']
        self.output_file = params['s3_output_file']
        self.config_file = params['s3_config_file']
        self.defvalue = params['default_value']
        self.client_id = params['client_id']
        self.transaction_selector = params['transaction_selector']
        self.csv_read_params = json.loads(params['csv_read_params'])
        self.b1_file_header = json.loads(params['b1_file_header'])
        self.message = ""
        self.index = 1

    def open_input_file(self):
        # get file information
        file_info = sfs.info(f'{self.s3_bucket}/{self.input_file}')
        if file_info['size'] == 0:
            raise ValueError(
                f"file open from s3 bucket failed with error: Input file {self.input_file} in s3 bucket {self.s3_bucket} is empty.")
        else:
            # Open input file
            input_file_uri = "s3://" + self.s3_bucket + "/" + self.input_file
            # Read the CSV file from S3 and extract only 'B1Card#' and 'Value' columns
            try:
                with sfs.open(input_file_uri, 'rb') as infile:
                    # input_list = pd.read_csv(infile, usecols=['B1Card#', 'Value'], dtype={'B1Card#': str, 'Value': str}, na_filter=False)
                    input_list = pd.read_csv(infile, usecols=self.csv_read_params['usecols'],
                                             dtype=self.csv_read_params['dtype'],
                                             na_filter=self.csv_read_params['na_filter'])
                    return input_list

            except FileNotFoundError as e:
                message = f"File open for the file {self.input_file} failed with s3 bucket {self.s3_bucket} not found error."
                splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message},
                                   get_run_id())
                raise Exception(message)

            except OSError as e:
                message = f"File open for the file {self.input_file} in s3 bucket {self.s3_bucket} failed with error: {str(e)}"
                splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message},
                                   get_run_id())
                raise Exception(message)

            except Exception as e:
                message = f"File open failed. {str(e)}"
                splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message},
                                   get_run_id())
                raise Exception(message)

    def open_config_file(self):
        # get file information
        config_file_info = sfs.info(f'{self.s3_bucket}/{self.config_file}')
        if config_file_info['size'] == 0:
            raise ValueError(
                f"config file open from s3 bucket failed with error: Config file {self.config_file} in s3 bucket {self.s3_bucket} is empty.")
        else:
            # Open input file
            config_file_uri = "s3://" + self.s3_bucket + "/" + self.config_file
            # Read the CSV file from S3 and extract only 'B1Card#' and 'Value' columns
            try:
                with sfs.open(config_file_uri, 'r') as configfile:
                    config_params = json.load(configfile)
                    self.b1_layout = config_params['b1_layout']
                    print(f"b1 layout is: {self.b1_layout}")

            except FileNotFoundError as e:
                message = f"File open for the file {self.config_file} failed with s3 bucket {self.s3_bucket} not found error."
                splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message},
                                   get_run_id())
                raise Exception(message)

            except OSError as e:
                message = f"File open for the file {self.config_file} in s3 bucket {self.s3_bucket} failed with error: {str(e)}"
                splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message},
                                   get_run_id())
                raise Exception(message)

            except Exception as e:
                message = f"File open failed. {str(e)}"
                splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message},
                                   get_run_id())
                raise Exception(message)

    def build_category_subsection(self, b1card, sectionname, subsections, inputparam):
        try:
            global out_list
            YorN_list = []
            # Iterate over each item in inputparam
            for value in inputparam:
                # Determine the result based on the value
                if value == "1":
                    result = "Yes"
                elif value == "0":
                    result = "No"
                else:
                    result = self.defvalue

                YorN_list.append(result)
            # Construct the output list
            for subsection in subsections:
                # category_output_list = []
                if subsection == "Categorization":
                    # category_output_list = [sectionname, b1card, subsection, list(set(YorN_list))]
                    self.construct_output_list([self.client_id, sectionname, b1card, subsection, list(set(YorN_list))])

        except Exception as e:
            message = f"Build failed for B1 card # {b1card}. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

    def build_NN_BB_format_subsection(self, b1card, sectionname, subsections, inputparam):
        try:
            category_sold_list = []
            category_bought_list = []
            global out_list
            # Iterate over each item in inputparam
            for value in inputparam:
                # output_list = []

                # Split NN and BB from the input parameter while retaining leading zeroes
                NN, BB = value.split('-')

                # Convert NN and BB to integers
                NN_int = int(NN)
                BB_int = int(BB)

                if NN_int == 0:
                    # Create lists for every subsection with category space
                    category_sold = self.defvalue
                    category_bought = self.defvalue

                elif 1 <= NN_int <= 25:
                    if BB_int == 0:
                        # Create lists for every subsection with Category {NN} for 'When Issued Sold', Category {NN} for 'When Issued Bought'
                        category_sold = NN
                        category_bought = NN
                        # Special case for b1card 503811
                        if b1card == 503811:
                            category_sold = NN
                            category_bought = BB
                    elif 1 <= BB_int <= 25:
                        # Create lists for every subsection with Category {NN} for 'When Issued Sold', Category {BB} for 'When Issued Bought'
                        category_sold = NN
                        category_bought = BB
                        # Special case for b1card 503811
                        if b1card == 503811:
                            category_sold = NN
                            category_bought = NN

                category_sold_list.append(category_sold)
                category_bought_list.append(category_bought)

            # Construct the output list
            for subsection in subsections:
                if subsection in ["When Issued Sold", "Securities Sold"]:
                    # list(set()) will eliminate duplicates in the list. If duplicates aren't to be eliminated, remove list(set())
                    # out_list.append(dict(zip(self.b1_layout['header_row'],[sectionname, b1card, subsection, category_sold_list])))
                    # out_list.append(dict(zip(self.db_params['column_names'],[sectionname, b1card, subsection, list(set(category_sold_list))])))
                    self.construct_output_list(
                        [self.client_id, sectionname, b1card, subsection, list(set(category_sold_list))])
                elif subsection in ["When Issued Bought", "Securities Bought"]:
                    # list(set()) will eliminate duplicates in the list. If duplicates aren't to be eliminated, remove list(set())
                    # out_list.append(dict(zip(self.b1_layout['header_row'],[sectionname, b1card, subsection, category_bought_list])))
                    # out_list.append(dict(zip(self.db_params['column_names'],[sectionname, b1card, subsection, list(set(category_bought_list))])))
                    self.construct_output_list(
                        [self.client_id, sectionname, b1card, subsection, list(set(category_bought_list))])

        except Exception as e:
            message = f"Build failed for B1 card # {b1card}. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

    def build_NN_format_subsection(self, b1card, sectionname, subsections, inputparam):
        try:
            global out_list
            NN_list = []
            # Iterate over each item in inputparam
            for value in inputparam:
                NN_int = int(value)
                # Determine the result based on the value
                if NN_int == 0:
                    NN_value = self.defvalue
                else:
                    NN_value = value
                if b1card == 503812:
                    print(f"Found it: {NN_value}")

                NN_list.append(NN_value)

            # Construct the output list
            for subsection in subsections:
                if subsection in ["Credit Interest", "Debit Interest", "Taxable Bond Interest",
                                  "Non Taxable Bond Interest", "Taxable Cash Dividend", "Non-Taxable Cash Dividend",
                                  "Stock Dividend BK", "Stock Dividend PS", "Security Activity BK",
                                  "Funds Paid And Received AllOrCredit", "Funds Paid And Received Debit", "Other",
                                  "External Assets", "Money Market Fund", "Mutual Fund"]:
                    # list(set()) will eliminate duplicates in the list. If duplicates aren't to be eliminated, remove list(set())
                    # out_list.append(dict(zip(self.b1_layout['header_row'],[sectionname, b1card, subsection, NN_list])))
                    # out_list.append(dict(zip(self.db_params['column_names'],[sectionname, b1card, subsection, list(set(NN_list))])))
                    self.construct_output_list([self.client_id, sectionname, b1card, subsection, list(set(NN_list))])
        except Exception as e:
            message = f"Build failed for B1 card # {b1card}. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

    def build_TT_SS_CC_P_Q_O_R_AA_XX_YY_format_subsection(self, b1card, sectionname, subsections, inputparam):
        try:
            checkList = []
            for value in inputparam:
                check = value.split("-")
                if len(check) >= 7:
                    if check[0] == "  " or check[0] == "":
                        raise Exception("Space is not allowed for trid")
                    checkList.append(check)
                else:
                    raise Exception("Check format is wrong")
            for subsection in subsections:
                self.construct_output_list([self.client_id, sectionname, b1card, subsection, checkList])
        except Exception as e:
            message = f"TT_SS_CC_P_Q_O_R_AA_XX_YY Build failed for B1 card # {b1card}. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

    def build_VALUE_AS_ARRAY_format_subsection(self, b1card, sectionname, subsections, inputparam, splitCharacter):
        try:
            arrayList = []
            for value in inputparam:
                arrayList.append(value.split(splitCharacter))
            for subsection in subsections:
                self.construct_output_list([self.client_id, sectionname, b1card, subsection, arrayList])
        except Exception as e:
            message = f"Build failed for B1 card # {b1card}. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

    def construct_output_list(self, output_list):
        try:
            global out_list

            # Extract column names
            # column_names = [column['name'] for column in self.db_params['columns']]
            # out_list.append(dict(zip(column_names,output_list)))
            out_list.append(dict(zip(self.b1_file_header, output_list)))
        except Exception as e:
            message = f"Output list construction failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

    def process_records(self, input_list):
        try:
            global out_list
            out_list = []
            # Combine headers and values
            b1_list = []
            selected_transactions = self.b1_layout.get(self.transaction_selector) + self.b1_layout.get('holdings_rows')
            for transaction in selected_transactions:
                b1_list.append(dict(zip(self.b1_layout['header_row'], transaction)))
            print(f"input_list:{input_list}")
            # self.write_log_file(input_list,"input_list.json")
            if not input_list.empty:
                # Merge input_list and b1_list
                grouped_input = input_list.groupby('B1Card#')['Value'].agg(list).reset_index()
                # print(f"grouped_input: {grouped_input}")

                merged_list = [{**item, 'inputValue':
                    grouped_input[grouped_input['B1Card#'] == item['B1Card#']]['Value'].values.tolist()[0] if
                    grouped_input[grouped_input['B1Card#'] == item['B1Card#']]['Value'].values.tolist() else ''} for
                               item in b1_list]
                print(f"Merged list is : {merged_list}")
                # self.write_log_file(merged_list,"merged_list.json")

            else:
                merged_list = []

            # print(f"merged_list: {json.dumps(selected_transaction_set, indent=4)}")

            for b1_item in merged_list:
                # Get the function dynamically based on the b1card
                b1card = b1_item["B1Card#"]
                sectionname = b1_item["SectionName"]
                subsections = b1_item["Subsection"]
                inputparam = b1_item["inputValue"]
                print("b1card is", b1card)
                if b1card in self.b1_layout['b1s_with_category_format']:
                    self.build_category_subsection(b1card, sectionname, subsections, inputparam)
                elif b1card in self.b1_layout['b1s_with_NN_BB_format']:
                    self.build_NN_BB_format_subsection(b1card, sectionname, subsections, inputparam)
                elif b1card in self.b1_layout['b1s_with_NN_format']:
                    self.build_NN_format_subsection(b1card, sectionname, subsections, inputparam)
                elif b1card in self.b1_layout['b1s_with_TT_SS_CC_P_Q_O_R_AA_XX_YY_format']:
                    self.build_TT_SS_CC_P_Q_O_R_AA_XX_YY_format_subsection(b1card, sectionname, subsections, inputparam)
                elif b1card in self.b1_layout['b1s_with_SS_TT_format'] or b1card in self.b1_layout[
                    'b1s_with_SSSSSSS_NN_B']:
                    self.build_VALUE_AS_ARRAY_format_subsection(b1card, sectionname, subsections, inputparam, "-")

            out_list1 = {"json_data": out_list}
            return out_list1

        except Exception as e:
            message = f"B1 layout creation failed. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

    def write_output_file(self, final_out_list):
        try:
            output_file_uri = "s3://" + self.output_file
            print(f"Writing output file to the path : {output_file_uri}")
            if not final_out_list:
                raise ValueError(
                    f"write operation to s3 bucket {self.s3_bucket} failed with error: Contents to output file {self.output_file} is empty.")
            else:
                with sfs.open(output_file_uri, 'w') as out_file:
                    out_file.write(json.dumps(final_out_list, indent=4))

        except FileNotFoundError as e:
            message = f"Write operation for the file {self.output_file} failed with s3 bucket {self.s3_bucket} not found error."
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

        except OSError as e:
            message = f"Write operation for the file {self.output_file} in s3 bucket {self.s3_bucket} failed with error: {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

        except Exception as e:
            message = f"Write operation for the file {self.output_file} in s3 bucket {self.s3_bucket} failed with error: {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

    def write_log_file(self, final_out_list, fileName):
        try:
            output_file_uri = "s3://" + self.s3_bucket + "/" + fileName

            print(f"Writing output file to the path : {output_file_uri}")
            with sfs.open(output_file_uri, 'wb') as out_file:
                out_file.write(json.dumps(final_out_list, indent=4))

        except FileNotFoundError as e:
            message = f"Write operation for the file {self.output_file} failed with s3 bucket {self.s3_bucket} not found error."
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

        except OSError as e:
            message = f"Write operation for the file {self.output_file} in s3 bucket {self.s3_bucket} failed with error: {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)

        except Exception as e:
            message = f"Write operation for the file {self.output_file} in s3 bucket {self.s3_bucket} failed with error: {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': self.input_file, 'Message': message}, get_run_id())
            raise Exception(message)


# main:
def main():
    try:
        iparm = get_configs()

    except Exception as e:
        message = f"failed to retrieve input parameters. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        dp = B1DataProcess(iparm)
        dp.open_config_file()
        input_list = dp.open_input_file()
        final_out_list = dp.process_records(input_list)
        print(f"Final_list: {json.dumps(final_out_list, indent=4)}")

    except Exception as e:
        message = f"B1 layout creation failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

    dp.write_output_file(final_out_list)


if __name__ == '__main__':
    main()