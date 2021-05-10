from analytics.analysis import Analysis
import datetime
import pandas as pd
import requests
import math
import re
from bs4 import BeautifulSoup


CLASS_NAME = "ElectricityCostCalculationAnalysis"
ANALYSIS_NAME = "electricity-cost-calculation"
REGIONS = ["RU-SVE", "RU-PER", "RU-BA", "RU-UD"]
RETAILERS = ["ORENBENE_PSVERDLE", "EKATSBKO_PEKELSK1", "ORENSES3_PROSKOM1", "OBTEPENG_PNUESBK1", "PERMENER_PPERMENE", "BASHKESK_PBASHENE", "ORENBENE_PUDMURTE"]
A_ARGS = {"analysis_code": "ELECTRICITYCOSTCALCULATION",
          "analysis_name": ANALYSIS_NAME,
          "input": "1 time series",
          "action": "Calculates the cost of electricity in 6 categories",
          "output": "Dataframe with costs (double)",
          "inputs_count": 1,
          "outputs_count": 1,
          "inputs_outputs_always_same_count": True,
          "parameters": [
              {"name": "method", "count": 1, "type": "SELECT", "options": ["one_category_*", "two_category_two_zones_*", "two_category_three_zones_*", "three_category_*", "four_category_*", "energy_storage", "peak_hours"],
               "info": "one_category - Calculation of cost for the first category"
                       "two_category_two_zones - Calculation of the cost of the second category, two zones"
                       "two_category_three_zones - Calculation of the cost of the second category, three zones"
                       "three_category - Calculation of cost for the third category"
                       "three_category_energy_storage - Calculation of cost for the third category with energy storage"
                       "three_category_energy_storage_effect - Calculation of the effect for the third category with energy storage" 
                       "four_category - Calculating the cost of the fourth category"
                       "four_category_energy_storage - Calculating the cost of the fourth category with energy storage"
                        "three_category_energy_storage_effect - Calculation of the effect for the third category with energy storage" 
                        "peak_hours - Combined maximum hours"
                        "energy_storage - New profile with energy_storage"
               },
              {"name": "mode_energy_storage", "count": 1, "type": "SELECT", "options": ["auto", "manual"],
               "info": "auto"
                       "manual - with time_return_energy_storage"
               },
              {"name": "data_type", "count": 1, "type": "SELECT", "options": ["current", "power"],
                "info": "current"
                       "power - default"
               },
               {"name": "capacity_energy_storage", "count": 1, "type": "FLOAT",
               "info": "Capacity, kW*h"
               },
              {"name": "region", "count": 1, "type": "SELECT", "options": REGIONS,
               "info": "Regions of Russia (ISO 3166-2): RU-SVE, RU-PER, RU-BA, RU-UD"},
              {"name": "retailer", "count": 1, "type": "SELECT", "options": RETAILERS,
               "info": "Sales companies listed on the atsenergo.ru website"},
              {"name": "start_peaks_date", "count": 1, "type": "STRING",
               "info": 'Start time for peak_hours. Example: "2019-02"'},
              {"name": "time_four_cat", "count": 1, "type": "DICT_OF_TIME",
               "info": 'Set of dictionaries containing time intervals. Example: "07:00:00,12:59:00", "14:00:00,21:59:00"'},
              {"name": "time_two_cat_night_zones", "count": 1, "type": "DICT_OF_TIME",
               "info": 'Set of dictionaries containing time intervals for second category. Night zone. Example: "07:00:00,12:59:00","14:00:00,17:59:00","19:00:00,21:59:00"'},
              {"name": "time_two_cat_peak_zones", "count": 1, "type": "DICT_OF_TIME",
               "info": 'Set of dictionaries containing time intervals for second category. Peak zone. Example: "07:00:00,12:59:00","14:00:00,21:59:00"'},
              {"name": "tariff_one_ee", "count": 1, "type": "FLOAT", "info": "Maximum level of unregulated prices (RUB/MWh): 1"},
              {"name": "tariff_two_two_zones_ee", "count": 2, "type": "FLOAT", "info": "Tariffs of the second category. 2 numbers: night, day. Example: 1.43, 4.87"},
              {"name": "tariff_two_three_zones_ee", "count": 3, "type": "FLOAT", "info": "Tariffs of the second category. 3 numbers: night, semi-peak, peak. Example: 1.43, 2.77, 7.85"},
              {"name": "tariff_losses_three", "count": 1, "type": "FLOAT", "info": "Rate for payment of technological losses, category: 1, 2, 3"},
              {"name": "tariff_maintenance_four", "count": 1, "type": "FLOAT", "info": "Tariff for maintenance of the power supply network, category: 4"},
              {"name": "tariff_losses_four", "count": 1, "type": "FLOAT", "info": "Rate for payment of technological losses, category: 4"},
              {"name": "tariff_sales", "count": 1, "type": "FLOAT", "info": "Rate for payment of technological losses, categories: 3, 4"},
              {"name": "actual_volume_other_service", "count": 1, "type": "FLOAT", "info": "Actual volume of electricity consumption by the guaranteeing supplier on the wholesale market, MW·h"},
              {"name": "volume_other_service", "count": 1, "type": "FLOAT", "info": "Volume of purchase of electric energy by the guaranteeing supplier from producers of electric energy (capacity) on retail markets, MW*h"},
              {"name": "power_return_energy_storage", "count": 1, "type": "FLOAT", "info": "Electric power storage discharge power, kW*h"},
              {"name": "power_charging_energy_storage", "count": 1, "type": "FLOAT", "info": "Electric power storage charge, kW*h"},
              {"name": "time_charging_energy_storage", "count": 1, "type": "DICT_OF_TIME",
               "info": 'Set of dictionaries containing time intervals. Example: "2019-12-01_00:00:00,2019-12-01_03:00:00","2019-12-02_00:00:00,2019-12-02_04:00:00"'},
              {"name": "time_return_energy_storage", "count": 1, "type": "DICT_OF_TIME",
               "info": 'Set of dictionaries containing time intervals. Example: "2019-12-01_07:00:00,2019-12-01_08:00:00","2019-12-02_00:05:00,2019-12-02_08:00:00"'},
          ]}

class ElectricityCostCalculationAnalysis(Analysis):

    def __init__(self):
        super().__init__()
        self.utc = 0
        self.logger.debug("Initialization")


    def analyze(self, parameters, data):
        """
        Do not modify this.
        This method implements analysis cycle.
        :return: analysis result represented as DF
        """
        try:
            p = self._parse_parameters(parameters)
            d = self._preprocess_df(data, p)
            res = self._analyze(p, d)
            # print(d)
            # pd.options.display.max_columns = 100
            # print("Результат")
            # print(res)
            return res
        except Exception as err:
            self.logger.error(err)
            raise Exception(str(err))

    def _preprocess_df(self, data, p):
        """
        Preprocesses DataFrame

        Fills NaN with 0s
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs
            if data is not None:
                if data.empty:
                    raise Exception("Empty DataFrame")
                dat = data.fillna(0.)
                new_data = dat.rename_axis('time').reset_index()
                new_data.columns = ['time', 'value']
                # new_data['time'] = new_data['time'].apply(lambda x:
                #                                           datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f+00:00'))
                new_data['time'] = new_data['time'].apply(lambda x:
                                                          self._clean_date(x))
                new_data['time'] = pd.to_datetime(new_data.time, format='%Y-%m-%d %H:%M:%S')

                if p['data_type'] == "current":
                    new_data = self._preprocess_df_current(new_data)
                else:
                    new_data = self._preprocess_df_power(new_data)
            else:
                raise Exception("DataFrame is None")
            self.logger.debug("DataFrame preprocessed")
            return new_data
        except Exception as err:
            self.logger.error("Failed to preprocess DataFrame: " + str(err))
            raise Exception("Failed to preprocess DataFrame: " + str(err))

    def _clean_date(self, x):
        try:
            first_match = re.search(r'\d{4}(-|\/)\d{2}(-|\/)\d{2} ([0-1]\d|2[0-3])(:[0-5]\d){2}', str(x))
            # first_match = re.search('2019', str(x))
            if first_match:
                return first_match.group()

        except Exception as err:
            self.logger.error("Failed to _clean_date: " + str(err))
            raise Exception("Failed to _clean_date: " + str(err))

    def _preprocess_df_current(self, data):
        """
        Convert DataFrame to Power/hour
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs
            if data is not None:
                if data.empty:
                    raise Exception("Empty DataFrame")
                # print(pd.to_datetime(new_data.time).dt.minute)
                ndf = data.groupby(pd.Grouper(key="time", freq="1H")).count()
                ndf.columns = ['count_val']
                ndf['sum_val'] = data.groupby(pd.Grouper(key="time", freq="1H")).sum()
                voltage = 230
                ndf['sum_power'] = ndf.sum_val * voltage/ndf.count_val
                ndf.reset_index(inplace=True)
                ndf.rename(columns={'sum_power': 'value', 'index': 'time'},
                                     inplace=True)
                return ndf[['time', 'value']]
            else:
                raise Exception("DataFrame is None")
            self.logger.debug("DataFrame preprocessed")
        except Exception as err:
            self.logger.error("Failed to preprocess DataFrame: " + str(err))
            raise Exception("Failed to preprocess DataFrame: " + str(err))

    def _preprocess_df_power(self, data):
        """
        Convert DataFrame to Power/hour
        """
        self.logger.debug("Preprocessing DataFrame")
        try:
            # Fill NaNs
            if data is not None:
                if data.empty:
                    raise Exception("Empty DataFrame")
                # print(pd.to_datetime(new_data.time).dt.minute)
                ndf = data.groupby(pd.Grouper(key="time", freq="1H")).count()
                ndf.columns = ['count_val']
                ndf['sum_val'] = data.groupby(pd.Grouper(key="time", freq="1H")).sum()
                ndf['sum_power'] = ndf.sum_val/ndf.count_val
                ndf.reset_index(inplace=True)
                ndf.rename(columns={'sum_power': 'value', 'index': 'time'},
                                     inplace=True)
                print(ndf)
                return ndf[['time', 'value']]
            else:
                raise Exception("DataFrame is None")
            self.logger.debug("DataFrame preprocessed")
        except Exception as err:
            self.logger.error("Failed to preprocess DataFrame: " + str(err))
            raise Exception("Failed to preprocess DataFrame: " + str(err))

    def _parse_links(self, dls):
        """
        Parse first link from the site table
        :param dls:
        :return: link without a domain
        """
        response = requests.get(dls)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')

        links = []
        for tr in table.findAll("tr"):
            trs = tr.findAll("td")
            for each in trs:
                try:
                    link = each.find('a')['href']
                    links.append(link)
                except:
                    pass
        return links[0]

    def _parse_parameters(self, parameters):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters")
        try:
            pn = {'method': self._check_method(parameters),
                  'data_type': self._check_data_type(parameters),
                    'region': self._check_region(parameters),
                    'retailer': self._check_retailer(parameters),
                    'all':parameters
                    }
            return pn
        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _parse_parameters_four_cat(self, p):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters for the fourth category")
        try:
            parameters = p['all']
            pn = dict()
            pn['time_four_cat'] = self._check_time_dict(parameters['time_four_cat'])
            pn['tariff_sales'] = self._check_float(parameters['tariff_sales'][0])
            pn['tariff_losses_four'] = self._check_float(parameters['tariff_losses_four'][0])
            pn['tariff_maintenance_four'] = self._check_float(parameters['tariff_maintenance_four'][0])
            pn['volume_other_service'] = self._check_float(parameters['volume_other_service'][0])
            pn['actual_volume_other_service'] = self._check_float(parameters['actual_volume_other_service'][0])
            return pn
        except Exception as err:
            self.logger.error("Error in _parse_parameters_four_cat: " + str(err))
            raise Exception("Error in _parse_parameters_four_cat: " + str(err))

    def _parse_parameters_peaks(self, p):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters for peaks")
        try:
            parameters = p['all']
            pn = dict()
            start_peaks_date = parameters['start_peaks_date'][0].split('-')
            pn['start_peaks_year'] = start_peaks_date[0]
            pn['start_peaks_month'] = start_peaks_date[1]
            return pn
        except Exception as err:
            self.logger.error("Error in _parse_parameters_peaks: " + str(err))
            raise Exception("Error in _parse_parameters_peaks: " + str(err))

    def _parse_parameters_three_cat(self, p):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters for the third category")
        try:
            parameters = p['all']
            pn = dict()
            pn['tariff_sales'] = self._check_float(parameters['tariff_sales'][0])
            pn['tariff_losses_three'] = self._check_float(parameters['tariff_losses_three'][0])
            pn['volume_other_service'] = self._check_float(parameters['volume_other_service'][0])
            pn['actual_volume_other_service'] = self._check_float(parameters['actual_volume_other_service'][0])
            return pn
        except Exception as err:
            self.logger.error("Error in _parse_parameters_three_cat: " + str(err))
            raise Exception("Error in _parse_parameters_three_cat: " + str(err))

    def _parse_parameters_one_cat(self, p):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters for the first category")
        try:
            parameters = p['all']
            pn = dict()
            pn['tariff_one_ee'] = self._check_float(parameters['tariff_one_ee'][0])
            pn['tariff_losses_three'] = self._check_float(parameters['tariff_losses_three'][0])
            return pn
        except Exception as err:
            self.logger.error("Error in _parse_parameters_one_cat: " + str(err))
            raise Exception("Error in _parse_parameters_one_cat: " + str(err))

    def _parse_parameters_two_cat(self, p, peak = False):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters for the second category")
        try:
            parameters = p['all']
            pn = dict()
            pn['tariff_losses_three'] = self._check_float(parameters['tariff_losses_three'][0])
            pn['time_two_cat_night_zones'] = self._generate_time_df(parameters['time_two_cat_night_zones'])
            if peak:
                pn['tariff_two_night'] = self._check_float(parameters['tariff_two_three_zones_ee'][0])
                pn['tariff_two_semipeak'] = self._check_float(parameters['tariff_two_three_zones_ee'][1])
                pn['tariff_two_peak'] = self._check_float(parameters['tariff_two_three_zones_ee'][2])
                pn['time_two_cat_peak_zones'] = self._generate_time_df(parameters['time_two_cat_peak_zones'])
            else:
                pn['tariff_two_night'] = self._check_float(parameters['tariff_two_two_zones_ee'][0])
                pn['tariff_two_day'] = self._check_float(parameters['tariff_two_two_zones_ee'][1])
            return pn
        except Exception as err:
            self.logger.error("Error in _parse_parameters_two_cat: " + str(err))
            raise Exception("Error in _parse_parameters_two_cat: " + str(err))

    def _parse_parameters_energy_storage(self, p, c):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters for the energy storage")
        try:
            parameters = p['all']
            pn = dict()
            pn['power_return_energy_storage'] = self._check_float(parameters['power_return_energy_storage'][0])
            pn['power_charging_energy_storage'] = self._check_float(parameters['power_charging_energy_storage'][0])
            pn['mode_energy_storage'] = parameters['mode_energy_storage'][0]
            if pn['mode_energy_storage'] == "auto":
                pn['capacity_energy_storage'] = parameters['capacity_energy_storage'][0]
            else:
                pn['time_return_energy_storage'] = self._generate_time_df(parameters['time_return_energy_storage'])
                pn['time_charging_energy_storage'] = self._generate_time_df(parameters['time_charging_energy_storage'])
            if c == 4:
                pn['time_four_cat'] = self._check_time_dict(parameters['time_four_cat'])
            if c == 2 or c == 20:
                pn['time_two_cat_night_zones'] = self._generate_time_df(parameters['time_two_cat_night_zones'])
            if c == 20:
                pn['time_two_cat_peak_zones'] = self._generate_time_df(parameters['time_two_cat_peak_zones'])
            return pn
        except Exception as err:
            self.logger.error("Error in _parse_parameters_energy_storage: " + str(err))
            raise Exception("Error in _parse_parameters_energy_storage: " + str(err))

    def _check_time_dict(self, times_dict):
        """
        Checks 'time dict' parameter
        """
        try:

            new_times_dict = []
            for time_v in times_dict:
                times_v_arr = time_v.split(',')
                this_time = {}
                this_time['start'] = datetime.datetime.strptime(times_v_arr[0], '%H:%M:%S').time()
                this_time['end'] = datetime.datetime.strptime(times_v_arr[1], '%H:%M:%S').time()
                new_times_dict.append(this_time)
            return new_times_dict
        except Exception as err:
            self.logger.debug("Error in dict of time: " + str(err))
            raise Exception("Error in dict of time: " + str(err))

    def _generate_time_df(self, times_dict):
        """
        Checks 'time dict' parameter
        """
        try:
            new_times_dict = pd.DataFrame()
            for time_v in times_dict:
                times_v_arr = time_v.split(',')
                start_time = pd.Timestamp(times_v_arr[0])
                end_time = pd.Timestamp(times_v_arr[1])
                df = pd.DataFrame(data={'time': pd.date_range(start=start_time,
                                     end=end_time, freq='1H')})
                new_times_dict = new_times_dict.append(df)

            return new_times_dict.reset_index(drop=True)
        except Exception as err:
            self.logger.debug("Error in the _generate_time_df: " + str(err))
            raise Exception("Error in the _generate_time_df: " + str(err))

    def _check_method(self, parameters):
        """
        Checks 'method' parameter
        """
        try:
            method = parameters['method'][0]
            # if method not in ["one_category", "two_category_two_zones", "two_category_three_zones", "three_category",
            #                   "three_category_energy_storage", "three_category_energy_storage_effect", "four_category",
            #                   "four_category_energy_storage", "four_category_energy_storage_effect", "peak_hours", "energy_storage"]:
            #     raise Exception
            #     self.logger.debug("Parsed parameter 'method': " + str(method))
            return method
        except Exception as err:
            self.logger.debug("Wrong parameter 'method': " + str(parameters['method']) + " " + str(err))
            raise Exception("Wrong parameter 'method': " + str(parameters['method']) + " " + str(err))

    def _check_data_type(self, parameters):
        """
        Checks 'method' parameter
        """
        try:
            data_type = parameters['data_type'][0]
            if data_type not in ["current", "power"]:
                data_type = "power"
            return data_type
        except Exception:
            return "power"

    def _check_float(self, x):
        """
        Checks 'float' parameter
        """
        try:
            x = float(x)
            return x
        except Exception as err:
            self.logger.debug("Error in parse float parameter" + str(err))
            raise Exception("Error in parse float parameter" + str(err))

    def _check_region(self, parameters):
        """
        Checks 'region' parameter
        """
        try:
            region = parameters['region'][0]
            if region not in REGIONS:
                raise Exception
            self.logger.debug("Parsed parameter 'region': " + str(region))
            return region
        except Exception as err:
            self.logger.debug("Wrong parameter 'region': " + str(parameters['region']) + " " + str(err))
            raise Exception("Wrong parameter 'region': " + str(parameters['region']) + " " + str(err))

    def _check_retailer(self, parameters):
        """
        Checks 'retailer' parameter
        """
        try:
            retailer = parameters['retailer'][0]
            if retailer not in RETAILERS:
                raise Exception
            self.logger.debug("Parsed parameter 'region': " + str(retailer))
            return retailer
        except Exception as err:
            self.logger.debug("Wrong parameter 'retailer': " + str(parameters['retailer']) + " " + str(err))
            raise Exception("Wrong parameter 'retailer': " + str(parameters['retailer']) + " " + str(err))

    def _dls_other_services(self, start_date):
        """
        Generates dls to other services
        :param start_date:
        :return: link
        """
        try:
            region_report = "eur"
            month_this = start_date.month
            year = year_prev = start_date.year
            if int(month_this) != 1:
                month_prev = "%02d" % (int(month_this) - 1)
            else:
                month_prev = '12'
                year_prev = year - 1

            year_prev = str(year_prev)

            dls = "https://www.atsenergo.ru/nreport?access=public&rname=FRSV_REESTR_INFRAORG_USLUGI_REGRF_ATS&region=" + region_report + "&rdate=" + year_prev + month_prev + "01"

            report_dls = "https://www.atsenergo.ru/nreport" + self._parse_links(dls)

            return report_dls

        except Exception as err:
            self.logger.error("Error in _dls_other_services: " + str(err))
            raise Exception("Error in _dls_other_services: " + str(err))

    def _dls_retail(self, start_date, p):
        """
        Generates dls to the retail
        :param start_date:
        :param p:
        :return: link
        """
        try:
            retailer = str(p['retailer'])
            region = str(p['region'])
            code = ''
            if (region == "RU-SVE"):
                self.utc = 2
                if (retailer == 'ORENBENE_PSVERDLE'):
                    code = 'ORENBENE_PSVERDLE'
                    gtp = 'gtp'
                elif (retailer == 'EKATSBKO_PEKELSK1'):
                    code = 'EKATSBKO_PEKELSK1'
                    gtp = 'gtp'
                elif (retailer == 'ORENSES3_PROSKOM1'):
                    code = 'ORENSES3_PROSKOM1'
                    gtp = 'gtp'
                elif (retailer == 'OBTEPENG_PNUESBK1'):
                    code = 'OBTEPENG_PNUESBK1'
                    gtp = 'gtp'
                else:
                    raise Exception("Wrong parameter 'retailer': " + retailer)
            elif (region == "RU-PER"):
                self.utc = 2
                if (retailer == 'PERMENER_PPERMENE'):
                    code = 'PERMENER_PPERMENE'
                    gtp = 'gtp'
                else:
                    raise Exception("Wrong parameter 'retailer': " + retailer)
            elif (region == "RU-BA"):
                self.utc = 0
                if (retailer == 'BASHKESK_PBASHENE'):
                    code = 'BASHKESK_PBASHENE'
                    gtp = 'gtp'
                else:
                    raise Exception("Wrong parameter 'retailer': " + retailer)
            elif (region == "RU-UD"):
                self.utc = 0
                if (retailer == 'ORENBENE_PUDMURTE'):
                    code = 'ORENBENE_PUDMURTE'
                    gtp = 'gtp'
                else:
                    raise Exception("Wrong parameter 'retailer': " + retailer)
            else:
                raise Exception("Wrong parameter 'region': " + region)
            month_this = start_date.month
            year = year_next = start_date.year
            if int(month_this) != 12:
                month_next = "%02d" % (int(month_this) + 1)
            else:
                month_next = '01'
                year_next = year + 1

            month_this = "%02d" % (int(month_this))

            year = str(year)
            year_next = str(year_next)

            return 'http://www.atsenergo.ru/dload/retail/' + year + month_this + '01/' + year_next + month_next + '10_' + code + '_' + month_this + year + '_' + gtp + '_1st_stage.xls'

        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _dls_calcfacthour(self, start_date, p):
        """
        Generates dls to the calcfacthour
        :param start_date:
        :param p:
        :return: link
        """
        try:
            retailer = str(p['retailer'])
            region = str(p['region'])
            code = ''
            if (region == "RU-SVE"):
                if (retailer == 'ORENBENE_PSVERDLE'):
                    code = 'ORENBENE_65'
                if (retailer == 'EKATSBKO_PEKELSK1'):
                    code = 'EKATSBKO_65'
                if (retailer == 'ORENSES3_PROSKOM1'):
                    code = 'ORENSES3_65'
                if (retailer == 'OBTEPENG_PNUESBK1'):
                    code = 'OBTEPENG_65'
            if (region == "RU-PER"):
                if (retailer == 'PERMENER_PPERMENE'):
                    code = 'PERMENER_57'
            if (region == "RU-BA"):
                if (retailer == 'BASHKESK_PBASHENE'):
                    code = 'BASHKESK_80'
            if (region == "RU-UD"):
                if (retailer == 'ORENBENE_PUDMURTE'):
                    code = 'ORENBENE_94'

            month_this = start_date.month
            year = start_date.year

            month_this = "%02d" % (int(month_this))

            year = str(year)

            return "http://www.atsenergo.ru/dload/calcfacthour_regions/" + year + month_this + '_' + code + "_calcfacthour.xls"

        except Exception as err:
            self.logger.error("Impossible to parse parameter: " + str(err))
            raise Exception("Impossible to parse parameter: " + str(err))

    def _parse_xls(self, dls):
        """
        Downloading excel file
        :param dls:
        :return: Pandas DataFrame
        """
        try:
            df = pd.read_excel(dls, converters={'Unnamed: 1': str})
            return df
        except Exception as err:
            self.logger.error("Error in _parse_xls: " + str(err))
            raise Exception("Error in _parse_xls: " + str(err))

    def _get_tariff_other_services(self, df, p, pn):
        """
        Gets a tariff for other services
        :param df: Data from Excel
        :param p: Main parameters
        :param pn: Advanced parameters
        :return: tariff_other (float)
        """
        try:
            retailer = str(p['retailer'])
            region = str(p['region'])
            volume_other_service = pn['volume_other_service']
            actual_volume_other_service = pn['actual_volume_other_service']
            rus_retailer = ''
            rus_region = ''
            if (region == "RU-SVE"):
                rus_region = 'Свердловская область'
                if (retailer == 'ORENBENE_PSVERDLE'):
                    rus_retailer = 'АО "ЭнергосбыТ Плюс"'
                if (retailer == 'EKATSBKO_PEKELSK1'):
                    rus_retailer = 'АО "ЕЭнС" (г.Екатеринбург)'
                if (retailer == 'ORENSES3_PROSKOM1'):
                    rus_retailer = 'АО "НТЭСК"'
                if (retailer == 'OBTEPENG_PNUESBK1'):
                    rus_retailer = 'АО "ОТЭК"'
            if (region == "RU-PER"):
                rus_region = 'Пермский край'
                if (retailer == 'PERMENER_PPERMENE'):
                    rus_retailer = 'ПАО "Пермэнергосбыт"'
            if (region == "RU-BA"):
                rus_region = 'Республика Башкортостан'
                if (retailer == 'BASHKESK_PBASHENE'):
                    rus_retailer = 'ООО "ЭСКБ"'
            if (region == "RU-UD"):
                rus_region = 'Удмуртская Республика'
                if (retailer == 'ORENBENE_PUDMURTE'):
                    rus_retailer = 'АО "ЭнергосбыТ Плюс"'
            df.columns = ['retailer', 'region', 'SO', 'CFR', 'ATS', 'date']
            index_start = df.loc[(df['retailer'] == rus_retailer) & (df['region'] == rus_region) & (df['SO'] != '-') & (df['CFR'] != '-') & (df['ATS'] != '-')].index[0]
            try:
                SO = float((df.loc[df.index == index_start]['SO'].iloc[0]).replace(',', '.').replace(' ', ''))
            except:
                SO = 0
            try:
                CFR = float((df.loc[df.index == index_start]['CFR'].iloc[0]).replace(',', '.').replace(' ', ''))
            except:
                CFR = 0
            try:
                ATS = float((df.loc[df.index == index_start]['ATS'].iloc[0]).replace(',', '.').replace(' ', ''))
            except:
                ATS = 0
            tariff_other = round((SO+CFR+ATS)/(actual_volume_other_service - volume_other_service)/1000, 5)
            return tariff_other
        except Exception as err:
            self.logger.error("Error in _get_tariff_other_services: " + str(err))
            raise Exception("Error in _get_tariff_other_services: " + str(err))

    def _get_peaks(self, peak_hours_file):
        """
        Gets peak hours
        :param peak_hours_file: Data from Excel
        :return peaks (DataFrame): time is datetime column
        """
        try:
            index_start_peak = 7
            filter_peak_hours_start = peak_hours_file.index >= index_start_peak
            filter_peak_hours_end = peak_hours_file.iloc[:, 0].isnull() != True
            peak_hours = peak_hours_file.loc[filter_peak_hours_start & filter_peak_hours_end]
            peak_hours.columns = ['day', 'hour']
            peaks_time = pd.DataFrame(pd.to_datetime(peak_hours['day'] + (peak_hours['hour']), format='%d.%m.%Y%H'), columns=['time'])
            return peaks_time.reset_index(drop=True)
        except Exception as err:
            self.logger.error("Error in get peaks: " + str(err))
            raise Exception("Error in get peaks: " + str(err))

    def _get_hourly_prices(self, df):
        """
        Gets hourly prices
        :param df: Data from Excel
        :return hourly_prices (DataFrame): time is a datetime column, value is a float column
        """
        try:
            index_start = df.loc[df['Unnamed: 0'] == 'Дифференцированные по зонам суток расчетного периода средневзвешенные нерегулируемые цены на электрическую энергию (мощность) на оптовом рынке и средневзвешенные нерегулируемые цены на электрическую энергию на оптовом рынке, определяемые для соответствующих зон суток, руб/МВтч'].index[0]
            filter_hourly_prices_start = df.index >= index_start + 32
            filter_hourly_prices_end = df['Unnamed: 5'].isnull() != True
            hourly_prices = df.loc[filter_hourly_prices_start & filter_hourly_prices_end][
                ['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 5']]
            hourly_prices.rename(columns={'Unnamed: 0': 'days', 'Unnamed: 1': 'hours', 'Unnamed: 5': 'value'}, inplace=True)
            hourly_prices['time'] = pd.to_datetime(hourly_prices['days'] + (hourly_prices['hours']), format='%d.%m.%Y%H')
            hourly_prices.drop(['days', 'hours'], axis='columns', inplace=True)
            hourly_prices.time = hourly_prices.time - datetime.timedelta(hours=self.utc)
            hourly_prices.drop(hourly_prices.index[:self.utc], inplace=True)

            for i in range(int(self.utc)):
                t = hourly_prices.loc[hourly_prices.index[-1], 'time']
                t = t + datetime.timedelta(hours=1)
                val = hourly_prices.loc[hourly_prices.index[-24], 'value']
                add = pd.DataFrame(data = {'value':[val], 'time':[t]})
                hourly_prices = hourly_prices.append(add)
            return hourly_prices.reset_index(drop=True)
        except Exception as err:
                    self.logger.error("Error in get hourly prices: " + str(err))
                    raise Exception("Error in get hourly prices: " + str(err))

    def _get_tariff (self, df):
        """
        Gets a power tariff
        :param df: Data from Excel
        :return: tariff_power (float)
        """
        try:
            index_start = df.loc[df['Unnamed: 0'] == 'Дифференцированные по зонам суток расчетного периода средневзвешенные нерегулируемые цены на электрическую энергию (мощность) на оптовом рынке и средневзвешенные нерегулируемые цены на электрическую энергию на оптовом рынке, определяемые для соответствующих зон суток, руб/МВтч'].index[0]
            tariff_power = df.loc[df.index == index_start + 15]['Unnamed: 1'].iloc[0]
            tariff_power = float(tariff_power.replace(',', '.'))
            return tariff_power
        except Exception as err:
            self.logger.error("Incorrect tariff power: " + str(err))
            raise Exception("Incorrect tariff power: " + str(err))

    def _calculate_ee(self, all_data, price):
        """
        Calculates the cost of electricity
        :param all_data:
        :param price:
        :return: float result
        """
        try:
            return (all_data.value*price.value/1000).sum()
        except Exception as err:
            self.logger.error("Error in _calculate_ee: " + str(err))
            raise Exception("Error in _calculate_ee: " + str(err))

    def _sum_peak(self, all_data, peaks, tariff_power):
        """
        Calculates the cost of power
        :param all_data:
        :param peaks:
        :param tariff_power:
        :return:
        """
        try:
            td_deltas = peaks
            count = pd.merge(td_deltas, all_data, how='inner', on=['time']).value.count()
            sum = (pd.merge(td_deltas, all_data, how='inner', on=['time'])).value.sum()
            if count != 0:
                return (sum/count/1000*tariff_power)
            return 0
        except Exception as err:
            self.logger.error("Error in _sum_peak: " + str(err))
            raise Exception("Error in _sum_peak: " + str(err))

    def _multiplication_data_tariff(self, all_data, tariff):
        """
        Multiplies the amount of kW by the tariff
        :param all_data:
        :param tariff:
        :return: float result
        """
        try:
            return all_data.value.sum()*tariff
        except Exception as err:
            self.logger.error("Error in _multiplication_data_tariff: " + str(err))
            raise Exception("Error in _multiplication_data_tariff: " + str(err))

    def _search_max_maintenance(self, all_data, time_zone, day):
        two_max = max = 0
        time_max = ''
        time_two_max = ''
        try:
            for index, row in all_data.iterrows():
                if row.time.day == day:
                    for time in time_zone:
                        time_value = datetime.time(hour=row['time'].hour, minute=row['time'].minute)
                        if time['start'] <= time_value < time['end']:
                            if max < row.value:
                                max = row['value']
                                time_max = row.time
                            if two_max < row.value and row.value != max:
                                two_max = row['value']
                                time_two_max = row.time
                    # if row.time.hour == 23:
                    #     sum += max
                    #     max = 0
                    #     count += 1
        except Exception as err:
            self.logger.error("Error in _multiplication_data_tariff: " + str(err))
            raise Exception("Error in _multiplication_data_tariff: " + str(err))

        dm = {'time': time_max, 'max': max, 'time_two_max': time_two_max, 'two_max': two_max}
        return dm

    def _maintenance(self, all_data, time_zone, tariff_maintenance):
        """
        Calculates the cost of maintenance (4 price categories)
        :param all_data:
        :param time_zone:
        :param tariff_maintenance:
        :return:
        """
        try:
            sum = 0
            count = 0
            max = 0
            for index, row in all_data.iterrows():
                if row.time.weekday() < 5:
                    for time in time_zone:
                        time_value = datetime.time(hour=row['time'].hour, minute=row['time'].minute)
                        print(row.value)
                        if time['start'] <= time_value < time['end']:
                            try:
                                if max < row.value: max = row.value
                            except Exception as err:
                                pass
                    if row.time.hour == 23:
                        sum += max
                        max = 0
                        count += 1
            if count != 0:
                return sum / count / 1000 * tariff_maintenance

            return 0
        except Exception as err:
            self.logger.error("Error in _maintenance: " + str(err))
            raise Exception("Error in _maintenance: " + str(err))

    def _last_day_of_month(self, any_day):
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
        return next_month - datetime.timedelta(days=next_month.day)

    def _peaks_row (self, p):
        """
        Output of combined maximum hours
        :param p:
        :param d:
        :return: dataframe with hours and peaks
        """
        try:

            try:
                pn = self._parse_parameters_peaks(p)
                date_new_format = pn['start_peaks_year'] + "-" + pn['start_peaks_month']
                start_date = datetime.datetime.strptime(date_new_format, '%Y-%m')
            except Exception as err:
                self.logger.error("Error in the start date: " + str(err))
                raise Exception("Error in the start date: " + str(err))

            peaks = self._get_peaks(self._parse_xls(self._dls_calcfacthour(start_date, p)))
            df_data = pd.DataFrame(index=pd.date_range(start=start_date, freq ='1H', end=self._last_day_of_month(start_date)))
            df_data['val_peak'] = (df_data.index.isin(peaks['time'])).astype(float)
            return df_data

        except Exception as err:
            self.logger.error("Error in the _peaks_row: " + str(err))
            raise Exception("Error in the _peaks_row: " + str(err))

    def _offset_power_profile(self, p, d, c):
        """
        Output of combined maximum hours
        :param p:
        :param d:
        :return: dataframe with times and value
        """
        try:
            pn = self._parse_parameters_energy_storage(p, c)
            if pn['mode_energy_storage'] == "auto":
                return_storage = self._peaks_row(p)
                return_storage = return_storage.loc[return_storage.val_peak == 1]
                return_storage.reset_index(inplace=True)
                return_storage.columns = ['time', 'val_peak']
                return_storage['valuem'] = pn['power_return_energy_storage']
                rm = pd.merge(d, return_storage, how='outer', on=['time'])
                rm = rm.fillna(0)
            else:
                return_storage = pn['time_return_energy_storage']
                return_storage['valuem'] = pn['power_return_energy_storage']
                charging_storage = pn['time_charging_energy_storage']
                charging_storage['valuep'] = pn['power_charging_energy_storage']
                rm = pd.merge(d, return_storage, how='outer', on=['time'])
                rm = pd.merge(rm, charging_storage, how='outer', on=['time'])
                rm = rm.fillna(0)
                rm['value'] = rm['value'] - rm['valuem']
                rm['value'] = rm['value'] + rm['valuep']
            new_rm = pd.DataFrame(columns=['day', 'delta', 'hours_charging'])

            if pn['mode_energy_storage'] == "auto":

                if c == 2:
                    d_night = pn['time_two_cat_night_zones']
                    d_all = d
                    d_all['hour'] = pd.to_datetime(d_all['time']).dt.hour
                    d_night['hour'] = pd.to_datetime(d_night['time']).dt.hour
                    count_days = d_all['time'].count()/24
                    rm_night = d_all.query("hour in @d_night.hour")
                    rm_day = d_all.query("hour not in @d_night.hour")
                    summ_kw = d.value.sum()
                    count_night = rm_night['value'].count()
                    count_day = rm_day['value'].count()
                    minus_day = pn['capacity_energy_storage']*count_days / count_day
                    plus_day = pn['capacity_energy_storage']*count_days / count_day
                    pd.options.mode.chained_assignment = None  # default='warn'
                    if (minus_day <= pn['power_return_energy_storage']):
                        rm_day.loc[rm_day["value"] - minus_day >= 0, "value"] = rm_day.loc[rm_day["value"] - minus_day >= 0, "value"] - minus_day
                        rm_day.loc[rm_day["value"] - minus_day < 0, "value"] = 0
                    else:
                        rm_day["value"] = rm_day["value"] - pn['power_return_energy_storage']
                    if (plus_day <= pn['power_return_energy_storage']):
                        rm_night["value"] = rm_night["value"] + plus_day
                    else:
                        rm_night["value"] = rm_night["value"] + pn['power_return_energy_storage']
                    # rm = rm_day.merge(rm_night)
                    rm = pd.concat([rm_day, rm_night], ignore_index=True)
                    rm = rm.sort_values(by='time')
                    # print(rm_day[10:60])
                    rm = rm.drop(['hour'], axis=1)
                    rm = rm.reset_index()
                    rm = rm.drop(['index'], axis=1)
                    # print(rm[24:75])

                if c == 20:
                    d_peak = pn['time_two_cat_peak_zones']
                    d_night = pn['time_two_cat_night_zones']
                    d_all = d
                    d_all['hour'] = pd.to_datetime(d_all['time']).dt.hour
                    d_night['hour'] = pd.to_datetime(d_night['time']).dt.hour
                    d_peak['hour'] = pd.to_datetime(d_peak['time']).dt.hour
                    count_days = d_all['time'].count()/24
                    rm_night = d_all.query("hour in @d_night.hour")
                    rm_peak = d_all.query("hour in @d_peak.hour")
                    print(rm_peak)
                    count_night = rm_night['value'].count()
                    count_peak = rm_peak['value'].count()
                    minus_day = pn['capacity_energy_storage']*count_days / count_peak
                    plus_day = pn['capacity_energy_storage']*count_days / count_peak
                    pd.options.mode.chained_assignment = None  # default='warn'
                    if (minus_day <= pn['power_return_energy_storage']):
                        rm_peak.loc[rm_peak["value"] - minus_day >= 0, "value"] = rm_peak.loc[rm_peak["value"] - minus_day >= 0, "value"] - minus_day
                        rm_peak.loc[rm_peak["value"] - minus_day < 0, "value"] = 0
                    else:
                        rm_peak["value"] = rm_peak["value"] - pn['power_return_energy_storage']
                    if (plus_day <= pn['power_return_energy_storage']):
                        rm_night["value"] = rm_night["value"] + plus_day
                    else:
                        rm_night["value"] = rm_night["value"] + pn['power_return_energy_storage']
                    rm = pd.concat([rm_peak, rm_night], ignore_index=True)
                    rm = rm.sort_values(by='time')
                    # print(rm_day[10:60])
                    rm = rm.drop(['hour'], axis=1)
                    rm = rm.reset_index()
                    rm = rm.drop(['index'], axis=1)
                    print(rm[30:60])

                if c == 3 or c == 4:
                    for index, row in rm.iterrows():
                        if row['valuem'] > 0:
                            deltar = list
                            day = row['time'].day
                            if row['value'] > row['valuem']:
                                rm.loc[rm['time'] == row['time'], 'value'] = row['value'] - row['valuem']
                                hours = math.ceil(pn['power_return_energy_storage'] / pn['power_charging_energy_storage'])
                                deltar = {"day": day, "delta": pn['power_return_energy_storage'], "hours_charging": hours}
                            else:
                                delta = row['value']
                                # rm['value'].iloc[index] = 0
                                #rm['delta'].where(rm['time'].day == day, inplace = True) = delta
                                # rm.loc[rm['time'].day() == day]['delta'] = delta
                                rm.loc[pd.to_datetime(rm['time']).dt.day == day, 'delta'] = delta
                                rm.loc[rm['time'] == row['time'], 'value'] = 0
                                hours = math.ceil(delta / pn['power_charging_energy_storage'])
                                deltar = {"day": day, "delta": delta, "hours_charging": hours}
                    # rm['value'] = rm['value'] - rm['valuem']
                    #         df = pd.DataFrame(data={'time': pd.date_range(start=start_time,
                    #                                                       end=end_time, freq='1H')})
                            new_rm = new_rm.append(deltar, ignore_index=True)

                #С зарядкой
                if c == 3:
                    for index, row in rm.iterrows():
                         for new_index, new_row in new_rm.iterrows():
                             if row['time'].day == new_row['day'] and row['time'].hour == 0.0:
                                 intFor = int(new_row['hours_charging'])
                                 for i in range(intFor):
                                     rm.loc[(rm['time'].dt.hour == (row['time'].hour+i)) & (rm['time'].dt.day == row['time'].day), 'value'] = row['value'] + (new_row['delta']/new_row['hours_charging'])

                    # rm['value'] = rm['value'] - rm['valuem']
                    #         df = pd.DataFrame(data={'time': pd.date_range(start=start_time,
                    #                                                       end=end_time, freq='1H')})
                if c == 4:
                    for new_index, new_row in new_rm.iterrows():
                        time_zone = pn['time_four_cat']
                        delta_k = new_row['delta']
                        while delta_k>0:
                            one_max = self._search_max_maintenance(rm, time_zone, new_row['day'])['max']
                            two_max = self._search_max_maintenance(rm, time_zone, new_row['day'])['two_max']
                            count_max = rm.loc[rm['value'] == one_max, 'value'].count()
                            # delta_k = delta_k - 1000
                            if (delta_k - ((one_max-two_max)*count_max)) > 0:
                                delta_k = delta_k - ((one_max-two_max)*count_max)
                                if two_max != '' and two_max > 0:
                                    rm.loc[(rm['value'] == one_max) & (rm['time'].dt.day == new_row['day']), 'value'] = two_max
                            else:
                                if (one_max-delta_k/count_max) >= 0:
                                    rm.loc[(rm['value'] == one_max) & (rm['time'].dt.day == new_row['day']), 'value'] = round(one_max-delta_k/count_max, 9)
                                delta_k = 0
                    for index, row in rm.iterrows():
                         for new_index, new_row in new_rm.iterrows():
                             if row['time'].day == new_row['day'] and row['time'].hour == 0.0:
                                 hours_charging = math.ceil(pn['capacity_energy_storage']/pn['power_charging_energy_storage'])
                                 intFor = int(hours_charging)
                                 for i in range(intFor):
                                     rm.loc[(rm['time'].dt.hour == (row['time'].hour+i)) & (rm['time'].dt.day == row['time'].day), 'value'] = row['value'] + pn['power_charging_energy_storage']
                    # rm['value'] = rm['value'] + rm['valuep']

            try:
                if pn['mode_energy_storage'] == "auto" and (c == 3 or c == 4):
                    rm = rm.drop(['delta', 'val_peak', 'valuem'], axis=1)
                if pn['mode_energy_storage'] != "auto":
                    rm = rm.drop(['valuem', 'valuep'], axis=1)
            except Exception as err:
                self.logger.error("Error in the _offset_power_profile: " + str(err))
                raise Exception("Error in the _offset_power_profile: " + str(err))
            # d.to_csv('d.csv', index=False)
            #rm_csv = rm.drop(['time'], axis=1)
            rm.to_csv('2offset_power_profile_'+str(c)+'.csv', index=False)
            # new_rm.to_csv('new_rm.csv', index=False)

            return rm

        except Exception as err:
            self.logger.error("Error in the _offset_power_profile: " + str(err))
            raise Exception("Error in the _offset_power_profile: " + str(err))

    def _three_category(self, p, d, type_val):
        """
        Calculates the total cost for 3 price categories
        :param p:
        :param p:
        :param d:
        :return df: DataFrame with result
        """
        try:
            try:
                start_date = d.loc[d.index[0], 'time']
            except Exception as err:
                self.logger.error("Error in the start date: " + str(err))
                raise Exception("Error in the start date: " + str(err))

            pn = self._parse_parameters_three_cat(p)
            tariff_sales_three = pn['tariff_sales']
            tariff_losses = pn['tariff_losses_three']
            xls = self._parse_xls(self._dls_other_services(start_date))
            tariff_other_services = self._get_tariff_other_services(xls, p, pn)
            xls = self._parse_xls(self._dls_retail(start_date, p))
            price_three_cat = self._get_hourly_prices(xls)
            tariff_power = self._get_tariff(xls)
            peaks = self._get_peaks(self._parse_xls(self._dls_calcfacthour(start_date, p)))
            df_data = {}
            if type_val == 'val_summ_kw':
                summ_kw = d.value.sum()
                df_data = {'val_summ_kw': [summ_kw]}
            if type_val == 'val_transfer':
                transfer = self._multiplication_data_tariff(d, tariff_losses)
                df_data = {'val_transfer': [transfer]}
            if type_val == 'val_calculate_ee':
                calculate_ee = self._calculate_ee(d, price_three_cat)
                df_data = {'val_calculate_ee': [calculate_ee]}
            if type_val == 'val_sales_add':
                sales_add = self._multiplication_data_tariff(d, tariff_sales_three)
                df_data = {'val_sales_add': [sales_add]}
            if type_val == 'val_power_cost':
                power_cost = self._sum_peak(d, peaks, tariff_power)
                df_data = {'val_power_cost': [power_cost]}
            if type_val == 'val_other_services':
                other_services = self._multiplication_data_tariff(d, tariff_other_services)
                df_data = {'val_other_services': [other_services]}
            if type_val == 'val_total':
                total = self._calculate_ee(d, price_three_cat) + self._sum_peak(d, peaks,
                                                                                tariff_power) + self._multiplication_data_tariff(
                    d, tariff_losses) + self._multiplication_data_tariff(d,
                                                                         tariff_sales_three) + self._multiplication_data_tariff(
                    d, tariff_other_services)
                df_data = {'val_total': [total]}
            df = pd.DataFrame(data=df_data, index=pd.date_range(start=start_date, end=start_date))
            df = df.astype(float)
            return df

        except Exception as err:
            self.logger.error("Error in the _three_category: " + str(err))
            raise Exception("Error in the _three_category: " + str(err))

    def _four_category(self, p, d, type_val):
        """
        Calculates the total cost for 4 price categories
        :param p:
        :param d:
        :return df: DataFrame with result
        """
        try:
            try:
                start_date = d.loc[d.index[0], 'time']
            except Exception as err:
                self.logger.error("Error in the start date: " + str(err))
                raise Exception("Error in the start date: " + str(err))

            pn = self._parse_parameters_four_cat(p)

            xls = self._parse_xls(self._dls_other_services(start_date))
            tariff_other_services = self._get_tariff_other_services(xls, p, pn)
            tariff_sales_four = pn['tariff_sales']
            tariff_losses = pn['tariff_losses_four'] / 1000
            time_zone = pn['time_four_cat']
            tariff_maintenance_four = pn['tariff_maintenance_four']
            xls = self._parse_xls(self._dls_retail(start_date, p))
            price_three_cat = self._get_hourly_prices(xls)
            tariff_power = self._get_tariff(xls)
            peaks = self._get_peaks(self._parse_xls(self._dls_calcfacthour(start_date, p)))
            df_data = {}
            if type_val == 'val_summ_kw':
                summ_kw = d.value.sum()
                df_data = {'val_summ_kw': [summ_kw]}
            if type_val == 'val_transfer':
                transfer = self._multiplication_data_tariff(d, tariff_losses)
                df_data = {'val_transfer': [transfer]}
            if type_val == 'val_calculate_ee':
                calculate_ee = self._calculate_ee(d, price_three_cat)
                df_data = {'val_calculate_ee': [calculate_ee]}
            if type_val == 'val_sales_add':
                sales_add = self._multiplication_data_tariff(d, tariff_sales_four)
                df_data = {'val_sales_add': [sales_add]}
            if type_val == 'val_power_cost':
                power_cost = self._sum_peak(d, peaks, tariff_power)
                df_data = {'val_power_cost': [power_cost]}
            if type_val == 'val_maintenance':
                maintenance = self._maintenance(d, time_zone, tariff_maintenance_four)
                df_data = {'val_maintenance': [maintenance]}
            if type_val == 'val_other_services':
                other_services = self._multiplication_data_tariff(d, tariff_other_services)
                df_data = {'val_other_services': [other_services]}
            if type_val == 'val_total':
                total = self._calculate_ee(d, price_three_cat) + self._sum_peak(d, peaks,
                                                                                tariff_power) + self._maintenance(d,
                                                                                                                  time_zone,
                                                                                                                  tariff_maintenance_four) + self._multiplication_data_tariff(
                    d, tariff_losses) + self._multiplication_data_tariff(d,
                                                                         tariff_sales_four) + self._multiplication_data_tariff(
                    d, tariff_other_services)
                df_data = {'val_total': [total]}
            df = pd.DataFrame(data=df_data, index=pd.date_range(start=start_date, end=start_date))
            df = df.astype(float)
            return df

        except Exception as err:
            self.logger.error("Error in the _four_category: " + str(err))
            raise Exception("Error in the _four_category: " + str(err))

    def _one_category(self, p, d, type_val):
        """
        Calculates the total cost for 1 price categories
        :param p:
        :param d:
        :return df: DataFrame with result
        """
        try:
            try:
                start_date = d.loc[d.index[0], 'time']
            except Exception as err:
                self.logger.error("Error in the start date: " + str(err))
                raise Exception("Error in the start date: " + str(err))
            pn = self._parse_parameters_one_cat(p)
            tariff_one_ee = pn['tariff_one_ee']
            tariff_losses_three = pn['tariff_losses_three']
            df_data = {}
            if type_val == 'val_summ_kw':
                summ_kw = d.value.sum()
                df_data = {'val_summ_kw': [summ_kw]}
            if type_val == 'val_transfer':
                transfer = self._multiplication_data_tariff(d, tariff_losses_three)
                df_data = {'val_transfer': [transfer]}
            if type_val == 'val_calculate_ee':
                calculate_ee = self._multiplication_data_tariff(d, tariff_one_ee)
                df_data = {'val_calculate_ee': [calculate_ee]}
            if type_val == 'val_total':
                transfer = self._multiplication_data_tariff(d, tariff_losses_three)
                calculate_ee = self._multiplication_data_tariff(d, tariff_one_ee)
                total = transfer + calculate_ee
                df_data = {'val_total': [total]}
            df = pd.DataFrame(data=df_data, index=pd.date_range(start=start_date, end=start_date))
            df = df.astype(float)
            return df

        except Exception as err:
            self.logger.error("Error in the _one_category: " + str(err))
            raise Exception("Error in the _one_category: " + str(err))

    def _two_category(self, p, d, type_val, peak = False):
        """
        Calculates the total cost for 2 price categories
        :param p:
        :param d:
        :return df: DataFrame with result
        """
        try:
            try:
                start_date = d.loc[d.index[0], 'time']
            except Exception as err:
                self.logger.error("Error in the start date: " + str(err))
                raise Exception("Error in the start date: " + str(err))
            pn = self._parse_parameters_two_cat(p, peak)
            tariff_two_night = pn['tariff_two_night']
            tariff_losses_three = pn['tariff_losses_three']
            transfer = self._multiplication_data_tariff(d, tariff_losses_three)
            d2 = pn['time_two_cat_night_zones']
            d['hour'] = pd.to_datetime(d['time']).dt.hour
            d2['hour'] = pd.to_datetime(d2['time']).dt.hour
            r2 = pd.merge(d, d2, how='inner', on=['hour'])
            summ_kw = d.value.sum()
            summ_kw_night = r2['value'].sum()
            calculate_ee_night = summ_kw_night*tariff_two_night
            df_data = {}
            if peak:
                tariff_two_peak = pn['tariff_two_peak']
                tariff_two_semipeak = pn['tariff_two_semipeak']
                d2 = pn['time_two_cat_peak_zones']
                d2['hour'] = pd.to_datetime(d2['time']).dt.hour
                r2 = pd.merge(d, d2, how='inner', on=['hour'])
                summ_kw_peak = r2['value'].sum()
                if type_val == 'val_summ_kw':
                    df_data = {'val_summ_kw': [summ_kw]}
                if type_val == 'val_transfer':
                    df_data = {'val_transfer': [transfer]}
                if type_val == 'val_calculate_ee_night':
                    df_data = {'val_calculate_ee_night': [calculate_ee_night]}
                if type_val == 'calculate_ee_semipeak':
                    calculate_ee_semipeak = (summ_kw - summ_kw_night - summ_kw_peak) * tariff_two_semipeak
                    df_data = {'calculate_ee_semipeak': [calculate_ee_semipeak]}
                if type_val == 'calculate_ee_peak':
                    calculate_ee_peak = summ_kw_peak * tariff_two_peak
                    df_data = {'calculate_ee_peak': [calculate_ee_peak]}
                if type_val == 'val_total':
                    calculate_ee_peak = summ_kw_peak * tariff_two_peak
                    calculate_ee_semipeak = (summ_kw - summ_kw_night - summ_kw_peak) * tariff_two_semipeak
                    total = transfer + calculate_ee_semipeak + calculate_ee_night + calculate_ee_peak
                    df_data = {'val_total': [total]}
            else:
                tariff_two_day = pn['tariff_two_day']
                if type_val == 'val_summ_kw':
                    df_data = {'val_summ_kw': [summ_kw]}
                if type_val == 'val_transfer':
                    df_data = {'val_transfer': [transfer]}
                if type_val == 'val_calculate_ee_night':
                    df_data = {'val_calculate_ee_night': [calculate_ee_night]}
                if type_val == 'val_calculate_ee_day':
                    calculate_ee_day = (summ_kw - summ_kw_night) * tariff_two_day
                    df_data = {'val_calculate_ee_day': [calculate_ee_day]}
                if type_val == 'val_total':
                    calculate_ee_day = (summ_kw - summ_kw_night) * tariff_two_day
                    total = transfer + calculate_ee_day + calculate_ee_night
                    df_data = {'val_total': [total]}
            df = pd.DataFrame(data=df_data, index=pd.date_range(start=start_date, end=start_date))
            df = df.astype(float)
            return df

        except Exception as err:
            self.logger.error("Error in the _two_category: " + str(err))
            raise Exception("Error in the _two_category: " + str(err))

    def _dispatch_dict(self, p, d):
        """
        Switch analysis.
        :return: output DataFrame
        """
        return {
            'one_category_val_summ_kw': lambda: self._one_category(p, d, 'val_summ_kw'),
            'one_category_val_transfer': lambda: self._one_category(p, d, 'val_transfer'),
            'one_category_val_calculate_ee': lambda: self._one_category(p, d, 'val_calculate_ee'),
            'one_category_val_total': lambda: self._one_category(p, d, 'val_total'),
            'two_category_two_zones_val_summ_kw': lambda: self._two_category(p, d, 'val_summ_kw'),
            'two_category_two_zones_val_transfer': lambda: self._two_category(p, d, 'val_transfer'),
            'two_category_two_zones_val_calculate_ee_night': lambda: self._two_category(p, d, 'val_calculate_ee_night'),
            'two_category_two_zones_val_calculate_ee_day': lambda: self._two_category(p, d, 'val_calculate_ee_day'),
            'two_category_two_zones_val_total': lambda: self._two_category(p, d, 'val_total'),
            'two_category_two_zones_energy_storage_val_total': lambda: self._two_category(p, self._offset_power_profile(p, d, 2), 'val_total'),
            'two_category_two_zones_energy_storage_effect_val_total': lambda: self._delta_result(self._two_category(p, d, 'val_total'), self._two_category(p, self._offset_power_profile(p, d, 2), 'val_total')),
            'two_category_three_zones_val_summ_kw': lambda: self._two_category(p, d, 'val_summ_kw', True),
            'two_category_three_zones_val_transfer': lambda: self._two_category(p, d, 'val_transfer', True),
            'two_category_three_zones_val_calculate_ee_night': lambda: self._two_category(p, d, 'val_calculate_ee_night', True),
            'two_category_three_zones_val_calculate_ee_semipeak': lambda: self._two_category(p, d, 'calculate_ee_semipeak', True),
            'two_category_three_zones_val_calculate_ee_peak': lambda: self._two_category(p, d, 'calculate_ee_peak', True),
            'two_category_three_zones_val_total': lambda: self._two_category(p, d, 'val_total', True),
            'two_category_three_zones_energy_storage_effect_val_total': lambda: self._delta_result(
                self._two_category(p, d, 'val_total', True),
                self._two_category(p, self._offset_power_profile(p, d, 20), 'val_total', True)),
            'two_category_three_zones_energy_storage_val_total': lambda: self._two_category(p, self._offset_power_profile(p,d,20), 'val_total', True),
            'three_category_val_summ_kw': lambda: self._three_category(p, d, 'val_summ_kw'),
            'three_category_val_transfer': lambda: self._three_category(p, d, 'val_transfer'),
            'three_category_val_sales_add': lambda: self._three_category(p, d, 'val_sales_add'),
            'three_category_val_calculate_ee': lambda: self._three_category(p, d, 'val_calculate_ee'),
            'three_category_val_power_cost': lambda: self._three_category(p, d, 'val_power_cost'),
            'three_category_val_other_services': lambda: self._three_category(p, d, 'val_other_services'),
            'three_category_val_total': lambda: self._three_category(p, d, 'val_total'),
            'three_category_energy_storage_val_summ_kw': lambda: self._three_category(p, self._offset_power_profile(p, d, 3), 'val_summ_kw'),
            'three_category_energy_storage_val_transfer': lambda: self._three_category(p, self._offset_power_profile(p, d, 3), 'val_transfer'),
            'three_category_energy_storage_val_sales_add': lambda: self._three_category(p, self._offset_power_profile(p, d, 3), 'val_sales_add'),
            'three_category_energy_storage_val_calculate_ee': lambda: self._three_category(p, self._offset_power_profile(p, d, 3), 'val_calculate_ee'),
            'three_category_energy_storage_val_power_cost': lambda: self._three_category(p, self._offset_power_profile(p, d, 3), 'val_power_cost'),
            'three_category_energy_storage_val_other_services': lambda: self._three_category(p, self._offset_power_profile(p, d, 3), 'val_other_services'),
            'three_category_energy_storage_val_total': lambda: self._three_category(p, self._offset_power_profile(p, d, 3), 'val_total'),
            'three_category_energy_storage_effect_val_calculate_ee': lambda: self._delta_result(self._three_category(p, d, 'val_calculate_ee'), self._three_category(p, self._offset_power_profile(p, d, 3), 'val_calculate_ee')),
            'three_category_energy_storage_effect_val_power_cost': lambda: self._delta_result(self._three_category(p, d, 'val_power_cost'), self._three_category(p, self._offset_power_profile(p, d, 3), 'val_power_cost')),
            'three_category_energy_storage_effect_val_total': lambda: self._delta_result(self._three_category(p, d, 'val_total'), self._three_category(p, self._offset_power_profile(p, d, 3), 'val_total')),
            'four_category_val_summ_kw': lambda: self._four_category(p, d, 'val_summ_kw'),
            'four_category_val_transfer': lambda: self._four_category(p, d, 'val_transfer'),
            'four_category_val_sales_add': lambda: self._four_category(p, d, 'val_sales_add'),
            'four_category_val_calculate_ee': lambda: self._four_category(p, d, 'val_calculate_ee'),
            'four_category_val_power_cost': lambda: self._four_category(p, d, 'val_power_cost'),
            'four_category_val_maintenance': lambda: self._four_category(p, d, 'val_maintenance'),
            'four_category_val_other_services': lambda: self._four_category(p, d, 'val_other_services'),
            'four_category_val_total': lambda: self._four_category(p, d, 'val_total'),
            'four_category_energy_storage_val_summ_kw': lambda: self._four_category(p, self._offset_power_profile(p, d, 4), 'val_summ_kw'),
            'four_category_energy_storage_val_transfer': lambda: self._four_category(p, self._offset_power_profile(p, d, 4), 'val_transfer'),
            'four_category_energy_storage_val_sales_add': lambda: self._four_category(p, self._offset_power_profile(p, d, 4), 'val_sales_add'),
            'four_category_energy_storage_val_calculate_ee': lambda: self._four_category(p, self._offset_power_profile(p, d, 4), 'val_calculate_ee'),
            'four_category_energy_storage_val_power_cost': lambda: self._four_category(p, self._offset_power_profile(p, d, 4), 'val_power_cost'),
            'four_category_energy_storage_val_maintenance': lambda: self._four_category(p, self._offset_power_profile(p, d, 4), 'val_maintenance'),
            'four_category_energy_storage_val_other_services': lambda: self._four_category(p, self._offset_power_profile(p, d, 4), 'val_other_services'),
            'four_category_energy_storage_val_total': lambda: self._four_category(p, self._offset_power_profile(p, d, 4), 'val_total'),
            'four_category_energy_storage_effect_val_calculate_ee': lambda: self._delta_result(self._four_category(p, d, 'val_calculate_ee'), self._four_category(p, self._offset_power_profile(p, 4), 'val_calculate_ee')),
            'four_category_energy_storage_effect_val_power_cost': lambda: self._delta_result(self._four_category(p, d, 'val_power_cost'), self._four_category(p, self._offset_power_profile(p, d, 4), 'val_power_cost')),
            'four_category_energy_storage_effect_val_total': lambda: self._delta_result(self._four_category(p, d, 'val_total'), self._four_category(p, self._offset_power_profile(p, d, 4), 'val_total')),
            'peak_hours': lambda: self._peaks_row(p),
            'two_category_two_zones_energy_storage': lambda: self._offset_power_profile(p, d, 2).set_index(['time']),
            'two_category_three_zones_energy_storage': lambda: self._offset_power_profile(p, d, 20).set_index(['time']),
            'three_category_energy_storage': lambda: self._offset_power_profile(p, d, 3).set_index(['time']),
            'four_category_energy_storage': lambda: self._offset_power_profile(p, d, 4).set_index(['time']),

            # 'three_category_': lambda: self._three_category(p, d, ''),
        }.get(p['method'], lambda: None)()

    def _delta_result(self, result1, result2):
        return result1 - result2

    def _analyze(self, p, d):
        """
        Run analysis.
        :return: output DataFrame
        """
        try:
            return self._dispatch_dict(p, d)
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))
