from analytics.analysis import Analysis
import datetime
import pandas as pd
import requests
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
          "parameters": [
              {"name": "method", "count": 1, "type": "SELECT", "options": ["one_category", "two_category_two_zones", "two_category_three_zones", "three_category", "four_category", "energy_storage", "peak_hours"],
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
              {"name": "region", "count": 1, "type": "SELECT", "options": REGIONS,
               "info": "Regions of Russia (ISO 3166-2): RU-SVE, RU-PER, RU-BA, RU-UD"},
              {"name": "retailer", "count": 1, "type": "SELECT", "options": RETAILERS,
               "info": "Sales companies listed on the atsenergo.ru website"},
              {"name": "time_four_cat", "count": 1, "type": "DICT OF TIME",
               "info": 'Set of dictionaries containing time intervals. Example: ["07:00:00","12:59:00"],["14:00:00","21:59:00"] '},
              {"name": "time_two_cat_night_zones", "count": 1, "type": "DICT OF TIME",
               "info": 'Set of dictionaries containing time intervals for second category. Night zone. Example: ["07:00:00","12:59:00"],["14:00:00","17:59:00"],["19:00:00","21:59:00"] '},
              {"name": "time_two_cat_peak_zones", "count": 1, "type": "DICT OF TIME",
               "info": 'Set of dictionaries containing time intervals for second category. Peak zone. Example: ["07:00:00","12:59:00"],["14:00:00","21:59:00"]'},
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
              {"name": "time_charging_energy_storage", "count": 1, "type": "DICT OF TIME",
               "info": 'Set of dictionaries containing time intervals. Example: ["2019-12-01_00:00:00","2019-12-01_03:00:00"],["2019-12-02_00:00:00","2019-12-02_04:00:00"] '},
              {"name": "time_return_energy_storage", "count": 1, "type": "DICT OF TIME",
               "info": 'Set of dictionaries containing time intervals. Example: ["2019-12-01_07:00:00","2019-12-01_08:00:00"],["2019-12-02_00:05:00","2019-12-02_08:00:00"] '},
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
            d = self._preprocess_df(data)
            res = self._analyze(p, d)
            # pd.options.display.max_rows = 100
            # print(res[:50])
            return res
        except Exception as err:
            self.logger.error(err)
            raise Exception(str(err))

    def _preprocess_df(self, data):
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
                new_data['time'] = new_data['time'].apply(lambda x:
                                                          datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S+00:00'))
            else:
                raise Exception("DataFrame is None")
            self.logger.debug("DataFrame preprocessed")
            return new_data
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
            pn['tariff_two_night'] = self._check_float(parameters['tariff_two_three_zones_ee'][0])
            if peak:
                pn['tariff_two_semipeak'] = self._check_float(parameters['tariff_two_three_zones_ee'][1])
                pn['tariff_two_peak'] = self._check_float(parameters['tariff_two_three_zones_ee'][2])
                pn['time_two_cat_peak_zones'] = self._generate_time_df(parameters['time_two_cat_peak_zones'])
            else:
                pn['tariff_two_day'] = self._check_float(parameters['tariff_two_two_zones_ee'][1])
            return pn
        except Exception as err:
            self.logger.error("Error in _parse_parameters_two_cat: " + str(err))
            raise Exception("Error in _parse_parameters_two_cat: " + str(err))

    def _parse_parameters_energy_storage(self, p):
        """
        Parameters parsing (type conversion, modification, etc).
        """
        self.logger.debug("Parsing parameters for the energy storage")
        try:
            parameters = p['all']
            pn = dict()
            pn['power_return_energy_storage'] = self._check_float(parameters['power_return_energy_storage'][0])
            pn['power_charging_energy_storage'] = self._check_float(parameters['power_charging_energy_storage'][0])
            pn['time_return_energy_storage'] = self._generate_time_df(parameters['time_return_energy_storage'])
            pn['time_charging_energy_storage'] = self._generate_time_df(parameters['time_charging_energy_storage'])
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
                this_time = {}
                this_time['start'] = datetime.datetime.strptime(time_v[0], '%H:%M:%S').time()
                this_time['end'] = datetime.datetime.strptime(time_v[1], '%H:%M:%S').time()
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
                start_time = pd.Timestamp(time_v[0])
                end_time = pd.Timestamp(time_v[1])
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
            if method not in ["one_category", "two_category_two_zones", "two_category_three_zones", "three_category",
                              "three_category_energy_storage", "three_category_energy_storage_effect", "four_category",
                              "four_category_energy_storage", "four_category_energy_storage_effect", "peak_hours", "energy_storage"]:
                raise Exception
                self.logger.debug("Parsed parameter 'method': " + str(method))
            return method
        except Exception as err:
            self.logger.debug("Wrong parameter 'method': " + str(parameters['method']) + " " + str(err))
            raise Exception("Wrong parameter 'method': " + str(parameters['method']) + " " + str(err))

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
                        if time['start'] <= time_value < time['end']:
                            if max < row.value: max = row.value
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

    def _peaks_row (self, p, d):
        """
        Output of combined maximum hours
        :param p:
        :param d:
        :return: dataframe with hours and peaks
        """
        try:
            try:
                start_date = d.loc[d.index[0], 'time']
            except Exception as err:
                self.logger.error("Error in the start date: " + str(err))
                raise Exception("Error in the start date: " + str(err))

            peaks = self._get_peaks(self._parse_xls(self._dls_calcfacthour(start_date, p)))
            d['val_peak'] = (d['time'].isin(peaks['time'])).astype(int)
            d = d.set_index(['time'])
            d = d.drop(['value'], axis=1)
            d.index.name = None
            return d

        except Exception as err:
            self.logger.error("Error in the _peaks_row: " + str(err))
            raise Exception("Error in the _peaks_row: " + str(err))

    def _offset_power_profile(self, p, d):
        """
        Output of combined maximum hours
        :param p:
        :param d:
        :return: dataframe with times and value
        """

        try:
            pn = self._parse_parameters_energy_storage(p)
            return_storage = pn['time_return_energy_storage']
            return_storage['valuem'] = pn['power_return_energy_storage']
            charging_storage = pn['time_charging_energy_storage']
            charging_storage['valuep'] = pn['power_charging_energy_storage']
            rm = pd.merge(d, return_storage, how='outer', on=['time'])
            rm = pd.merge(rm, charging_storage, how='outer', on=['time'])
            rm = rm.fillna(0)
            rm['value'] = rm['value'] - rm['valuem']
            rm['value'] = rm['value'] + rm['valuep']
            rm = rm.drop(['valuem', 'valuep'], axis=1)
            return rm

        except Exception as err:
            self.logger.error("Error in the _offset_power_profile: " + str(err))
            raise Exception("Error in the _offset_power_profile: " + str(err))

    def _three_category(self, p, d):
        """
        Calculates the total cost for 3 price categories
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
            total = self._calculate_ee(d, price_three_cat) + self._sum_peak(d, peaks, tariff_power) + self._multiplication_data_tariff(d, tariff_losses) + self._multiplication_data_tariff(d, tariff_sales_three) + self._multiplication_data_tariff(d, tariff_other_services)
            summ_kw = d.value.sum()
            transfer = self._multiplication_data_tariff(d, tariff_losses)
            sales_add = self._multiplication_data_tariff(d, tariff_sales_three)
            calculate_ee = self._calculate_ee(d, price_three_cat)
            power_cost = self._sum_peak(d, peaks, tariff_power)
            other_services = self._multiplication_data_tariff(d, tariff_other_services)
            d = {'val_summ_kw': [summ_kw], 'val_transfer': [transfer], 'val_sales_add': [sales_add], 'val_calculate_ee': [calculate_ee], 'val_power_cost': [power_cost], 'val_other_services': [other_services], 'val_total': [total]}
            df = pd.DataFrame(data=d, index=pd.date_range(start=start_date, end=start_date))
            df = df.astype(float)
            return df

        except Exception as err:
            self.logger.error("Error in the _three_category: " + str(err))
            raise Exception("Error in the _three_category: " + str(err))

    def _four_category(self, p, d):
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

            total = self._calculate_ee(d, price_three_cat) + self._sum_peak(d, peaks, tariff_power) + self._maintenance(d, time_zone,
                                                                                              tariff_maintenance_four) + self._multiplication_data_tariff(
                d, tariff_losses) + self._multiplication_data_tariff(d, tariff_sales_four) + self._multiplication_data_tariff(d, tariff_other_services)

            summ_kw = d.value.sum()
            transfer = self._multiplication_data_tariff(d, tariff_losses)
            sales_add = self._multiplication_data_tariff(d, tariff_sales_four)
            calculate_ee = self._calculate_ee(d, price_three_cat)
            maintenance = self._maintenance(d, time_zone, tariff_maintenance_four)
            power_cost = self._sum_peak(d, peaks, tariff_power)
            other_services = self._multiplication_data_tariff(d, tariff_other_services)

            d = {'val_summ_kw': [summ_kw], 'val_transfer': [transfer], 'val_sales_add': [sales_add], 'val_calculate_ee': [calculate_ee], 'val_power_cost': [power_cost], 'val_maintenance': [maintenance], 'val_other_services': [other_services], 'val_total': [total]}
            df = pd.DataFrame(data=d, index=pd.date_range(start=start_date, end=start_date))
            df = df.astype(float)
            return df

        except Exception as err:
            self.logger.error("Error in the _four_category: " + str(err))
            raise Exception("Error in the _four_category: " + str(err))

    def _one_category(self, p, d):
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
            transfer = self._multiplication_data_tariff(d, tariff_losses_three)
            calculate_ee = self._multiplication_data_tariff(d, tariff_one_ee)
            total = transfer + calculate_ee
            summ_kw = d.value.sum()
            d = {'val_summ_kw': [summ_kw], 'val_transfer': [transfer], 'val_calculate_ee': [calculate_ee], 'val_total': [total]}
            df = pd.DataFrame(data=d, index=pd.date_range(start=start_date, end=start_date))
            df = df.astype(float)
            return df

        except Exception as err:
            self.logger.error("Error in the _one_category: " + str(err))
            raise Exception("Error in the _one_category: " + str(err))

    def _two_category(self, p, d, peak = False):
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
            if peak:
                tariff_two_peak = pn['tariff_two_peak']
                tariff_two_semipeak = pn['tariff_two_semipeak']
                d2 = pn['time_two_cat_peak_zones']
                d2['hour'] = pd.to_datetime(d2['time']).dt.hour
                r2 = pd.merge(d, d2, how='inner', on=['hour'])
                summ_kw_peak = r2['value'].sum()
                calculate_ee_peak = summ_kw_peak * tariff_two_peak
                calculate_ee_semipeak = (summ_kw - summ_kw_night - summ_kw_peak)*tariff_two_semipeak
                total = transfer + calculate_ee_semipeak + calculate_ee_night + calculate_ee_peak
                d = {'val_summ_kw': [summ_kw], 'val_transfer': [transfer], 'val_calculate_ee_night': [calculate_ee_night],
                     'calculate_ee_semipeak': [calculate_ee_semipeak], 'calculate_ee_peak': [calculate_ee_peak],
                     'val_total': [total]}
            else:
                tariff_two_day = pn['tariff_two_day']
                calculate_ee_day = (summ_kw - summ_kw_night)*tariff_two_day
                total = transfer + calculate_ee_day + calculate_ee_night
                d = {'val_summ_kw': [summ_kw], 'val_transfer': [transfer], 'val_calculate_ee_night': [calculate_ee_night], 'val_calculate_ee_day': [calculate_ee_day],
                     'val_total': [total]}

            df = pd.DataFrame(data=d, index=pd.date_range(start=start_date, end=start_date))
            df = df.astype(float)
            return df

        except Exception as err:
            self.logger.error("Error in the _two_category: " + str(err))
            raise Exception("Error in the _two_category: " + str(err))

    def _analyze(self, p, d):
        """
        Run analysis.
        :return: output DataFrame
        """
        try:
            if p['method'] == 'one_category':
                self.logger.debug("one_category")
                result = self._one_category(p, d)
            elif p['method'] == 'two_category_two_zones':
                self.logger.debug("two_category_two_zones")
                result = self._two_category(p, d)
            elif p['method'] == 'two_category_three_zones':
                self.logger.debug("two_category_three_zones")
                result = self._two_category(p, d, True)
            elif p['method'] == 'three_category':
                self.logger.debug("three_category")
                result = self._three_category(p, d)
            elif p['method'] == 'three_category_energy_storage':
                self.logger.debug("three_category_energy_storage")
                d_energy = self._offset_power_profile(p, d)
                result = self._three_category(p, d_energy)
            elif p['method'] == 'three_category_energy_storage_effect':
                self.logger.debug("three_category_energy_storage_effect")
                d_energy = self._offset_power_profile(p, d)
                result_d = self._three_category(p, d)
                result_storage = self._three_category(p, d_energy)
                result = result_d - result_storage
            elif p['method'] == 'four_category':
                self.logger.debug("four_category")
                result = self._four_category(p, d)
            elif p['method'] == 'four_category_energy_storage':
                self.logger.debug("four_category_energy_storage")
                d_energy = self._offset_power_profile(p, d)
                result = self._four_category(p, d_energy)
            elif p['method'] == 'four_category_energy_storage_effect':
                self.logger.debug("four_category_energy_storage_effect")
                d_energy = self._offset_power_profile(p, d)
                result_d = self._four_category(p, d)
                result_storage = self._four_category(p, d_energy)
                result = result_d - result_storage
            elif p['method'] == 'peak_hours':
                self.logger.debug("peak_hours")
                result = self._peaks_row(p, d)
            elif p['method'] == 'energy_storage':
                self.logger.debug("energy_storage")
                d_energy = self._offset_power_profile(p, d)
                d_energy = d_energy.set_index(['time'])
                d_energy.index.name = None
                result = d_energy
            else:
                raise Exception("Unknown method: " + str(p['method']))
            return result
        except Exception as err:
            self.logger.error("Impossible to analyze: " + str(err))
            raise Exception("Impossible to analyze: " + str(err))
