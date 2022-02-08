import arcpy
import logging
import pandas as pd
import tqdm
from os import path
from arcpy import env
from datetime import datetime
from zipfile import ZipFile
from collections import OrderedDict
from collections import defaultdict as dd
from typing import Optional
from arcgis.features import GeoAccessor

# Fixed Wireless Codebase Imports
from FixedWireless.postgis.connect import ODW
from FixedWireless.postgis import queries as queries
from FixedWireless.utils import google as google
from FixedWireless.utils import siklu as siklu
from FixedWireless.utils import helpers as helpers
from FixedWireless.utils import arcgis as arc

LOCAL_SCRATCH_FOLDER = r'C:\Users\kryan\Documents\Local_Pro_Projects\Fixed Wireless\FixedWireless\scratch_folder'


class IdentifyCandidates:

    """
    IdentifyCandidates is the class that houses a vast majority of the Fixed Wireless Pre-Qualification program logic.
    This class encapsulates the entirety of the processing workflow that takes a single point location X/Y and then
    uses the supplied parameters to identify and report on candidate features that meet the users specifications.

    - Writes output data into an excel workbook using multiple sheets to organize the results. Additionally an ESRI
      FileGDB is created and populated with **Candidate** features as well as **Spider Plot Polylines** that
      represent the *as-the-crow-flies* connections to identified candidates.
    """

    def __init__(
            self,
            output_name: str,
            source_site_coordinates: str,
            latitude: float,
            longitude: float,
            input_min_speed: int,
            input_max_neighbors: int,
            distance: int,
            raw_coordinates: bool = False,
            include_non_lit: bool = False,
            google_check: Optional[bool] = False,
            extra_control: Optional[bool] = False,
            input_max_lit: Optional[int] = 0,
            input_max_re: Optional[int] = 0,
            input_max_cci_sites: Optional[int] = 0,
            scoring_excel: Optional[str] = None
    ):
        """
        Initializes IdentifyCandidates instance and stores the parameters supplied prior to runtime that will be
        referenced throughout the process of searching the Operational Data Warehouse (ODW) for potential candidates
        that satisfy the end-users criteria.

        :param output_name: output_name value will be sanitized to remove illegal characters
        :param latitude: Search target Latitude in decimal degrees
        :param longitude: Search target Longitude in decimal degrees
        :param input_min_speed: Minimum speed for Siklu Link Budget API call
        :param input_max_neighbors: Maximum number of neighbors to consider - value is multiplied by 4 (query vertices)
        :param distance: Maximum search distance ranging between 1 - 11 km
        :param include_non_lit: Flags whether to include non_lit query results
        :param google_check: Flags whether to process candidates using the Google elevation check
        :param extra_control: Flags whether additional limits by type should be applied
        :param input_max_lit: Maximum number of lit building candidates to allow
        :param input_max_re: Maximum number of real estate candidates to allow (ASD & MGT)
        :param input_max_cci_sites: Maximum number of CCI Sites candidates to allow
        """
        self._start = datetime.now()  # get timestamp for logfile name
        self._logfile = f"FW_Log_{self._start.strftime('%d%b%Y-%I%M%S')}"  # construct name
        self.odw = ODW(username='CC_GEO_PRIVATE')  # helper class that stores ODW connection params
        self.logger = None  # setup_logging assigns the root logger to this field
        self.setup_logging() # initialize logger
        self.workspace = r'C:\Users\kryan\Documents\Local_Pro_Projects\Fixed Wireless\FixedWireless\scratch_folder\Testing.gdb' # TODO swap after test env.scratchGDB  # get path to scratch GDB
        self.folder = LOCAL_SCRATCH_FOLDER  # TODO Swap out after testing self.folder = env.scratchFolder
        self.project = helpers.sanitize_value(output_name)  # Check project name for invalid characters & replace if any
        self.source_site = source_site_coordinates
        self.latitude = latitude
        self.longitude = longitude
        if not raw_coordinates:
            self.parse_source_site()
        self.min_speed = input_min_speed
        self.max_neighbors = input_max_neighbors
        self.distance = distance

        # Boolean Params
        self.raw_coorinates = raw_coordinates
        self.non_lit = include_non_lit
        self.check_google = google_check
        self.limit_by_type = extra_control

        # property placeholders
        self._max_lit_buildings = -1
        self._max_real_estate = -1
        self._max_cci_sites = -1
        self._parameters = None

        # Optional Params
        self.max_lit_buildings = input_max_lit
        self.max_real_estate = input_max_re
        self.max_cci_sites = input_max_cci_sites

        # Internals
        self._zipfile = None  # ZipFile object stored here during process
        self._zipfile_path = path.join(self.folder, f"{self.project}.zip")  # output location on disk

        self._xlsx_writer = None  # Pandas ExcelWriter object for creating .xlsx
        self._xlsx_path = path.join(self.folder, f"{self.project}.xlsx")  # location on disk

        self._gnp_name = f"{self.project}_GNP.csv"  # Google Network Planner filename
        self._gnp_path = path.join(self.folder, self._gnp_name)  # location on disk

        # Result Data
        self.unique_ids = dd(list)  # collections.defaultdict factory for dict of lists e.g. {<default>: []}
        self.ranking_dataframes = dict()  # keeps track of individual ranking results that will be written to excel
        self.output_data = None  # final dataframe where results are assembled
        self.output_data_all = None
        self.output_data_sorted = None
        self.output_fields = []
        
        # Instantiate empty dataframes to receive query results
        self.candidates = pd.DataFrame(columns=queries.CANDIDATE_FIELDS)  # initial_search query results are appended here
        self.lit_building_ranking = pd.DataFrame(columns=queries.LIT_BUILDING_RANKING_FIELDS)  # lit building ranks

        # Not always populated
        self.cci_sites_ranking = pd.DataFrame(columns=queries.CCI_SITES_RANKING_FIELDS)
        self.real_estate_asd_ranking = pd.DataFrame(columns=queries.REAL_ESTATE_ASD_RANKING_FIELDS)
        self.real_estate_mgt_ranking = pd.DataFrame(columns=queries.REAL_ESTATE_MGT_RANKING_FIELDS)

        # Siklu Budget API Dataframe
        self.siklu_api_responses = pd.DataFrame(columns=siklu.SIKLU_FIELDS)  # Link reports are appended here

        # Feature Class Params
        self.candidate_point_fields = []
        self.spider_plot_fields = []
        self.candidate_points = None
        self.spider_plot_lines = None
        self.line_arrays = []
        self.sanitized = []

        # Geoprocessing environment settings
        env.outputCoordinateSystem = arcpy.SpatialReference(4326)
        env.overwriteOutput = True
        self.wgs84 = arcpy.SpatialReference(4326)

        # Scoring Template
        self.score_inputs = pd.read_excel(scoring_excel) if scoring_excel else None

        # setup script
        self._init()

    def _init(self) -> None:
        """
        Wrapper method that calls setup_logging and create_output_handlers.

        :return: None
        """
        self.setup_logging()
        self.create_output_handlers()

    def setup_logging(self) -> None:
        """
        Initializes Logger using logging.basicConfig to specify the output filepath, write mode, message format,
        datetime format, and logging level.

        - Once the basicConfig has been invoked we can assign the return value
          of logging.getLogger(*filename*) to the instance attribute **self.logger**.
        """
        filepath = path.join("./logs/", (self._logfile + '.log'))
        logging.basicConfig(
            filename=filepath,
            filemode='a',
            format='|%(levelname)s|%(message)s|%(asctime)s|',
            datefmt='%H:%M:%S',
            level=logging.DEBUG
        )

        self.logger = logging.getLogger(self._logfile)
        self.logger.info('Logging Initialized')

    def create_output_handlers(self) -> None:
        """
        Instantiates both file handlers, ZipFile for the .zip directory produced by the tool, and Pandas ExcelWriter
        for writing various dataframes to the final Excel workbook.

        :return: None
        """
        self._zipfile = ZipFile(self._zipfile_path, 'w')
        self._xlsx_writer = pd.ExcelWriter(self._xlsx_path)
        self.logger.debug('Output Zip & Excel handlers instantiated.')

    @property
    def distance(self) -> int:
        """
        Distance to search for candidates. Must be between 1-11 km
        """
        return self._distance

    @distance.setter
    def distance(self, value):
        """
        distance.setter coerces supplied value to either the min or max value if it is greater than 11 or less than 0.
        """
        self.logger.debug(f"Distance Raw: {value}km")

        if 0 < value <= 11:
            # If value is within the allowed range it is assigned to the instance attribute
            self._distance = value
        else:
            value = 0 if value < 0 else 11
            # Conditional re-assigns the value to either the minimum or maximum allowed value depending
            self._distance = value

        self.logger.debug(f"Distance Set: {value}km")

    @property
    def max_lit_buildings(self) -> int:
        """
        Returns the maximum number of Lit Building candidates allowed as an integer.

         - **Always applicable**

        :return: Maximum Lit Buildings limit
        """
        return self._max_lit_buildings

    @max_lit_buildings.setter
    def max_lit_buildings(self, value):
        if not self.limit_by_type:
            self._max_lit_buildings = self.max_neighbors * 4
            self.logger.debug(f"{value} not assigned to max_lit_buildings")
        else:
            self._max_lit_buildings = value * 4

    @property
    def max_real_estate(self) -> int:
        """
        Returns the maximum number of Real Estate candidates as an integer.

        - **Only applicable when the user has opted to limit the number of candidates by type.**

        - **Real Estate Limit applies to both ASD & MGT queries**

        :return: Maximum Real Estate limit
        """
        return self._max_real_estate

    @max_real_estate.setter
    def max_real_estate(self, value: int):
        """
        Set Real Estate maximum.

        - If user did **NOT** elect to *limit by type* the supplied value is **coerced** to equal
        the maximum number of neighbors that the user supplied at the start.

        :param value: Integer to assign
        :return: None
        """

        if not self.limit_by_type:
            self._max_real_estate = self.max_neighbors
            self.logger.debug(f"{value} not assigned to max_real_estate")
        else:
            self._max_real_estate = value

    @property
    def max_cci_sites(self) -> int:
        """
        Returns the maximum number of CCI Sites candidates as an integer.

        - **Only applicable when the user has opted to limit the number of candidates by type.**

        :return: Maximum CCI Sites limit
        """
        return self._max_cci_sites

    @max_cci_sites.setter
    def max_cci_sites(self, value: int):
        """
        Set CCI sites maximum.

        - If user did **NOT** elect to *limit by type* the supplied value is **coerced to equal the maximum number
          of neighbors** that the user supplied at the start.

        :param value: Integer to assign
        :return: None
        """
        if not self.limit_by_type:
            self._max_cci_sites = self.max_neighbors
            self.logger.debug(f"{value} not assigned to max_cci_sites")
        else:
            self._max_cci_sites = value
            
    @property
    def ZipFile(self) -> ZipFile:
        return self._zipfile
    
    @property
    def ExcelWriter(self) -> pd.ExcelWriter:
        return self._xlsx_writer

    @property
    def parameters(self) -> pd.DataFrame:
        """
        Converts raw input args into **Pandas DataFrame**
        """
        if self._parameters is not None:
            return self._parameters

        if not self.non_lit and not self.limit_by_type:
            column_names = ['Project Name', 'Latitude', 'Longitude', 'Minimum Speed', 'Max Candidates', 'Distance (km)']
            values = [[self.project, self.latitude, self.longitude, self.min_speed, self.max_neighbors, self.distance]]

        else:
            column_names = ['Project Name', 'Latitude', 'Longitude', 'Minimum Speed', 'Max Candidates', 'Distance (km)',
                        'Non-Lit?', 'Extra Control?', 'Lit Number', 'RE Number', 'CCI Sites Number']
            values = [[self.project, self.latitude, self.longitude, self.min_speed, self.max_neighbors, self.distance,
                      self.non_lit, self.limit_by_type, self.max_lit_buildings, self.max_real_estate, self.max_cci_sites]]

        self.logger.debug(f"Parameter DF Columns: {column_names}")

        self.logger.debug(f"Parameter DF Values: {values}")

        param_df = pd.DataFrame(data=values, columns=column_names)

        self._parameters = param_df

        return self._parameters

    def add_to_zip(self, file_to_add: str, name: str) -> None:
        """
        Add file created during the course of the script to the Zip directory instantiated by _init().

        :param file_to_add: Filepath to object you wish to add to the archive
        :param name: The name to assign the object within the archive

        :return: None
        """
        self._zipfile.write(file_to_add, name)
        self.logger.debug(f'Added {file_to_add} to output zip file')

    def write_dataframe_to_excel(self, dataframe: pd.DataFrame, sheet: str) -> None:
        """
        Use the Pandas ExcelFile handler to write out contents of the DataFrame within the Excel Workbook using the
        sheet name provided.

        :param dataframe: Pandas DataFrame with data
        :param sheet: Name of the sheet you wish to write the data to
        :return: None
        """
        dataframe.to_excel(self._xlsx_writer, sheet_name=sheet, index=False)
        self.logger.debug(f'{dataframe.shape[0]} rows written to Excel on sheet: {sheet}')

    def create_gnp_input(self) -> None:
        """
        Creates the formatted input spreadsheet required by Google Network Planner. Uses the template function stored
        within the FixedWireless.Utils.google module.

        :return: None
        """
        template = google.param_df_template(self.parameters)

        formatted_gnp_input = pd.DataFrame.from_dict(template)

        formatted_gnp_input.to_csv(self._gnp_path, index=False)

        self.logger.debug('GNP parameter CSV generated')

        self.add_to_zip(self._gnp_path, self._gnp_name)

        self.logger.debug('GNP parameter CSV added to Zip directory')

    def initial_search(self) -> None:

        """
        Dynamically determines which queries are required based on user parameters. Each query is assigned to a
        dictionary value
        :return:
        """

        self.logger.debug('Beginning Initial Search...')

        if self.non_lit:

            query_dict = {
                'build_lit_building_query': self.max_lit_buildings,
                'build_asd_real_estate_query': self.max_real_estate,
                'build_mgt_real_estate_query': self.max_real_estate,
                'build_macro_cci_sites_query': self.max_cci_sites
            }

        else:
            query_dict = {
                'build_lit_building_query': self.max_lit_buildings,
                'build_macro_cci_sites_query': self.max_cci_sites
            }

        param_template = [self.longitude, self.latitude, self.distance]

        self.logger.debug(f"{len(query_dict)} total queries included in initial search")

        for function_name, limit_value in query_dict.items():

            parameters = param_template[:]
            parameters.append(limit_value)

            if function_name == 'build_macro_cci_sites_query':
                parameters.append(self.non_lit)

            sql_string = getattr(queries, function_name)(*parameters)

            results = pd.read_sql(sql_string, self.odw.connection)

            self.logger.debug(f"{'_'.join(function_name.split('_')[1:-1])} returned {results.shape[0]} records w/in {self.distance}km")

            self.candidates = self.candidates.append(results, ignore_index=True)

        self.candidates['height'] = self.candidates['height'].fillna(float(30))

        self.logger.debug("Filled Candidates DF NULL height values with value -> 30.0")

    def check_google_los(self) -> None:

        self.candidates['Clearance_Score'] = self.candidates.apply(lambda row: google.get_elevation_path(row.lat, row.long, self.latitude, self.longitude, row.height), axis=1)

        delete_indices = self.candidates['Clearance_Score'] == 'Delete'
        if any(delete_indices.values.tolist()):
            self.logger.debug(f"{len(delete_indices)} Candidates to be removed after Google elevation check")

            self.candidates.drop(delete_indices, inplace=True, axis=1)

        self.candidates.reset_index(drop=True, inplace=True)

        self.logger.debug(f"{self.candidates.shape[0]} Candidates remain after Google elevation check")

    # Vectorized method for Pandas DataFrame
    def categorize_id(self, row) -> None:
        self.unique_ids[row['candidate_type']].append(row['id'])

    def extract_unique_ids(self) -> None:
        self.logger.debug("Begin extracting unique ID values")
        self.candidates.apply(self.categorize_id, axis=1)
        nl = '\n'
        self.logger.debug(f"{nl.join([ctype +': ' + str(len(id_list)) for ctype, id_list in self.unique_ids.items()])}")

    def ranking(self) -> None:
        """
        Constructs a dictionary of query function identifiers and their corresponding lists of unique IDs based on
        the user supplied parameter include_non_lit. Each query function is stored in the postgis.queries module and
        acts as template that inserts the query parameters (unique ID). The SQL string is passed via Pandas
        read_sql() method where the record set returned is converted on the fly into a dataframe.

        """
        self.logger.debug('Begin Ranking...')

        if self.non_lit:

            query_dict = OrderedDict([
                ('build_lit_building_rank_query', self.unique_ids['ospi']),
                ('build_asd_real_estate_rank_query', self.unique_ids['re_asd']),
                ('build_mgt_real_estate_rank_query', self.unique_ids['re_mgt']),
                ('build_macro_cci_sites_rank_query', self.unique_ids['cci_sites'])
            ])

        else:
            query_dict = OrderedDict([
                ('build_lit_building_rank_query', self.unique_ids['ospi']),
                ('build_macro_cci_sites_rank_query', self.unique_ids['cci_sites'])
            ])

        self.logger.debug(f"{len(query_dict)} total ranking queries")

        for function_name, unique_ids in query_dict.items():
            if not unique_ids:
                self.logger.warning(f"No unique IDs for {function_name}")
                continue
            short_name = '_'.join(function_name.split('_')[1:-2])

            sql_string = getattr(queries, function_name)(unique_ids)

            self.logger.debug(f"calling {function_name}")

            results = pd.read_sql(sql_string, self.odw.connection)

            self.logger.debug(f"completed {function_name} - {results.shape[0]} records")

            self.ranking_dataframes.update({short_name: results})

        self.logger.debug("Ranking complete")

    def assemble_output_dataframe(self) -> None:
        """
        Depending on the number of dataframes returned by the ranking() method this method combines the resulting
        ranking dataframes into a single object and merges it with the candidates dataframe.
        """
        self.logger.debug('Assembling output dataframe')
        if len(self.ranking_dataframes) > 1:

            self.logger.debug('Concatenating ranking dataframes')
            combined_ranks = pd.concat(self.ranking_dataframes, ignore_index=True, sort=False)

            self.logger.debug('Merging combined ranking dataframes with candidate dataframe')
            self.output_data = self.candidates.merge(
                combined_ranks,
                left_on=self.candidates.id.astype(str),
                right_on=combined_ranks.id.astype(str),
                how='left',
                sort=False,
                suffixes=['', '_Drop']
            )

        elif len(self.ranking_dataframes) == 1:

            self.logger.debug('Merging single ranking dataframe with candidate dataframe')

            combined_ranks = self.ranking_dataframes[list(self.ranking_dataframes.keys()).pop()]
            self.output_data = self.candidates.merge(
                combined_ranks,
                left_on=self.candidates.id.astype(str),
                right_on=combined_ranks.id.astype(str),
                how='left',
                sort=False,
                suffixes=['', '_Drop']
            )
        else:

            self.logger.critical('No Ranking DataFrames resulted from queries!')
            raise RuntimeError('No Ranking Dataframes were found!')

        self.output_data.drop(['key_0', 'id_Drop'], axis=1, inplace=True)
        self.logger.debug('Dropped key_0 & id_Drop from output dataframe')

        if 'open_levels' in self.output_data:
            self.logger.debug('Parsing open_levels attribute')
            self.output_data['open_levels'].fillna('NULL', inplace=True)
            self.output_data['TX AGL'] = self.output_data.apply(self.parse_open_levels, axis=1)

        else:
            self.logger.debug('Setting default value of 30 for TX AGL')
            self.output_data['TX AGL'] = 30

    # Vectorized method for Pandas DataFrame
    def parse_open_levels(self, row: pd.Series) -> float:
        """
        When applied to the output DataFrame e.g. dataframe['column'] = dataframe.apply(func) this function parses
        the open_levels column from gis_dw_private.private_cci_sites_scrubbing_vw and extracts the right-hand most
        integer before the distance unit abbreviation.

        Example *open_levels* value: 30 - 115 FT , 117 - 125 FT , 129 - 150 FT
        """
        value = row['open_levels']
        if value in ['NULL', 'None', 'nan']:
            return 30.0
        else:
            #
            last_value = value.split('-')[-1]
            self.logger.debug(f"{row.id} - open_levels - {last_value}")
            try:
                result = float(''.join([char for char in last_value if char.isnumeric()])) - 5
                return result
            except ValueError as ve:
                self.logger.error(f'{ve.args}')
                return 30.0

    def siklu_api_call(self) -> None:
        self.output_data.apply(lambda row: siklu.link_budget_api(self, siklu.DEFAULT_MODEL, row_series=row), axis=1)

        #  Concatenate siklu API responses DF
        self.output_data = pd.concat([self.siklu_api_responses, self.output_data], axis=1)

        # Rename output dataframe fields capacity & model
        self.output_data.rename(
            columns={
                'capacity': 'Speed',
                'model': 'Equipment'
            }, inplace=True
        )

        self.output_data.dropna(how='any', subset=['Speed'])

        num_records = len(self.output_data.index)

        self.logger.debug(f"{num_records} Candidates remaining after Siklu API search")

        if num_records < 1:
            raise ValueError(
                'Siklu search deemed all candidates unworthy. Increase search radius or try another location.'

            )

    def output_ranker(self):

        if self.check_google:
            self.output_dataframe_ranker_with_google_attributes()
        else:
            self.output_dataframe_ranker()

        self.output_fields.extend(self.output_data.columns.values.tolist())
        self.logger.debug(f"output fields: {', '.join(self.output_fields)}")

    def output_dataframe_ranker(self):
        """ **Sort** all neighbors based on *distance*, and other *attributes* like has *power*, *has fiber connectivity*, *has FCC license on roof*, etc.
            Also return the winning or "top" **20** neighbors """
  
        sort_df = self.output_data.drop_duplicates('id')
        
        unique_types = self.output_data.candidate_type.unique().tolist()

        if 'cci_sites' in unique_types and 'ospi' in unique_types and any([_type in unique_types for _type in ['re_asd', 're_mgt']]):
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'fiber_on_site',
                'is_cci_power_available',
                'if_no_cci_power_is_meter_avail',
                'pt_id',
                'score',
                'antenna',
                'revshare',
                'Speed',
                'fi_dist',
                'fcc_cnt'
            ],
                ascending=[True, True, True, False, True, True, False, False, True, False])

        elif 'cci_sites' in unique_types and any([_type in unique_types for _type in ['re_asd', 're_mgt']]):
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'fiber_on_site',
                'is_cci_power_available',
                'if_no_cci_power_is_meter_avail',
                'pt_id',
                'score',
                'antenna',
                'revshare',
                'Speed',
                'fi_dist'
            ],
                ascending=[True, True, True, False, True, True, False, False, True])

        elif 'cci_sites' in unique_types and 'ospi' in unique_types:
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'fiber_on_site',
                'is_cci_power_available',
                'if_no_cci_power_is_meter_avail',
                'pt_id',
                'score',
                'antenna',
                'revshare',
                'Speed',
                'fcc_cnt'
            ],
                ascending=[True, True, True, False, True, True, False, False, False])

        elif 'cci_sites' in unique_types:
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'fiber_on_site',
                'is_cci_power_available',
                'if_no_cci_power_is_meter_avail',
                'pt_id',
                'score',
                'antenna',
                'revshare',
                'Speed'
            ],
                ascending=[True, True, True, False, True, True, False, False])

        elif 'ospi' in unique_types and any([_type in unique_types for _type in ['re_asd', 're_mgt']]):
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'score',
                'antenna',
                'Speed',
                'fi_dist',
                'fcc_cnt'
            ],
                ascending=[True, True, False, True, False])

        elif 'ospi' in unique_types:
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'score',
                'antenna',
                'Speed',
                'fcc_cnt'
            ],
                ascending=[True, True, False, False])

        elif any([_type in unique_types for _type in ['re_asd', 're_mgt']]):
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'antenna',
                'Speed',
                'fi_dist'
            ],
                ascending=[True, False, True])
        else:
            sort_df_sorted_temp = sort_df.sort_values(by=['antenna', 'Speed'], ascending=[True, False])

        sort_df_sorted_temp.reset_index(drop=True, inplace=True)
        sort_df_sorted_temp = sort_df_sorted_temp.rename_axis('fw_rank').reset_index()
        sort_df_sorted_temp['fw_rank'] = sort_df_sorted_temp['fw_rank'].astype(int) + 1

        self.output_data_sorted = sort_df_sorted_temp
        self.output_data_all = self.output_data.join(sort_df_sorted_temp[['id', 'fw_rank']].set_index('id'), on='id')
        self.logger.debug('Created Output Data ALL Dataframe')

        self.output_data = sort_df_sorted_temp
        self.output_data['id'] = self.output_data['id'].astype(str)
        self.output_data.dropna(1, 'all', inplace=True)
        self.output_data = self.output_data.sort_values(by='fw_rank')

    def output_dataframe_ranker_with_google_attributes(self):

        sort_df = self.output_data.drop_duplicates('id')

        unique_types = self.output_data.candidate_type.unique().tolist()

        if all([_type in unique_types for _type in ['cci_sites', 'ospi']]) and any(
                [_type in unique_types for _type in ['re_asd', 're_mgt']]):
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'Clearance_Score',
                'fiber_on_site',
                'is_cci_power_available',
                'if_no_cci_power_is_meter_avail',
                'pt_id',
                'score',
                'antenna',
                'revshare',
                'Speed',
                'fi_dist',
                'fcc_cnt'
            ],
                ascending=[False, True, True, True, False, True, True, False, False, True, False]
            )

        elif 'cci_sites' in unique_types and any([_type in unique_types for _type in ['re_asd', 're_mgt']]):
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'Clearance_Score',
                'fiber_on_site',
                'is_cci_power_available',
                'if_no_cci_power_is_meter_avail',
                'pt_id',
                'score',
                'antenna',
                'revshare',
                'Speed',
                'fi_dist'
            ],
                ascending=[False, True, True, True, False, True, True, False, False, True])

        elif 'cci_sites' in unique_types and 'ospi' in unique_types:
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'Clearance_Score',
                'fiber_on_site',
                'is_cci_power_available',
                'if_no_cci_power_is_meter_avail',
                'pt_id',
                'score',
                'antenna',
                'revshare',
                'Speed',
                'fcc_cnt'
            ],
                ascending=[False, True, True, True, False, True, True, False, False, False])

        elif 'cci_sites' in unique_types:
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'Clearance_Score',
                'fiber_on_site',
                'is_cci_power_available',
                'if_no_cci_power_is_meter_avail',
                'pt_id',
                'score',
                'antenna',
                'revshare',
                'Speed'
            ],
                ascending=[False, True, True, True, False, True, True, False, False])

        elif 'ospi' in unique_types and any([_type in unique_types for _type in ['re_asd', 're_mgt']]):
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'Clearance_Score',
                'score',
                'antenna',
                'Speed',
                'fi_dist',
                'fcc_cnt'
            ],
                ascending=[False, True, True, False, True, False])

        elif 'ospi' in unique_types:
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'Clearance_Score',
                'score',
                'antenna',
                'Speed',
                'fcc_cnt'
            ],
                ascending=[False, True, True, False, False])

        elif any([_type in unique_types for _type in ['re_asd', 're_mgt']]):
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'Clearance_Score',
                'antenna',
                'Speed',
                'fi_dist'
            ],
                ascending=[False, True, False, True])
        else:
            sort_df_sorted_temp = sort_df.sort_values(by=[
                'Clearance_Score',
                'antenna',
                'Speed'
            ],
                ascending=[False, True, False])

        sort_df_sorted_temp.reset_index(drop=True, inplace=True)
        sort_df_sorted_temp = sort_df_sorted_temp.rename_axis('fw_rank').reset_index()
        sort_df_sorted_temp['fw_rank'] = sort_df_sorted_temp['fw_rank'].astype(int) + 1

        self.output_data_sorted = sort_df_sorted_temp
        self.output_data_all = self.output_data.join(sort_df_sorted_temp[['id', 'fw_rank']].set_index('id'), on='id')
        self.logger.debug('Created Output Data ALL Dataframe')

        self.output_data = sort_df_sorted_temp
        self.output_data['id'] = self.output_data['id'].astype(str)
        self.output_data.dropna(1, 'all', inplace=True)
        self.output_data = self.output_data.sort_values(by='fw_rank')
    
    def score_final_candidates(self):
        # TODO Implement scoring/sorting mechanism
        self.logger.warning('Scoring NOT IMPLEMENTED')
        return NotImplementedError('Method Not Yet Implemented!!')

    def write_data_to_excel(self):

        all_candidate_filepath = path.join(self.folder, f'{self.project}_Candidate_file.csv')

        self.output_data_all[['candidate_type', 'id', 'lat', 'long', 'fw_rank']].to_csv(
            all_candidate_filepath, index=False
        )

        self.ZipFile.write(all_candidate_filepath, f'{self.project}_Candidate_file.csv')

        self.output_data_sorted.drop(['height'], axis=1, inplace=True)
        self.output_data_sorted.to_excel(self.ExcelWriter, f'Top {len(self.output_data_sorted.index)} Sorted', index = False)

        helpers.set_column_width(
            f'Top {len(self.output_data_sorted.index)} Sorted',
            self.output_data_sorted,
            self.ExcelWriter
        )

        for candidate_type, dataframe in self.ranking_dataframes.items():
            dataframe['id'] = dataframe['id'].astype(str)
            dataframe = dataframe.merge(self.output_data_all[['fw_rank', 'id']], how='left', on='id')
            dataframe.to_excel(self.ExcelWriter, f"all_{candidate_type}", index=False)
            
            self.logger.debug(f"Wrote all_{candidate_type} to Excel Workbook")
            
            helpers.set_column_width(f"all_{candidate_type}", dataframe, self.ExcelWriter)
            
            self.logger.debug(f'Set maximum column width for {candidate_type}')

        self.cci_scrub_query()
        self.ExcelWriter.save()
        self.ZipFile.write(self._xlsx_path, f'{self.project}.xlsx')
        self.ZipFile.close()

    def cci_scrub_query(self) -> None:
        cci_dataframe = self.output_data_sorted.query("candidate_type == 'cci_sites'")

        if cci_dataframe.empty:
            self.logger.warning(f"No CCI Sites Candidates remaining!!!")
            return

        sql_string = queries.build_cc_scrub_query(cci_dataframe.id.tolist())

        scrub_dataframe = pd.read_sql(sql_string, self.odw.connection)

        scrub_dataframe.to_excel(self.ExcelWriter, 'Tower Scrub', index=False)

    def parse_source_site(self):
        epsg = arcpy.Describe(self.source_site).spatialReference.factoryCode

        with arcpy.da.SearchCursor(self.source_site, ['SHAPE@X', 'SHAPE@Y']) as site_cursor:
            for r in site_cursor:
                if epsg != 4326:
                    self.odw.cursor.execute(
                        f"""
                        Select ST_X(ST_Transform(ST_GeomFromText('Point ({r[0]} {r[1]})', {epsg}), 4326)) as lon, 
                        ST_Y(ST_Transform(ST_GeomFromText('Point ({r[0]} {r[1]})', {epsg}), 4326)) as lat
                        """
                    )
                    coordinates = self.odw.cursor.fetchone()
                    self.longitude = coordinates[0]
                    self.latitude = coordinates[1]
                else:
                    self.longitude = r[0]
                    self.latitude = r[1]

    def sanitize_field_names(self):
        self.sanitized = ['SHAPE@']

        for field_name in self.output_fields:
            self.sanitized.append(field_name.replace(' ', '_').replace(')', '_').replace('(', '_'))

        for field_map in arc.FW_FIELD_MAP:
            if field_map[0] in self.sanitized:
                self.candidate_point_fields.append(field_map)

    def build_plotline_arrays(self, row):
        # insert_row = []
        # candidate_point = arcpy.PointGeometry(arcpy.Point(row.long, row.lat), self.wgs84)
        # insert_row.append(candidate_point)

        self.line_arrays.append([
            arcpy.Polyline(
                arcpy.Array([
                    arcpy.Point(row.long, row.lat),
                    arcpy.Point(self.longitude, self.latitude)]),
                self.wgs84
            ), row.Speed
        ])
        # for value in self.candidate_point_fields:
        #     fieldName = value[0]
        #     if fieldName in row:
        #
        #         insert_row.append(row[fieldName])
        #     else:
        #         insert_row.append('')
        # toExtend = [row[v[0]] for v in self.candidate_point_fields]
        # insert_cursor.insertRow(insert_row.extend(toExtend))

    def construct_features(self):

        self.sanitize_field_names()

        self.candidate_points = path.join(self.workspace, 'Candidates')

        # self.candidate_points = arcpy.CreateFeatureclass_management(self.workspace, 'Candidates', 'POINT')[0]
        # arcpy.AddFields_management(self.candidate_points, self.candidate_point_fields)

        self.spider_plot_lines = arcpy.CreateFeatureclass_management(self.workspace, 'Spider_lines', 'POLYLINE')[0]
        arcpy.AddField_management(self.spider_plot_lines, 'Speed', 'SHORT')

        #candidate_cursor = arcpy.da.InsertCursor(self.candidate_points, self.sanitized)

        self.output_data_sorted.apply(lambda row: self.build_plotline_arrays(row), axis=1)

        ga = GeoAccessor.from_xy(self.output_data_sorted, 'long', 'lat')

        ga.spatial.to_featureclass(self.candidate_points)

        spid_cursor = arcpy.da.InsertCursor(self.spider_plot_lines, ['SHAPE@', 'Speed'])
        for spid_line in self.line_arrays:
            spid_cursor.insertRow(spid_line)

        del spid_cursor

        arcpy.SetParameter(14, self.candidate_points)
        arcpy.SetParameter(15, self.spider_plot_lines)
        arcpy.SetParameter(16, path.join(self.folder, f'{self.project}.zip'))

    def execute_process(self):
        self.logger.debug('Execute Process Wrapper Method Called')
        process_methods = OrderedDict([
            ('create_gnp_input', (self.logger.debug, 'execute: create_gnp_input')),
            ('initial_search', (self.logger.debug, 'execute: initial_search')),
            ('check_google_los', (self.logger.debug, 'execute: check_google_los')),
            ('extract_unique_ids', (self.logger.debug, 'execute: extract_unique_ids')),
            ('ranking', (self.logger.debug, 'execute: ranking')),
            ('assemble_output_dataframe', (self.logger.debug, 'execute: assemble_output_dataframe')),
            ('siklu_api_call', (self.logger.debug, 'execute: siklu_api_call')),
            ('output_ranker', (self.logger.debug, 'execute: df_ranker')),
            ('write_data_to_excel', (self.logger.debug, 'execute: write to excel')),
            ('construct_features', (self.logger.debug, 'execute: create esri features'))
        ])
        with tqdm.tqdm(total=len(process_methods), desc='Execute All') as prog:
            arcpy.SetProgressor('step', message='Identifying FW Candidates', min_range=0, max_range=len(process_methods), step_value=1)
            for method_name, logging_tuple in process_methods.items():
                arcpy.SetProgressorLabel(f"{method_name}")
                if method_name == 'check_google_los' and not self.check_google:
                    arcpy.SetProgressorPosition()
                    continue
                prog.desc = method_name
                method = getattr(self, method_name)
                handle, message = logging_tuple
                handle(message)
                method()
                arcpy.SetProgressorPosition()
                prog.update()
            arcpy.ResetProgressor()

    def test_logs(self):
        self.logger.warning('TEST WARNING!')
        self.logger.error('TEST ERROR!')
        self.logger.critical('TEST CRITICAL!')


if __name__ == '__main__':

    run_gp_tool = False
    if run_gp_tool:
        project_name = arcpy.GetParameterAsText(0)
        use_raw_coords = arcpy.GetParameterAsText(1)
        raw_lat = arcpy.GetParameter(2)
        raw_long = arcpy.GetParameter(3)
        source_site_xy = arcpy.GetParameter(4)
        min_speed = arcpy.GetParameter(5)
        num_neighbors = arcpy.GetParameter(6)
        distance_km = arcpy.GetParameter(7)
        google_check = arcpy.GetParameterAsText(8)
        include_non_lit = arcpy.GetParameterAsText(9)
        control_by_type = arcpy.GetParameterAsText(10)
        lit_num = arcpy.GetParameter(11)
        re_num = arcpy.GetParameter(12)
        cc_num = arcpy.GetParameter(13)

    else:

        score_input = r'C:\Users\kryan\Documents\Local_Pro_Projects\Fixed Wireless\FixedWireless\scoring\input_data\building_type_priorities.xlsx'

        project_name = 'TestProject'
        use_raw_coords = 'true'
        raw_lat = 40.7390831
        raw_long = -73.9913778
        source_site_xy = None
        min_speed = 900
        num_neighbors = 25
        distance_km = 2
        google_check = 'true'
        include_non_lit = 'true'
        control_by_type = 'false'
        lit_num = 0
        re_num = 0
        cc_num = 0

    use_raw_coords = helpers.convert_arcpy_bool(use_raw_coords)
    google_check = helpers.convert_arcpy_bool(google_check)
    include_non_lit = helpers.convert_arcpy_bool(include_non_lit)
    control_by_type = helpers.convert_arcpy_bool(control_by_type)

    gp_tool_test = IdentifyCandidates(
        project_name,
        source_site_xy,
        raw_lat,
        raw_long,
        min_speed,
        num_neighbors,
        distance_km,
        use_raw_coords,
        include_non_lit,
        google_check=google_check,
        extra_control=control_by_type,
        input_max_lit=lit_num,
        input_max_re=re_num,
        input_max_cci_sites=cc_num,
        scoring_excel=score_input)

    gp_tool_test.execute_process()
