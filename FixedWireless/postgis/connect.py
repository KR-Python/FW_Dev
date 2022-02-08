import os
import sys
import importlib
import logging
import traceback
from typing import Dict, Optional
from psycopg2 import connect, DatabaseError, OperationalError
from sqlalchemy import create_engine

pd, gpd, GeoAccessor, GeoSeriesAccessor = None, None, None, None

class ODW:
    """
    Helper class for initializing a psycopg2 connection object and optionally instantiating a cursor object.
    """

    postgres = ('ODW_PROD', 'ODW_DEV', 'ODW_UAT',)

    user_creds = ('CC_GEO_PRIVATE', 'CC_GEO_PUBLIC', 'CCFODWADMIN')

    connection_params: Dict[str, str] = {
        'database': '',
        'user': '',
        'password': '',
        'host': '',
        'port': ''
    }

    def __init__(self,
                 environment: str = 'ODW_PROD',
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 logger_object: Optional[logging.Logger] = None):
        """
        Initialize ODW connection.


        :param environment: ['ODW_PROD', 'ODW_DEV', 'ODW_UAT']
        :param username:
        :param password:
        :param logger_object:
        """
        self._env = None
        self.environment = environment
        self._win_user = os.getlogin()
        self._qgis_user = f"qgis{self._win_user}"
        self._set_user = username
        self._set_password = password
        self._conn: connect = None
        self._cursor = None
        self._server_cursors = {}
        self._logger = logger_object
        self._reset = False
        self._init()

    def _init(self) -> None:
        """
        Additional setup logic.

        :return:
        """
        host, port, dbname = os.environ.get(self.environment).split(';')

        if not self._set_password:

            if self.user.upper() not in self.user_creds:
                raise KeyError(f"""
                |{self.user}| is not a member of: {self.user_creds}
                Enter a different username or provide a username and password.
                """)

            user = self._set_user
            password = os.environ.get(self._set_user.upper())
        else:
            user = self._set_user
            password = self._set_password

        self.connection_params.update({
            'host': host,
            'port': int(port),
            'database': dbname,
            'user': user.lower(),
            'password': password
        })

    @property
    def environment(self):
        return self._env

    @environment.setter
    def environment(self, environment_key):
        if environment_key not in self.postgres:
            raise ValueError(
                f"Value {environment_key} is not a member of {list(self.postgres)} unable to set environment."
            )
        else:
            self._env = environment_key

    @property
    def user(self):
        return self._set_user

    @user.setter
    def user(self, username):
        if username is None:
            self._set_user = self._qgis_user
            self._set_password = self._qgis_user
        if username == 'ccfodwadmin' and self.environment != 'ODW_DEV':
            raise EnvironmentError('ccfodwadmin only available on ODW_DEV')
        else:
            self._set_user = username

    @property
    def connection(self):
        if self._conn and (not self._conn.closed or self._reset):
            return self._conn

        else:

            try:
                self._conn = connect(**self.connection_params)
            except (DatabaseError, OperationalError) as db_err:
                if self._logger:
                    self._logger.error(db_err)
                print(db_err)
                sys.exit(1)

            return self._conn

    @property
    def engine(self):
        func = lambda x: self.connection_params.get(x)
        eng = create_engine(
            f'postgresql+psycopg2://{func("user")}:{func("password")}@{func("host")}/{func("database")}?port={func("port")}'
        )
        return eng

    @property
    def cursor(self) -> connect:
        if self._cursor and not self.connection.closed:

            return self._cursor
        else:
            self._cursor = self.connection.cursor()
            return self._cursor

    def serverSideCursor(self, name, refresh=False):

        if refresh:
            cur = self.connection.cursor(name)
            self._server_cursors.update({name: cur})
            return cur
        elif name in self._server_cursors:
            cur = self._server_cursors.get(name)
            return cur
        else:
            raise ValueError(f'Cursor name {name} not member of {list(self._server_cursors.keys())}')

    # TODO refactor this mess
    @staticmethod
    def importDataFrameLib(lib_name):

        if lib_name not in ('pandas', 'geopandas', 'esri'):
            raise ValueError(f"{lib_name} is not a member of (pandas, geopandas, esri)")

        if (lib_name.lower() == 'pandas') and ('pandas' not in sys.modules):
            global pd
            pd = importlib.import_module('pandas')

        elif (lib_name.lower() == 'geopandas') and ('geopandas' not in sys.modules):
            global gpd
            gpd_spec = importlib.util.find_spec('geopandas')
            if not gpd_spec:
                raise ImportWarning('Geopandas library not found!\n')
            gpd = importlib.import_module('geopandas')

        elif lib_name.lower() == 'esri':
            global GeoAccessor
            global GeoSeriesAccessor
            gpd_spec = importlib.util.find_spec('geopandas')
            if not gpd_spec:
                raise ImportWarning(
                    'GeoPandas library not found!\nSpatially Enabled DataFrame depends on GeoPandas to read directly from PostGIS!\n\nIn the Python Terminal type: conda install geopandas and try again\n\n')

            gpd = importlib.import_module('geopandas') if gpd is None else gpd
            feats = importlib.import_module('arcgis', 'arcgis.features')
            GeoAccessor = getattr(feats, 'GeoAccessor')
            GeoSeriesAccessor = getattr(feats, 'GeoSeriesAccessor')

    # TODO replace current error handling, too generic
    # noinspection PyUnresolvedReferences
    def queryToDataFrame(self, query, flavor='pandas', geometry_column=None):
        try:
            self.importDataFrameLib(flavor)
        except (ValueError, ImportWarning) as err:
            print(f'Error when attempting to import required library for DataFrame flavor: {flavor}')
            #exType, exValue, exTrace = sys.exc_info()
            traceback.print_exc(file=sys.stdout)

        if flavor == 'pandas':
            try:
                data = pd.read_sql(query, con=self.connection)
                return data
            except Exception as e:
                if self._logger:
                    self._logger.error(e.args)
                print(e.args)

        elif flavor == 'geopandas':

            try:
                data = gpd.GeoDataFrame.from_postgis(query, con=self.engine.connect(), geom_col=geometry_column)
                return data
            except Exception as e:
                print(e)

        elif flavor == 'esri':

            try:
                data = gpd.GeoDataFrame.from_postgis(query, con=self.engine.connect(), geom_col=geometry_column)
                return GeoAccessor.from_geodataframe(data)
            except Exception as e:
                print(e)