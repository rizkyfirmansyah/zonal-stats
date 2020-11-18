import os
import pandas as pd
import sqlite3
import logging

class Raster(object):
    """ A layer class to prep the input shapefile to zonal stats
    :param source_aoi: the path to the shapefile to run zonal stats
    :param source_id_col: the unique ID field in the input shapefile
    :return:
    """

    def __init__(self, analysis, geodatabase, area, forest, loss, tcd, biomass):
        logging.info("\ncreating a raster object for analysis {}".format(analysis))
        self.analysis = analysis
        self.zone = None
        self.value = None
        self.geodatabase = geodatabase
        self.cellsize = None
        self.output_tables = []
        self.df = None
        self.area = area
        self.forest = forest
        self.loss = loss
        self.tcd = tcd
        self.biomass = biomass

        self.populate_ras_prop()

    def populate_ras_prop(self):
        zone_value_dict = {"forest_loss": {"zone": self.loss, "value": self.area, "cellsize": "MAXOF"},
                           "forest_extent": {"zone": self.forest, "value": self.area, "cellsize": "MAXOF"},
                           "biomass_weight": {"zone": self.tcd, "value": self.biomass, "cellsize": "MAXOF"},
                           "emissions": {"zone": self.loss, "value": self.biomass, "cellsize": "MAXOF"}}

        self.zone = os.path.join(self.geodatabase, zone_value_dict[self.analysis]["zone"])
        self.value = os.path.join(self.geodatabase, zone_value_dict[self.analysis]["value"])
        self.cellsize = zone_value_dict[self.analysis]['cellsize']

        logging.info("populating raster properties with zone: {} and value: {} and cell size {}".format(os.path.basename(self.zone),
                                                                                os.path.basename(self.value),
                                                                                                 self.cellsize))

    def db_to_df(self, l):

        # convert sql table to df
        print("converting sql table to df")
        tables_dir = os.path.join(l.root_dir, 'tables')
        zstats_results_db = os.path.join(tables_dir, 'zstats_results_db.db')

        conn = sqlite3.connect(zstats_results_db)
        # self.analysis is like: forest_loss and/or emissions, etc
        qry = "SELECT VALUE, ID, {0} FROM {0} WHERE VALUE > 0".format(self.analysis)
        df = pd.read_sql(qry, conn)

        logging.info(df)
        self.df = df