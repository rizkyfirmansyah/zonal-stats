import os
import datetime
import sys
import logging
import configparser

from data_types.layer import Layer
from data_types.raster import Raster
from raster_functions import raster_prep
from utilities import zstats_handler, post_processing, prep_shapefile

# get user inputs from config file:
config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_file.ini")
config = configparser.ConfigParser()
config.read(config_file)
config_dict = config['inputs']

log_file = config_dict['log_file']
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.getLogger().addHandler(logging.StreamHandler())

start = datetime.datetime.now()
logging.info("\n\n{} BEGINNING LOG {}".format('='*5, '='*5))

# analysis: forest_extent, forest_loss, biomass_weight, emissions
analysis = [x.strip() for x in config_dict['analysis'].split(",")]
shapefile = config_dict['shapefile']
threshold = config_dict['threshold']
tcd_categorized = config_dict['tcd_categorized']
geodatabase = config_dict['geodatabase']
user_def_column_name = config_dict['user_def_column_name']
col_name = "FID"  # if this is in a gdb, make sure it assigns it OBJECT_ID
output_file_name = config_dict['output_file_name']
intersect = config_dict['intersect']

# Create a handler for default input config file
def initInputRasterVariable():
    global area, forest, biomass, tcd, loss, database_name, intersect_col
    database_name = config_dict['database_name']
    intersect_col = config_dict['intersect_col']
    intersect_col = [x.strip() for x in intersect_col.split(',')]
    
    try:
        if config_dict['area'] is not None or not config_dict['area']:
            area = config_dict['area']
        else:
            area = ''
    except KeyError:
        area = 'area'
        
    try:
        if config_dict['forest'] is not None or not config_dict['forest']:
            forest = config_dict['forest']
        else:
            forest = ''
    except KeyError:
        forest = ''

    try:
        if config_dict['biomass'] is not None or not config_dict['biomass']:
            biomass = config_dict['biomass']
        else:
            biomass = ''
    except KeyError:
        biomass = ''
        
    try:
        if config_dict['loss'] is not None or not config_dict['loss']:
            loss = config_dict['loss']
        else:
            loss = ''
    except KeyError:
        loss = 'loss'
        
    try:
        if config_dict['tcd'] is not None or not config_dict['tcd']:
            tcd = config_dict['tcd']
        else:
            tcd = ''
    except KeyError:
        tcd = 'tcd'


    return area, forest, biomass, tcd, loss, database_name, intersect_col

initInputRasterVariable()
logging.info("intersect_col: {}".format(intersect_col))
logging.info("Categorizing TCD? {}".format(tcd_categorized))
logging.info("Area defined as = {}".format(area))
logging.info("Forest Extent defined as = {}".format(forest))
logging.info("Forest Loss defined as = {}".format(loss))
logging.info("Biomass defined as = {}".format(biomass))

# delete existing database so duplicate data isn't appended
prep_shapefile.delete_database(database_name)

# if user requests emissions analysis, need to runs 2 zonal stats, one min, one max.
analysis_requested = prep_shapefile.build_analysis(analysis)

# remap the tcd mosaic and apply a raster function that adds tcd + loss year mosaics
# raster_prep.remap_threshold(geodatabase, threshold)

# create layer object. this just sets up the properties that will later be filled in for each analysis
l = Layer(shapefile, col_name)

# set final aoi equal to the shapefile
l.final_aoi = shapefile

# project input to wgs84
l.project_source_aoi()

# loop over the analysis. If forest_loss or biomass_weight, will just be one analysis. if emissions, need to
# run forest_loss and emissions

for analysis_name in analysis_requested:

    # create raster object.
    r = Raster(analysis_name, geodatabase, area, forest, loss, tcd, biomass)

    # run zstats, put results into sql db.
    zstats_handler.main_script(l, r, database_name, intersect, intersect_col)

    # get results from sql to pandas df
    r.db_to_df(l, database_name)

    # this roughly translate to layer.analysis_name == r.df
    # or forest_loss = pd.DataFrame(forestlossdata). It gives the resulting dataframe the name of the analysis and sets
    # it as the attribute l.forest_loss, l.emissions, which are the dataframes
    setattr(l, analysis_name, r.df)

if l.emissions is not None:
    print("converting biomass to emissions")
    l.emissions = post_processing.biomass_to_mtc02(l)

# join possible tables (loss, emissions, extent, etc) and decode to loss year, tcd
l.join_tables(tcd_categorized, threshold, user_def_column_name, output_file_name)
logging.info(("elapsed time: {}".format(datetime.datetime.now() - start)))