import subprocess
import arcpy
import os
import sys
import logging

import sqlite3
from arcpy.sa import *
import datetime
import geopandas as gpd
import pandas as pd
from utilities import prep_shapefile

def gdf2pd(dbfile):
    """
    Convert dbf to pandas dataframe using geopandas
    """
    gdf = gpd.read_file(dbfile)
    df = pd.DataFrame(gdf.drop('geometry', axis=1))

    return df


def zstats(start, stop, final_aoi, cellsize, value, zone, analysis):

    arcpy.CheckOutExtension("Spatial")
    arcpy.env.overwriteOutput = True

    for i in range(start, stop):
        print("prepping feature id {}".format(i))

        # select one individual feature from the input shapefile
        mask = prep_shapefile.zonal_stats_mask(final_aoi, i)

        scratch_wkspc = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scratch.gdb')

        # set environments
        arcpy.env.extent = mask
        arcpy.env.mask = mask
        arcpy.env.cellSize = cellsize
        arcpy.env.snapRaster = value
        arcpy.env.scratchWorkspace = scratch_wkspc
        arcpy.env.workspace = scratch_wkspc

        tables_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tables')

        z_stats_tbl = os.path.join(tables_dir, 'output_{}.dbf'.format(i))

        start_time = datetime.datetime.now()
        
        print("running zstats")
        outzstats = arcpy.sa.ZonalStatisticsAsTable(zone, "VALUE", value, z_stats_tbl, "DATA", "SUM")

        end_time = datetime.datetime.now() - start_time
        print("debug:time elapsed: {}".format(end_time))

        # convert the output zstats table into a pandas DF
        df = gdf2pd(z_stats_tbl)

        # populate a new field "id" with the FID and analysis with the sum
        df['ID'] = i
        df[analysis] = df['SUM']

        # sometimes this value came back as an object, so here we are fixing that bug
        df.VALUE = df.VALUE.astype(int)

        # name of the sql database to store the sql table
        zstats_results_db = os.path.join(tables_dir, 'zstats_results_db.db')

        # create a connection to the sql database
        conn = sqlite3.connect(zstats_results_db)

        # append the dataframe to the database
        df.to_sql(analysis, conn, if_exists='append')

        # delete these because they create a lock
        del df
        os.remove(z_stats_tbl)

        # reset these environments. Otherwise the shapefile is redefined based on features within the extent
        arcpy.env.extent = None
        arcpy.env.mask = None
        arcpy.env.cellSize = None
        arcpy.env.snapRaster = None

        print('process succeeded for id {0}'.format(i))


def main_script(layer, raster):

    # this is the shapefile after being projected
    final_aoi = layer.final_aoi

    # get the range of features to run
    start_id = 0
    end_id = int(arcpy.GetCount_management(final_aoi).getOutput(0))

    start = start_id
    stop = end_id

    logging.debug("Number of features: {}".format(end_id))
    zstats_subprocess = zstats(start_id, end_id, layer.final_aoi, raster.cellsize, raster.value, raster.zone, raster.analysis)

    # run using python3
    executable = sys.executable

    script_cmd = [executable, zstats_subprocess, raster.value,
                  raster.zone, layer.final_aoi, raster.cellsize, raster.analysis]

    expected_complete_total = len(list(range(start_id, end_id)))
    feature_status = {} # if the feature failed, this gets populated

    while len(feature_status) < expected_complete_total: # if there is an error, write a message but keep going

        cmd = script_cmd + [str(start_id), str(end_id)]

        # this runs the analysis. send to a for-loop that will run zstats for each feature ID
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            # for each line that comes back from the subprocess, if "succeeded" is in the line
            # increase counter by 1.
            for line in iter(p.stdout.readline, b''):

                arcpy.AddMessage(line)

                # Each line that comes back from the subprocess represents 1 feature/ID
                # We need to keep track of this in case a feature fails so we can skip it

                if b'debug' in line:
                    pass
                else:
                    if b'process succeeded' in line:
                        feature_status[start_id] = True
                        start_id += 1

            p.wait()
            # Since no lines are returned from sub if it fails, add this: get the return code from the failure,
            # as long as it isn't 0, its a failure, and increment the counter by 1 so it starts on the next feautre        

            if p.returncode is None or p.returncode != 0:
                print("failed")
                logging.debug("WARNING: Feature id {} failed".format(start_id))
                feature_status[start_id] = False

                # this will increase the start ID so we don't rerun the failed feature
                start_id += 1
        except TypeError as e:
            logging.debug("Type Error during zonal stats subprocess. {}".format(e))
            break