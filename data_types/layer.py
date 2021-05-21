import os
import pandas as pd
import arcpy
import sys
import logging

from utilities import zstats_handler
from utilities import post_processing


class Layer(object):
    """ A layer class to prep the input shapefile to zonal stats
    :param source_aoi: the path to the shapefile to run zonal stats
    :param source_id_col: the unique ID field in the input shapefile
    :return:
    """

    def __init__(self, source_aoi, source_id_col):

        self.source_aoi = source_aoi
        self.source_id_col = source_id_col

        self.final_aoi = None

        self.emissions = None
        self.forest_loss = None
        self.biomass_weight = None
        self.forest_extent = None

        self.root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        print("creating Layer with aoi {} and source id column {}\n".format(self.source_aoi, self.source_id_col))

    # these are all the things i want to do with the input shapefile. this is called from zonal_stats.py
    def project_source_aoi(self):
        arcpy.env.overwriteOutput = True
        out_cs = arcpy.SpatialReference(4326)
        self.final_aoi = os.path.join(self.root_dir, 'shapefile', "project.shp")
        arcpy.Project_management(self.source_aoi, self.final_aoi, out_cs)

    def join_tables(self, tcd_categorized, threshold, user_def_column_name, output_file_name):
        print("joining tables \n")

        # make a list of all the tables we have. These are already dataframes
        possible_dfs = [self.emissions, self.forest_loss, self.biomass_weight, self.forest_extent]

        # get rid of df's we don't have
        df_list = [x for x in possible_dfs if x is not None]

        # how to get column names to keep? like extent, emissions, loss? i'm going through and getting
        # second column for each df which is the analysis name
        print(df_list)
        analysis_names = [x.columns.values[2] for x in df_list]
        logging.info("Analysis names required: {}".format(analysis_names))

        # convert original SUM values into the right units
        for index, item in enumerate(analysis_names):

            if item == 'forest_loss':
                analysis_names[index] = 'forest_loss_ha'

                self.forest_loss['forest_loss'] = self.forest_loss.forest_loss.astype(float)
                self.forest_loss['forest_loss_ha'] = self.forest_loss['forest_loss'] / 10000

            if item == 'forest_extent':
                analysis_names[index] = 'forest_extent_ha'

                self.forest_extent['forest_extent'] = self.forest_extent.forest_extent.astype(float)
                self.forest_extent['forest_extent_ha'] = self.forest_extent['forest_extent'] / 10000

            if item == 'biomass_weight':
                analysis_names[index] = 'biomass_weight_Tg'

                self.biomass_weight['biomass_weight'] = self.biomass_weight.biomass_weight.astype(float)
                self.biomass_weight['biomass_weight_Tg'] = self.biomass_weight['biomass_weight'] / 1000000

        # join all the data frames together on Value and ID. Value is the tcd/loss code (41 = loss in 2001 at 1-10%tcd
        # or loss in 2001 at >30% tcd. ID is the unique ID of the feature in the shapefile
        merged = pd.concat([df.set_index(['VALUE', 'ID']) for df in df_list], axis=1)
        merged = merged.reset_index()

        # To 2 get outputs from a single function and apply to 2 different columns in the dataframe:
        # http://stackoverflow.com/questions/12356501/pandas-create-two-new-columns-in-a-dataframe-with-
        # values-calculated-from-a-pre?rq=1
        # tcd and year columns is equal to the first and second output from the function: value_to_tcd_year

        if tcd_categorized == "yes":
            try:
                merged['tcd'], merged['year'] = list(zip(*merged["VALUE"].map(post_processing.value_to_tcd_year)))
            except KeyError:
                logging.info("oops, loss mosaic doesn't have the arithmetic function applied. Refer to readme file")
        else:
            try:
                merged['tcd'], merged['year'] = list(zip(*merged["VALUE"].map(post_processing.value_to_tcd_year_each)))
            except KeyError:
                logging.info("oops, loss mosaic doesn't have the arithmetic function applied. Refer to readme file")

        # the value_to_tcd_year function is good for when user runs all thresholds, but not just one.
        # so, overwrite the tcd column when it comes back
        if threshold != "all" and tcd_categorized == "yes":
            merged['tcd'] = "> {}%".format(threshold)

        # convert shp to pandas dataframe
        final_aoi_df = zstats_handler.gdf2pd(self.final_aoi)

        # reset index of final_aoi_df
        final_aoi_df = final_aoi_df.reset_index()

        merged = pd.merge(merged, final_aoi_df, left_on='ID', right_on='index')
        
        # get rid of undesired columns here
        if "ID" in merged.columns:
            del merged['ID']
        if "index" in merged.columns:
            del merged['index']
        if "ha" in merged.columns:
            del merged['ha']
        if "forest_loss" in merged.columns:
            del merged['forest_loss']
        if "forest_extent" in merged.columns:
            del merged['forest_extent']

        print('SAMPLE OF OUTPUT:')
        print(merged.head(5))

        final_output_csv = os.path.join(self.root_dir, 'result', '{}.csv'.format(output_file_name))
        merged.to_csv(final_output_csv, index=False)
