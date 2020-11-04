import os
# import simpledbf
import pandas as pd
import arcpy
import sys

from utilities import zstats_handler
from utilities import post_processing, final_columns


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

    def join_tables(self, user_def_column_name, output_file_name):
        print("joining tables \n")

        # make a list of all the tables we have. These are already dataframes
        possible_dfs = [self.emissions, self.forest_loss, self.biomass_weight, self.forest_extent]
        # get rid of df's we don't have
        df_list = [x for x in possible_dfs if x is not None]
        # how to get column names to keep? like extent, emissions, loss? i'm going through and getting
        # third column for each df which is the analysis name
        analysis_names = [x.columns.values[3] for x in df_list]

        # convert original SUM values into the right units
        for index, item in enumerate(analysis_names):

            if item == 'forest_loss':
                analysis_names[index] = 'forest_loss_ha'

                self.forest_loss['forest_loss'] = self.forest_loss.forest_loss.astype(float)
                self.forest_loss['forest_loss_ha'] = self.forest_loss['forest_loss'] / 10000

            if item == 'forest_extent':
                analysis_names[index] = 'forest_extent_ha'
                self.forest_extent['forest_extent_ha'] = self.forest_extent['forest_extent'] / 10000

            if item == 'biomass_weight':
                analysis_names[index] = 'biomass_weight_Tg'
                self.biomass_weight['biomass_weight_Tg'] = self.biomass_weight['biomass_weight'] / 1000000

        # join all the data frames together on Value and ID. Value is the tcd/loss code (41 = loss in 2001 at 1-10%tcd
        # or loss in 2001 at >30% tcd. ID is the unique ID of the feature in the shapefile
        merged = pd.concat([df.set_index(['VALUE', 'ID']) for df in df_list], axis=1)
        merged = merged.reset_index()

        # get the input shapefile into df format
        final_aoi_dbf = self.final_aoi.replace(".shp", ".dbf")
        # final_aoi_dbf = simpledbf.Dbf5(final_aoi_dbf)

        # convert dbf to pandas dataframe
        # final_aoi_df = final_aoi_dbf.to_dataframe()
        final_aoi_df = zstats_handler.gdf2pd(final_aoi_dbf)

        # reset index of final_aoi_df
        final_aoi_df = final_aoi_df.reset_index()

        if user_def_column_name:
            merged = final_columns.user_cols(user_def_column_name, final_aoi_df, merged, analysis_names)

        else:
            columns_to_keep = ['ID']
            columns_to_keep.extend(analysis_names)

            merged = merged[columns_to_keep]

        # Renaming the VALUE column of Loss data into year
        merged.rename(columns={'VALUE': 'year'}, inplace=True)
        merged['year'] = merged['year'] + 2000
        merged.sort_values(by='year')

        print('SAMPLE OF OUTPUT:')
        print (merged.head(5))

        final_output_csv = os.path.join(self.root_dir, 'result', '{}.csv'.format(output_file_name))
        merged.to_csv(final_output_csv, index=False)
