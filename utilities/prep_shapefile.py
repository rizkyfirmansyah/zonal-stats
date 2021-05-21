import os
import numpy as np
import math
import arcpy
import logging

def intersect_gp(final_aoi, intersect, intersect_col, workspace):

    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = workspace

    intersected_file = "intersect.shp"
    out_file = "shapefile.shp"
    intersect_col = ';'.join(intersect_col)

    arcpy.Intersect_analysis([final_aoi, intersect], intersected_file)
    try:
        arcpy.Dissolve_management(in_features=intersected_file, out_feature_class=out_file, dissolve_field=intersect_col, statistics_fields="", multi_part="MULTI_PART", unsplit_lines="DISSOLVE_LINES")
    except:
        logging.info(arcpy.GetMessages(2))

    print("intersected with boundary\n")

    return intersected_file


def zonal_stats_mask(final_aoi, i, intersect, intersect_col):
    arcpy.env.overwriteOutput = True
    workspace = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "shapefile")

    exp = """"FID" = {}""".format(int(i))

    if intersect:
       intersect_gp(final_aoi, intersect, intersect_col, workspace)

    mask = os.path.join(workspace, "shapefile.shp")

    arcpy.FeatureClassToFeatureClass_conversion(mask, workspace, "zonal_shapefile.shp", exp)

    return mask


def delete_database(database_name):
    tables_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    zstats_results_db = os.path.join(tables_dir, 'tables', database_name)
    
    if os.path.exists(zstats_results_db):
        print("deleting database")
        os.remove(zstats_results_db)


def build_analysis(analysis_requested):
    # if analysis is emissions, we still need to run forest_loss
    if "biomass_weight" in analysis_requested or "emissions" in analysis_requested:
        if not "forest_loss" in analysis_requested:
            analysis_requested.append('forest_loss')

    return analysis_requested