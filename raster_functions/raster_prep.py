import os
import arcpy
import csv


def remap_threshold(geodatabase, threshold):

    # Apply a remap function to the tcd mosaic. Turns values of TCD into bins recoded to
    # values of 40, 80, etc.
    print("remaping mosaics in {} to {}".format(geodatabase, threshold))
    this_dir = os.path.dirname(os.path.abspath(__file__))
    remap_func = os.path.join(this_dir, "remap_gt" + str(threshold) + ".rft.xml")
    tcd_mosaic = os.path.join(geodatabase, "tcd")

    arcpy.EditRasterFunction_management(tcd_mosaic, "EDIT_MOSAIC_DATASET", "REPLACE", remap_func)
