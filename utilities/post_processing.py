import pandas as pd
import logging

def biomass_to_mtc02(layer):
    layer.emissions.SUM = layer.emissions.SUM.astype(float)
    layer.emissions['emissions_mtc02'] = layer.emissions.SUM * 3.67 * .5 / 1000000

    del layer.emissions['emissions']

    return layer.emissions


def value_to_tcd_year(value):

    remap_dict = {
        1: [{'tcd': '1-10 %', 'sub': 40}],
        2: [{'tcd': '11-15 %', 'sub': 80}],
        3: [{'tcd': '16-20 %', 'sub': 120}],
        4: [{'tcd': '21-25 %', 'sub': 160}],
        5: [{'tcd': '26-30 %', 'sub': 200}],
        6: [{'tcd': '31-50 %', 'sub': 240}],
        7: [{'tcd': '51-75 %', 'sub': 280}],
        8: [{'tcd': '76-100 %', 'sub': 320}]
    }

    # divide the coded value by interval. if its 1.175, use int to get 1
    div = int(value / 40)

    # look up that value to get TCD
    tcd = remap_dict[div][0]['tcd']

    # find this value which is subtraced from the coded value. 47-40 = 7. Gets the year
    sub = remap_dict[div][0]['sub']

    year = 2000 + (value - sub)

    if year == 2000:
        year = "no loss"

    return tcd, year

def value_to_tcd_year_each(value):
    """
    Returns all the value of zonal stats into tcd and year, which preserve the value of tcd 0 - 100
    value : value from zonal stats (multiplied by forest (x1) and added with remapping tcd raster) 
    """
    """
    You cannot do process forest_extent and forest_loss at the same task to elaborate within this function
    since it will give you: IndexError: list index out of range
    In your remap function:
        loss -> +tcd xforest
        forest -> xtcd
        
        then, forest_loss -> +tcd xtcd
        max_number -> 4040 x 4040 = 16321600

    """

    remap_dict = [dict(tcd=i, sub=(i+1)*40) for i in range(101)]

    div = int(value / 40)

    # accessing the value by minus 1 since python slicing start with 0 index
    tcd = remap_dict[div-1]['tcd']
    sub = remap_dict[div-1]['sub']

    year = 2000 + (value - sub)
    logging.info("YEAR: {}".format(year))
    
    if year == 2000:
        year = "no loss"

    return tcd, year
