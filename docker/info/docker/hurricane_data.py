"""User script to get hurricane info relevant to the workflow
This script gether information about:
    - Hurricane track
    - Hurricane windswath
    - Hurricane event dates
    - Stations info for historical hurricane
"""

import sys
import logging
import pathlib
import argparse
from datetime import datetime, timedelta

import pandas as pd
import geopandas as gpd
from searvey.coops import COOPS_TidalDatum
from searvey.coops import COOPS_TimeZone
from searvey.coops import COOPS_Units
from shapely.geometry import box
from stormevents import StormEvent
from stormevents.nhc import VortexTrack


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(
    stream=sys.stdout,
    format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S')

EFS_MOUNT_POINT = pathlib.Path('~').expanduser() / f'app/io/output'

def main(args):

    name = args.name
    year = args.year
    date_out = EFS_MOUNT_POINT / args.date_range_outpath
    track_out = EFS_MOUNT_POINT / args.track_outpath
    swath_out = EFS_MOUNT_POINT / args.swath_outpath
    sta_dat_out = EFS_MOUNT_POINT / args.station_data_outpath
    sta_loc_out = EFS_MOUNT_POINT / args.station_location_outpath

    logger.info("Fetching hurricane info...")
    event = None
    # TODO: Use proper parameters. For now this is a quick hack to test
    if year == 0:
        event = StormEvent.from_nhc_code(name)
    else:
        event = StormEvent(name, year)
    logger.info("Fetching a-deck track info...")

    # TODO: Get user input for whether its forecast or now!
    is_forecast = False
    now = datetime.now()
    if now - event.start_date < timedelta(days=30):
        is_forecast = True

        temp_track = event.track(file_deck='a')
        adv_avail = temp_track.unfiltered_data.advisory.unique()
        adv_order = ['OFCL', 'HWRF', 'HMON', 'CARQ']
        advisory = adv_avail[0]
        for adv in adv_order:
            if adv in adv_avail:
                advisory = adv
                break

        # NOTE: Track taken from StormEvent object is up to now only.
        # See GitHub issue #57 for StormEvents
        #
        # TODO: Use proper parameters. For now this is a quick hack to test
        if year == 0:
            track = VortexTrack(
                name, file_deck='a', advisories=[advisory]
            )
        else:
            track = VortexTrack.from_storm_name(
                name, year, file_deck='a', advisories=[advisory]
            )
        windswath_dict = track.wind_swaths(wind_speed=34)

        df_dt = pd.DataFrame(columns=['date_time'])
        # Put both dates as now(), for pyschism to setup forecast
        df_dt['date_time'] = (now, now)

        logger.info(f"Fetching {advisory} windswath...")
        windswaths = windswath_dict[advisory]
        latest_advistory_stamp = max(pd.to_datetime(list(windswaths.keys())))
        windswath = windswaths[
            latest_advistory_stamp.strftime("%Y%m%dT%H%M%S")
        ]
        
        coops_ssh = None

    else:
        is_forecast = False

        logger.info("Fetching b-deck track info...")

        df_dt = pd.DataFrame(columns=['date_time'])
        df_dt['date_time'] = (event.start_date, event.end_date)

        logger.info("Fetching BEST windswath...")
        track = event.track(file_deck='b')
        windswath_dict = track.wind_swaths(wind_speed=34)
        # NOTE: event.start_date (first advisory date) doesn't
        # necessarily match the windswath key which comes from track
        # start date for the first advisory (at least in 2021!)
        windswaths = windswath_dict['BEST']
        latest_advistory_stamp = max(pd.to_datetime(list(windswaths.keys())))
        windswath = windswaths[
            latest_advistory_stamp.strftime("%Y%m%dT%H%M%S")
        ]

        logger.info("Fetching water level measurements from COOPS stations...")
        coops_ssh = event.coops_product_within_isotach(
            product='water_level', wind_speed=34,
            datum=COOPS_TidalDatum.NAVD,
            units=COOPS_Units.METRIC,
            time_zone=COOPS_TimeZone.GMT,
        )

    logger.info("Writing relevant data to files...")
    df_dt.to_csv(date_out)
    track.to_file(track_out)
    gs = gpd.GeoSeries(windswath)
    gdf_windswath = gpd.GeoDataFrame(
        geometry=gs, data={'RADII': len(gs) * [34]}, crs="EPSG:4326"
    )
    gdf_windswath.to_file(swath_out)
    if coops_ssh is not None:
        coops_ssh.to_netcdf(sta_dat_out, 'w')
        coops_ssh[['x', 'y']].to_dataframe().drop(columns=['nws_id']).to_csv(
                sta_loc_out, header=False, index=False)

        
if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "name", help="name of the storm", type=str)
    parser.add_argument(
        "year", help="year of the storm", type=int)

    parser.add_argument(
        "--date-range-outpath",
        help="output date range",
        type=pathlib.Path,
        required=True
    )

    parser.add_argument(
        "--track-outpath",
        help="output hurricane track",
        type=pathlib.Path,
        required=True
    )

    parser.add_argument(
        "--swath-outpath",
        help="output hurricane windswath",
        type=pathlib.Path,
        required=True
    )

    parser.add_argument(
        "--station-data-outpath",
        help="output station data",
        type=pathlib.Path,
        required=True
    )

    parser.add_argument(
        "--station-location-outpath",
        help="output station location",
        type=pathlib.Path,
        required=True
    )

    args = parser.parse_args()
    
    main(args)
