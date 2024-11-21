import meteostat
from datetime import datetime
import numpy as np



# Station nearest a location with largest dataset
def nws_station_nearest_location(
    latitude: float,
    longitude: float, 
    radius_miles: float,
    largest_dataset: bool = True
):
    all_stations = meteostat.Stations()
    stations = all_stations.nearby(
        lat= latitude,
        lon= longitude,
        radius= (1609.344*radius_miles)
    )

    stations = stations.fetch()
    
    if largest_dataset:
        stations = stations[stations["distance"] <= stations["distance"].median()]
        return stations["hourly_start"].idxmin()

    return stations["distance"].idxmin()


station = nws_station_nearest_location(44.08, -69.8, 50)


# Current year to date heating degree days (hdd)
# https://portfoliomanager.energystar.gov/pm/glossary#DegreeDays
def ytd_hdd_for_location(
    station: str,
    start: datetime,
    end: datetime
):
    data = meteostat.Daily(
        start=start,
        end= end,
        loc= station
    ).fetch()
    
    data["tavg_f"] = data["tavg"]*1.8 + 32.
    data["hdd"] = np.where(data["tavg_f"] <=65., 65. - data["tavg_f"], 0.)
    
    return data
    

# today's observations for that station
today = datetime.now()
print(today)
data = meteostat.Hourly(
    start= datetime(today.year, today.month, today.day, 0),
    end= datetime(today.year, today.month, today.day, today.hour),
    loc= station
).fetch()



hdd = ytd_hdd_for_location(station, datetime(today.year, 1, 1),
    end= datetime(today.year, today.month, today.day, today.hour))

print(hdd)
print(f'Cumulative HDD for NWS Station ({station}) in {today.year} as of {today}: {hdd["hdd"].sum()}')
print(f'Coldest Day for NWS Station ({station}) in {today.year} as of {today}: {hdd["hdd"].idxmax()} with average temp (F) {hdd["tavg_f"][hdd["hdd"].idxmax()]}')