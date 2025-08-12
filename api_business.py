import csv
import time
import random
from datetime import date, timedelta, datetime
from amadeus import Client

iata_to_city={'DEL': 'Delhi',
    'BOM': 'Mumbai',
    'MAA': 'Chennai',
    'BLR': 'Bangalore',
    'HYD': 'Hyderabad'}

carrier_to_airline={'AI': 'Air_India',
    'UK': 'Vistara',
    '6E': 'Indigo',
    'SG': 'SpiceJet',
    'G8': 'Go_First'}

def time_of_day(dept_time):
    hour=dept_time.hour
    if 4 <= hour < 8:
        return "Early_Morning"
    elif 8 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 16:
        return "Afternoon"
    elif 16 <= hour < 20:
        return "Evening"
    elif 20 <= hour < 24:
        return "Night"
    else:
        return "Late_Night"

def duration_to_hours(durations):
    hours, minutes = 0, 0
    dur=durations.replace('PT', '')
    if 'H' in dur:
        hours = int(dur.split('H')[0])
        if 'M' in dur:
            minutes = int(dur.split('H')[1].replace('M', '') or 0)
    elif 'M' in dur:
        minutes = int(dur.replace('M', ''))
    return round(hours + minutes/60, 2)

def get_stops(segments):
    stops_count=len(segments)-1
    if stops_count==0:
        return "zero"
    elif stops_count==1:
        return "one"
    else:
        return "two_or_more"

amadeus=Client(client_id='Zp69gfErG6N7bjUPlVI86spBCNGNvW69', client_secret='sMhMXXwPOOEgjaAU')
source_cities=['DEL', 'BOM', 'MAA', 'BLR', 'HYD']
destination_cities=['DEL', 'BOM', 'MAA', 'BLR', 'HYD']

today=date.today()
dates=[today + timedelta(days=i) for i in range(60)]

local_file="/home/sunbeam/Big_Data_Project_data/data/business.csv"
with open(local_file, 'w', newline='') as f:
    writer=csv.writer(f)
    writer.writerow(['airline', 'flight', 'source_city', 'departure_time', 'stops',
        'arrival_time', 'destination_city', 'class', 'duration', 'days_left', 'price'])
    for src in source_cities:
        for dst in destination_cities:
            if src==dst:
                continue
            for dep_date in dates:
                print(f"Fetching {src} → {dst} | BUSINESS | {dep_date}")
                response=amadeus.shopping.flight_offers_search.get(
                    originLocationCode=src,
                    destinationLocationCode=dst,
                    departureDate=dep_date.isoformat(),
                    adults=1,
                    travelClass="BUSINESS",
                    max=20
                )
                for offer in response.data:
                    segments=offer['itineraries'][0]['segments']
                    dep_datetime=datetime.fromisoformat(segments[0]['departure']['at'])
                    arr_datetime=datetime.fromisoformat(segments[-1]['arrival']['at'])

                    airline_code=segments[0]['carrierCode']
                    flight_number=segments[0]['number'].zfill(4)
                    airline_name=carrier_to_airline.get(airline_code, airline_code)

                    flight=f"{airline_code}-{flight_number}"
                    source_city=iata_to_city[src]
                    destination_city=iata_to_city[dst]
                    dep_time=time_of_day(dep_datetime)
                    arr_time=time_of_day(arr_datetime)
                    stops=get_stops(segments)
                    duration_hours=duration_to_hours(offer['itineraries'][0]['duration'])
                    days_left=(dep_date - today).days
                    price=round(float(offer['price']['total']) *90)

                    writer.writerow([airline_name, flight, source_city, dep_time, stops,
                        arr_time, destination_city, "Business",
                        duration_hours, days_left, price])
                time.sleep(0.5)