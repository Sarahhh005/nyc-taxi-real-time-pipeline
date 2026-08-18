Select Count(*) as total_trips
from NYC_TAXI.taxi_trips;

Select AVG(fare_amount) as avg_fare
from NYC_TAXI.taxi_trips;

Select AVG(tip_amount) as avg_tip
from NYC_TAXI.taxi_trips;
Select
    pickup_location_id,
    AVG(tip_amount) as avg_tip
from NYC_TAXI.taxi_trips
Group by pickup_location_id
Order by avg_tip DESC;
