# Data Contract (Published Tables)

All Retool/dashboard components should use tables in `data/published/`.

## dim_zones
**Grain:** 1 row per taxi zone  
**Purpose:** lookup for borough/zone/service_zone  
**Columns:** LocationID, Borough, Zone, service_zone

## mart_hour
**Grain:** 1 row per pickup hour (0–23)  
**Purpose:** hourly demand + average metrics  
**Columns:** pickup_hour, trips, avg_fare, avg_distance

## mart_dow
**Grain:** 1 row per day of week  
**Purpose:** weekly seasonality  
**Columns:** pickup_dow, trips, avg_fare, avg_distance

## mart_zone
**Grain:** 1 row per pickup zone  
**Purpose:** top demand zones + revenue/tip patterns  
**Columns:** PU_Borough, PU_Zone, trips, avg_total, avg_tip, avg_distance

## mart_quality_hour
**Grain:** 1 row per pickup hour  
**Purpose:** transparency on data quality (payment_type=0)  
**Columns:** pickup_hour, trips, unknown_payments, unknown_payment_rate
