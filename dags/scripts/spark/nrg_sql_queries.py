bal_auth_query = """
SELECT DISTINCT
    `Balancing Authority` AS bal_auth,
    Region AS region,
    `Local Time at End of Hour` AS local_time,
    `UTC Time at End of Hour`AS utc_time,
    `Demand Forecast (MW)` AS demand_forecast,
    `Demand (MW) (Adjusted)` AS demand,
    `Net Generation (MW) (Adjusted)` AS net_generation,
	`Net Generation (MW) from Coal` AS net_generation_coal,
	`Net Generation (MW) from Natural Gas` AS net_generation_nat_gas,
	`Net Generation (MW) from Nuclear` AS net_generation_nuclear,
	`Net Generation (MW) from All Petroleum Products` AS net_generation_petro,
	`Net Generation (MW) from Hydropower and Pumped Storage` AS net_generation_hydro,
	`Net Generation (MW) from Solar` AS net_generation_solar,
	`Net Generation (MW) from Wind` AS net_generation_wind,
	`Net Generation (MW) from Other Fuel Sources` AS net_generation_other,
	`Net Generation (MW) from Unknown Fuel Sources` AS net_generation_unknown,
    EXTRACT(MONTH FROM TO_DATE(`Data Date`,'MM/dd/yyyy')) AS month,
    EXTRACT(YEAR FROM TO_DATE(`Data Date`,'MM/dd/yyyy')) AS year
FROM balancing_authorities
"""

weather_query = """
SELECT  B.Acronym AS bal_auth,
	A.*,
	EXTRACT(MONTH FROM TO_DATE(`date`,'yyyyMMdd')) AS month,
    EXTRACT(YEAR FROM TO_DATE(`date`,'yyyyMMdd')) AS year
FROM weather A LEFT OUTER JOIN
     locations B ON A.station_id=B.Stations

"""

time_query = """
SELECT DISTINCT
    `Local Time at End of Hour` AS local_time,
    EXTRACT(HOUR FROM TO_TIMESTAMP(`Local Time at End of Hour`,'MM/dd/yyyy HH12:MI:SS AM')) AS hour,
    EXTRACT(DAY FROM TO_TIMESTAMP(`Local Time at End of Hour`,'MM/dd/yyyy HH12:MI:SS AM')) AS day,
    EXTRACT(WEEK FROM TO_TIMESTAMP(`Local Time at End of Hour`,'MM/dd/yyyy HH12:MI:SS AM')) AS week,
    EXTRACT(MONTH FROM TO_TIMESTAMP(`Local Time at End of Hour`,'MM/dd/yyyy HH12:MI:SS AM')) AS month,
    EXTRACT(YEAR FROM TO_TIMESTAMP(`Local Time at End of Hour`,'MM/dd/yyyy HH12:MI:SS AM')) AS year,
    EXTRACT(DAYOFWEEK FROM TO_TIMESTAMP(`Local Time at End of Hour`,'MM/dd/yyyy HH12:MI:SS AM')) AS weekday
FROM balancing_authorities
"""

# Quality Check queries

def null_checker(table, column):
    """

    """
    query = f"""
    SELECT COUNT(*) FROM {table} WHERE {column} IS NULL;
    """
    return query

def count_rows(table):
    rows_count = f"""
    SELECT COUNT (*) FROM {table}
    """
    return count_rows

net_gen_qc = """
SELECT COUNT (*)
FROM (
    SELECT net_generation,
           net_generation_coal + net_generation_nat_gas + net_generation_nuclear +
           net_generation_petro + net_generation_hydro + net_generation_solar +
           net_generation_wind + net_generation_other + net_generation_unknown AS sum_of_parts
    FROM balancing_authorities ) main
WHERE main.net_generation != main.sum_of_parts
"""
