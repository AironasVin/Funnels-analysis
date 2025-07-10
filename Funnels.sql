--CTE for unique events
WITH unique_events AS
(
  SELECT 
    user_pseudo_id,
    event_name,
    country,
    MIN(event_timestamp) AS Timestamp
  FROM `tc-da-1.turing_data_analytics.raw_events`
  GROUP BY 1,2,3
),
--CTE for finding top 3 countries
top_countries AS
(
  SELECT 
    country,
    COUNT(Timestamp) AS Events
  FROM unique_events
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 3
),
--CTE to create funnel/categorize specific events to funnel steps
funnel AS
(
  SELECT
    u.country,
    u.event_name,
    COUNT(event_name) AS event_count,
    (
    CASE event_name
      WHEN 'page_view' THEN 1
      WHEN 'view_item' THEN 2
      WHEN 'add_to_cart' THEN 3
      WHEN 'begin_checkout' THEN 4
      WHEN 'purchase' THEN 5
      ELSE 0
    END
    ) AS event_order
  FROM unique_events u
  JOIN top_countries t
  ON u.country = t.country
  GROUP BY 1,2
)
SELECT 
  event_order,
  event_name,
  SUM(CASE WHEN country = 'United States' THEN event_count END) AS United_States_events,
  SUM(CASE WHEN country = 'India' THEN event_count END) AS India_events,
  SUM(CASE WHEN country = 'Canada' THEN event_count END) AS Canada_events,
  SUM(event_count)/MAX(SUM(event_count)) OVER () AS Full_perc,
  SUM(CASE WHEN country = 'United States' THEN event_count END)/(SELECT MAX(event_count) FROM funnel WHERE country = 'United States') AS United_States_perc_drop,
  SUM(CASE WHEN country = 'India' THEN event_count END)/(SELECT MAX(event_count) FROM funnel WHERE country = 'India') AS India_perc_drop,
  SUM(CASE WHEN country = 'Canada' THEN event_count END)/(SELECT MAX(event_count) FROM funnel WHERE country = 'Canada') AS Canada_perc_drop
FROM funnel
WHERE event_order != 0
GROUP BY 1, 2
ORDER BY 1
