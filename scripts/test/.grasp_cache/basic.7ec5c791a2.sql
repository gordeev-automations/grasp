CREATE MATERIALIZED VIEW "only_a" AS
SELECT DISTINCT "ft15:fact"."a" AS "a"
  FROM "fact" AS "ft15:fact"
GROUP BY "ft15:fact"."a"
CREATE MATERIALIZED VIEW "only_b" AS
SELECT DISTINCT "ft19:fact"."b" AS "b"
  FROM "fact" AS "ft19:fact"
GROUP BY "ft19:fact"."b"
