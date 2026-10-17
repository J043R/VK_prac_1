-- Вариант 3 преподавателя. Масштаб по умолчанию sf1.
-- Runner добавляет orderkey последним ключом сортировки для разрешения равенств.
SELECT
  c.name,
  c.custkey,
  o.orderkey,
  o.orderdate,
  o.totalprice,
  sum(l.quantity) AS total_quantity
FROM tpch.sf1.customer AS c
JOIN tpch.sf1.orders AS o ON c.custkey = o.custkey
JOIN tpch.sf1.lineitem AS l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (
  SELECT orderkey
  FROM tpch.sf1.lineitem
  GROUP BY orderkey
  HAVING sum(quantity) > 300
)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100;
