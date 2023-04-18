
                  SELECT BANDEIRA, COUNT(*) AS TOTAL FROM tb_ticket

                  --
                   GROUP BY BANDEIRA
                  UNION ALL
                  SELECT BANDEIRA, COUNT(*) AS TOTAL FROM tb_alelo

                  --
                   GROUP BY BANDEIRA
                  UNION ALL
                  SELECT BANDEIRA, COUNT(*) AS TOTAL FROM tb_vr

                  --
                   GROUP BY BANDEIRA
                  ;
                  
SELECT * FROM tb_ticket
WHERE BANDEIRA IS NULL
ORDER BY BANDEIRA ASC 
LIMIT 20;

DELETE FROM tb_vr
WHERE BANDEIRA IS NULL;