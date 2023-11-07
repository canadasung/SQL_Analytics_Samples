-- PostgreSQL
-- Check Duplicates
SELECT *
FROM user_data
WHERE user_id IN (
 SELECT user_id
 FROM user_data
 GROUP BY user_id
 HAVING COUNT(user_id)>1
);
