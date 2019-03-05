SELECT `region`, `population`, `state`, `size`
FROM (SELECT `region`, `population`, `state`, `size`
FROM (SELECT *
FROM (SELECT `region`, `population`, `state`, `size`, `region` || ' || `population` AS `concatKey`
FROM (SELECT DISTINCT *
FROM (SELECT DISTINCT *
FROM (SELECT `region`, `population`, `state`, `size`
FROM (SELECT DISTINCT *
FROM (SELECT DISTINCT *
FROM (SELECT `region`, `population`, `state`, `size`
FROM (SELECT `division`, `region`, `state`, `population`, `size`
FROM (SELECT *
FROM (SELECT `division`, `region`, `state`, `population`, `size`, `region` || ' || `population` AS `concatKey`
FROM (SELECT *
FROM `state`))
WHERE (`concatKey` IN ('Sout3615.0', 'Wes365.0', 'Wes2280.0', 'Sout2110.0', 'Wes21198.0', 'Wes2541.0', 'Northeas3100.0', 'Sout579.0', 'Sout8277.0', 'Sout4931.0'))))
UNION ALL
SELECT `region`, `population`, `state`, `size`
FROM (SELECT `division`, `region`, `state`, `population`, `size`
FROM (SELECT *
FROM (SELECT `division`, `region`, `state`, `population`, `size`, `region` || ' || `population` AS `concatKey`
FROM (SELECT *
FROM `state`))
WHERE (`concatKey` IN ('Sout579.0', 'Sout8277.0', 'Sout4931.0', 'Wes868.0', 'Wes813.0', 'North Centra11197.0', 'North Centra5313.0', 'North Centra2861.0')))))))
UNION ALL
SELECT `region`, `population`, `state`, `size`
FROM (SELECT `division`, `region`, `state`, `population`, `size`
FROM (SELECT *
FROM (SELECT `division`, `region`, `state`, `population`, `size`, `region` || ' || `population` AS `concatKey`
FROM (SELECT *
FROM `state`))
WHERE (`concatKey` IN ('North Centra2861.0', 'North Centra2280.0', 'Sout9111.0'))))))))
WHERE (`concatKey` IN ('Sout3615.0', 'Wes365.0', 'Wes2280.0', 'Sout2110.0', 'Wes21198.0'))))
