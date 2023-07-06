CREATE
OR REPLACE TABLE `jubo-ai.raw_prod_datahub_mongo.{target}_clean` as (
    SELECT
        A.*,
        ROUND(
            SAFE_DIVIDE(
                SAFE_CAST(A.weight AS NUMERIC),
                POW(
                    SAFE_MULTIPLY(SAFE_CAST(A.height AS NUMERIC), 0.01),
                    2
                )
            ),
            1
        ) AS bmi,
        B.age
    FROM
        `jubo-ai.raw_prod_datahub_mongo.{target}` AS A
        LEFT JOIN(
            SELECT
                _id,
                FLOOR(
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(birthday), DAY) / 365
                ) AS age
            FROM
                `jubo-ai.raw_prod_datahub_mongo.{target}`
            WHERE
                birthday != 'NULL'
        ) AS B ON A._id = B._id
)