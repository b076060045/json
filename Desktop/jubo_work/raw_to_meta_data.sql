CREATE
OR REPLACE TABLE `jubo-ai.meta_prod_knowledgegraph.patientKG_{target}` as (
    SELECT
        {columns}
    FROM
        `jubo-ai.raw_prod_datahub_mongo.{target}` AS A
        INNER JOIN (
            SELECT
                A.{id_key},
                STRING(
                    TIMESTAMP_SUB(
                        TIMESTAMP(MAX(A.{time_key})),
                        INTERVAL {time_range} DAY
                    )
                ) AS timestamp_start,
                MAX(A.{time_key}) AS timestamp_end
            FROM
                `jubo-ai.raw_prod_datahub_mongo.{target}` AS A
            WHERE
                A.{time_key} != "None"
                AND A.{time_key} != "NULL"
            GROUP BY
                A.{id_key}
        ) AS SQ ON (
            (A.{id_key} = SQ.{id_key})
            AND (
                A.{time_key} BETWEEN SQ.timestamp_start
                AND SQ.timestamp_end
            )
        )
)
