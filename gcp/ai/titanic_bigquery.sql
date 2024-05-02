DECLARE avg_age DEFAULT (SELECT AVG(Age) FROM `train_data.titanic`);

CREATE OR REPLACE TABLE `vertexai.titanic_processed` AS (
  SELECT
    LOWER(Sex) AS sex,
    LOWER(TRIM(SPLIT(SPLIT(Name, ",")[OFFSET(1)], ".")[OFFSET(0)])) AS title,
    Pclass AS pclass,
    CAST(COALESCE(Age, avg_age) AS INT64) AS age,
    Embarked AS embarked,
    Survived as survived
  FROM
    `train_data.titanic`
  WHERE
    NOT(Sex || Name || Pclass || Embarked || Survived IS NULL)
);


# Titanic train model
CREATE OR REPLACE MODEL `vertexai.titanic_model`
OPTIONS(
  MODEL_TYPE = "LOGISTIC_REG",
  EARLY_STOP = TRUE,
  MIN_REL_PROGRESS = 0.01,
  NUM_TRIALS = 10,
  MAX_PARALLEL_TRIALS = 2,
  DATA_SPLIT_METHOD = "AUTO_SPLIT",
  AUTO_CLASS_WEIGHTS = TRUE,
  CATEGORY_ENCODING_METHOD = "ONE_HOT_ENCODING",
  INPUT_LABEL_COLS = ["survived"]
) AS (
  SELECT
    *
  FROM
    `vertexai.titanic_processed`
);


# Titanic predict
SELECT
  *
FROM
  ML.PREDICT(
    MODEL `vertexai.titanic_model`,
    (
      SELECT
        "female" AS sex,
        "mrs" AS title,
        3 AS pclass,
        3 AS age,
        "Q" AS embarked
    ),
    STRUCT(
      0.6 AS threshold,
      7 AS trial_id
    )
  );

# Almacenamos el modelo en GCS
EXPORT MODEL `vertexai.titanic_model`
OPTIONS(URI = 'gs://bucket/model/titanic_model');