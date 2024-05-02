-- Se crea el modelo almacenado en Cloud Storage
CREATE OR REPLACE MODEL `vertexai.nnlm_model`
OPTIONS (
  model_type = 'TENSORFLOW',
  model_path = 'gs://bucket/model/nnlm_model/*');

-- Se usa el modelo para obtener los embedding de las reviews de imdb
CREATE OR REPLACE TABLE `vertexai.imdb_reviews_embedding_nnlm` AS
SELECT
  *
FROM
  ML.PREDICT(
    MODEL `vertexai.nnlm_model`,
    (SELECT review as content, sentiment AS label FROM `train_data.imdb_review`)
);

-- Se crea el modelo que clasifica segun el embedding la review como positiva|negativa
CREATE OR REPLACE MODEL `vertexai.imdb_review_sentiment_classifier`
  OPTIONS(
    MODEL_TYPE="LOGISTIC_REG",
    AUTO_CLASS_WEIGHTS=TRUE,
    INPUT_LABEL_COLS=["label"],
    MAX_ITERATIONS=10,
    DATA_SPLIT_METHOD="AUTO_SPLIT"
  ) AS
  SELECT
    text_embedding,
    label
  FROM `vertexai.imdb_reviews_embedding_nnlm`;


-- Se utiliza el modelo entrenado para realizar predicciones
WITH embedding_review AS (
  SELECT
    *
  FROM
    ML.PREDICT(
      MODEL `vertexai.nnlm_model`,
      (SELECT "The movie was horrible I will never recommend to anyone." AS content)
  )
)

SELECT
  *
FROM
  ML.PREDICT(
    MODEL `vertexai.imdb_review_sentiment_classifier`,
    (SELECT * FROM embedding_review)
);

-- Creando modelos con conexion remota
CREATE OR REPLACE MODEL `vertexai.text_embedding`
  REMOTE WITH CONNECTION `project.us-central1.vertexai-models`
  OPTIONS( ENDPOINT = "textembedding-gecko@latest");

-- Usando modelo con conexion remota para obtener embeedings
CREATE TABLE `vertexai.imdb_reviews_embedding` AS (
  SELECT
    *
  FROM
    ML.GENERATE_EMBEDDING(
      MODEL `vertexai.text_embedding`,
      (SELECT review as content, sentiment AS label FROM `train_data.imdb_review` LIMIT 1000),
      STRUCT(TRUE AS flatten_json_output)

    )
);
    
-- Creamos un KMEANS cluster model
CREATE OR REPLACE MODEL `vertexai.imdb_reviews_cluster`
OPTIONS (
  MODEL_TYPE = "KMEANS",
  MAX_ITERATIONS = 10,
  EARLY_STOP = TRUE,
  MIN_REL_PROGRESS = 0.01,
  NUM_CLUSTERS = 5,
  DISTANCE_TYPE = "COSINE"
) AS (
  SELECT
    text_embedding
  FROM
    `vertexai.imdb_reviews_embedding_nnlm`
);

-- Utilizamos el modelo
SELECT
  *
FROM 
  ML.PREDICT(
    MODEL `project.vertexai.imdb_reviews_cluster`,
    (SELECT
      text_embedding
    FROM
      ML.PREDICT(
        MODEL `vertexai.nnlm_model`,
        (
          SELECT
            "Jack Sparrow is the best movie" AS content
        )
      )));

-- Calculamos similitud entre las distintas reviews y una custom.
SELECT
  my_review.content,
  reviews.content,
  ML.DISTANCE(reviews.text_embedding, my_review.text_embedding, "COSINE") AS distance
FROM 
  `vertexai.imdb_reviews_embedding_nnlm` as reviews,
    (SELECT
      text_embedding,
      content
    FROM
      ML.PREDICT(
        MODEL `vertexai.nnlm_model`,
        (
          SELECT
            "Romantic film, where the focus is on the reconstruction of a failed relationship" AS content
        )
      )) as my_review
ORDER BY distance DESC
LIMIT 10;