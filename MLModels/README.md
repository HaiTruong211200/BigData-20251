# Training and testing ML models's report

This note explains why a Decision Tree (DT) classifier is a good fit for our flight departure delay classification task and how it is integrated into our Spark ML pipeline.

## Problem summary

We classify flights into three classes based on `DEP_DELAY`:

- 0: on time or early (<= 0 minutes)
- 1: delayed 1–30 minutes
- 2: delayed > 30 minutes

Features used:

- Numeric: `QUARTER`, `MONTH`, `DAY_OF_MONTH`, `DAY_OF_WEEK`, `DISTANCE`, `CRS_DEP_TIME`
- Categorical: `OP_UNIQUE_CARRIER`, `ORIGIN`, `DEST` (indexed and one-hot encoded)

Train/test split: 80/20. Metrics reported: Accuracy, F1-micro, F1-macro.

## Why choose a Decision Tree

- Interpretability: Easy to explain decision paths to stakeholders (clear rules and thresholds).
- Handles mixed feature types: Works well with our one-hot encoded categorical features and numeric features without scaling.
- Captures non-linearities and interactions: Learns splits that model complex relationships between schedule, route, and carrier.
- Fast training and prediction: Lower complexity than ensembles; suitable for iteration and deployment.
- Minimal preprocessing: No need for normalization/standardization.

Limitations and mitigations:

- Overfitting on deep trees → control with `maxDepth`, `minInfoGain`, `minInstancesPerNode` and validation.
- Class imbalance sensitivity → consider using `weightCol` or resampling if needed.

## Model pipeline

The Spark ML pipeline used in the notebook:

- `StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `DecisionTreeClassifier(maxDepth=16)`

Example (simplified):

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline

cat_cols = ['OP_UNIQUE_CARRIER', 'ORIGIN', 'DEST']
indexer = StringIndexer(inputCols=cat_cols,
                        outputCols=[c + 'Index' for c in cat_cols])
encoder = OneHotEncoder(inputCols=[c + 'Index' for c in cat_cols],
                        outputCols=[c + 'Vec' for c in cat_cols])
assembler = VectorAssembler(
    inputCols=['QUARTER','MONTH','DAY_OF_MONTH','DAY_OF_WEEK','OP_UNIQUE_CARRIERVec','ORIGINVec','DESTVec','DISTANCE','CRS_DEP_TIME'],
    outputCol='features'
)
clf = DecisionTreeClassifier(featuresCol='features', labelCol='LABEL', maxDepth=16)
pipeline = Pipeline(stages=[indexer, encoder, assembler, clf])
model = pipeline.fit(train)
```

## Evaluation

- Report Accuracy, F1-micro, and F1-macro on both train and test sets.
- Inspect prediction distribution and a confusion matrix to understand per-class performance.
- Watch for signs of overfitting (large train–test gap); reduce `maxDepth` or increase `minInfoGain` if needed.

## Saved artifacts

A trained Spark ML `PipelineModel` for DT is saved at:

- `MLModels/Model/DT/`

You can load and use it with:

```python
from pyspark.ml.pipeline import PipelineModel
model_DT = PipelineModel.load('MLModels/Model/DT')
predictions = model_DT.transform(new_df)
```

Label mapping reminder:

- 0 → on time/early
- 1 → 1–30 min delay
- 2 → > 30 min delay
