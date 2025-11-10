import argparse
import json
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.pipeline import PipelineModel

try:
    from sklearn.metrics import f1_score, classification_report, confusion_matrix
    SKLEARN_AVAILABLE = True
except ImportError: 
    SKLEARN_AVAILABLE = False


def parse_args():
    p = argparse.ArgumentParser(description="Train Decision Tree pipeline for flight delay classification")
    p.add_argument("--input-csv", required=True, help="Path to input CSV dataset")
    p.add_argument("--model-dir", default="MLModels/Model/DT", help="Directory to save the trained PipelineModel")
    p.add_argument("--max-depth", type=int, default=16, help="Max depth for DecisionTreeClassifier")
    p.add_argument("--min-info-gain", type=float, default=0.0, help="Minimum info gain for a split")
    p.add_argument("--test-ratio", type=float, default=0.2, help="Test set ratio (0-1)")
    p.add_argument("--metrics-file", default="metrics_dt.json", help="Filename for metrics JSON (saved inside model-dir)")
    p.add_argument("--app-name", default="FlightDelayDT", help="Spark application name")
    return p.parse_args()


def build_spark(app_name: str) -> SparkSession:
    return (SparkSession.builder.appName(app_name)
            .config("spark.executor.memory", "4g")
            .getOrCreate())


def load_dataset(spark: SparkSession, path: str):
    df = spark.read.csv(path, header=True, inferSchema=True)
    if "FL_DATE" in df.columns and all(c in df.columns for c in ["OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "OP_CARRIER_FL_NUM"]):
        df = df.withColumn('DATE', F.split('FL_DATE', ' ')[0]) \
               .withColumn('ID', F.concat(F.col('DATE'), F.lit('_'), F.col('OP_UNIQUE_CARRIER'), F.lit('_'),
                                          F.col('ORIGIN'), F.lit('_'), F.col('DEST'), F.lit('_'), F.col('OP_CARRIER_FL_NUM')))
    return df


def add_label(df):
    if 'DEP_DELAY' not in df.columns:
        raise ValueError("DEP_DELAY column missing from dataset")
    df = df.withColumn('DEP_DELAY', F.when(F.col('DEP_DELAY') <= 0, 0).otherwise(F.col('DEP_DELAY'))) \
           .withColumn('DEP_DELAY', F.when((F.col('DEP_DELAY') > 0) & (F.col('DEP_DELAY') <= 30), 1).otherwise(F.col('DEP_DELAY'))) \
           .withColumn('DEP_DELAY', F.when(F.col('DEP_DELAY') > 30, 2).otherwise(F.col('DEP_DELAY'))) \
           .withColumnRenamed('DEP_DELAY', 'LABEL') \
           .withColumn('LABEL', F.col('LABEL').cast('INT'))
    return df


def select_columns(df):
    needed = ['QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'OP_UNIQUE_CARRIER', 'ORIGIN', 'DEST', 'DISTANCE', 'CRS_DEP_TIME', 'LABEL']
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df.select(*needed)


def build_pipeline(max_depth: int, min_info_gain: float) -> Pipeline:
    cat_cols = ['OP_UNIQUE_CARRIER', 'ORIGIN', 'DEST']
    indexer = StringIndexer(inputCols=cat_cols, outputCols=[c + 'Index' for c in cat_cols], handleInvalid='keep')
    encoder = OneHotEncoder(inputCols=[c + 'Index' for c in cat_cols], outputCols=[c + 'Vec' for c in cat_cols])
    assembler = VectorAssembler(inputCols=['QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'OP_UNIQUE_CARRIERVec', 'ORIGINVec', 'DESTVec', 'DISTANCE', 'CRS_DEP_TIME'],
                                outputCol='features')
    clf = DecisionTreeClassifier(featuresCol='features', labelCol='LABEL', maxDepth=max_depth, minInfoGain=min_info_gain)
    return Pipeline(stages=[indexer, encoder, assembler, clf])


def evaluate(pred_df, label_col='LABEL', prediction_col='prediction'):
    evaluator = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol=prediction_col, metricName='accuracy')
    accuracy = evaluator.evaluate(pred_df)

    pdf = pred_df.select(prediction_col, label_col).toPandas()
    metrics = {
        'accuracy': accuracy,
    }
    if SKLEARN_AVAILABLE:
        f1_micro = f1_score(pdf[label_col], pdf[prediction_col], average='micro')
        f1_macro = f1_score(pdf[label_col], pdf[prediction_col], average='macro')
        report = classification_report(pdf[label_col], pdf[prediction_col], target_names=['0', '1', '2'], digits=4, output_dict=True)
        cm = confusion_matrix(pdf[label_col], pdf[prediction_col]).tolist()
        metrics.update({
            'f1_micro': f1_micro,
            'f1_macro': f1_macro,
            'classification_report': report,
            'confusion_matrix': cm,
        })
    return metrics


def main():
    args = parse_args()
    spark = build_spark(args.app_name)
    print(f"[INFO] Loading dataset: {args.input_csv}")
    raw = load_dataset(spark, args.input_csv)
    print(f"[INFO] Raw count: {raw.count()}")

    df = add_label(raw)
    df = select_columns(df).dropna()
    print(f"[INFO] After selection count: {df.count()}")

    train_ratio = 1.0 - args.test_ratio
    train_df, test_df = df.randomSplit([train_ratio, args.test_ratio], seed=2022)
    print(f"[INFO] Train count: {train_df.count()} | Test count: {test_df.count()}")

    pipeline = build_pipeline(args.max_depth, args.min_info_gain)
    print("[INFO] Fitting pipeline...")
    model = pipeline.fit(train_df)

    print("[INFO] Evaluating train set...")
    train_pred = model.transform(train_df)
    train_metrics = evaluate(train_pred)

    print("[INFO] Evaluating test set...")
    test_pred = model.transform(test_df)
    test_metrics = evaluate(test_pred)

    os.makedirs(args.model_dir, exist_ok=True)
    model.write().overwrite().save(args.model_dir)
    print(f"[INFO] Model saved to {args.model_dir}")

    metrics_path = os.path.join(args.model_dir, args.metrics_file)
    metrics_blob = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'params': {
            'max_depth': args.max_depth,
            'min_info_gain': args.min_info_gain,
            'test_ratio': args.test_ratio,
        },
        'train': train_metrics,
        'test': test_metrics,
        'label_mapping': {"0": "on time/early", "1": "1-30 min delay", "2": ">30 min delay"}
    }
    with open(metrics_path, 'w') as f:
        json.dump(metrics_blob, f, indent=2)
    print(f"[INFO] Metrics written to {metrics_path}")

    def fmt(m):
        return f"acc={m.get('accuracy'):.4f} f1_micro={m.get('f1_micro', float('nan')):.4f} f1_macro={m.get('f1_macro', float('nan')):.4f}" if 'accuracy' in m else ''
    print(f"[SUMMARY] Train: {fmt(train_metrics)} | Test: {fmt(test_metrics)}")

    spark.stop()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
