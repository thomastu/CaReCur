import altair as alt
import pandas as pd

from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV

from loguru import logger
from src.conf import settings


INPUT_DIR = settings.DATA_DIR / "processed/training/"
OUTPUT_DIR = settings.DATA_DIR / "processed/results/"


class BaseModel:
    def __init__(self, data, y_col, n_vars, c_vars):
        self.X = data[n_vars + c_vars].copy()
        self.y = data[y_col]
        self.n_vars = n_vars
        self.c_vars = c_vars

    def build_model(self, regressor, numeric_vars, categorical_vars):
        """Build a generic model
        """
        numeric_transformer = Pipeline(
            steps=[
                # Convert all numeric values to units of variance.
                # This helps us avoid weird number conditions when using
                # sklearn with very big and very small numbers.
                ("scaler", StandardScaler())
            ]
        )
        categorical_transformer = Pipeline(
            steps=[
                # Use one-hot encoding to handle categoricals like weekday and month
                ("onehot", OneHotEncoder(handle_unknown="ignore"))
            ]
        )

        preprocessor = ColumnTransformer(
            transformers=[
                ("num", numeric_transformer, numeric_vars),
                ("cat", categorical_transformer, categorical_vars),
            ]
        )

        clf = Pipeline(
            steps=[("preprocessor", preprocessor), ("classifier", regressor)]
        )
        return clf

    def model(self):
        """Define the relevant model for this class
        """
        raise NotImplementedError

    def fit_train_predict(self, clf, test_size=0.2):
        """
        """
        X_train, X_test, y_train, y_test = train_test_split(
            self.X.copy(), self.y.copy(), test_size=test_size
        )
        clf.fit(X_train, y_train)
        predictions = clf.predict_proba(X_test)
        predictions = pd.DataFrame(
            clf.predict_proba(X_test), columns=list(map(str, clf.classes_))
        ).assign(actual=y_test.values)
        return predictions

    def run(self, trials=100, test_size=0.2):
        """Resample and re-run the model multiple times.
        """
        clf = self.model()
        predictions = []
        for i in range(trials):
            p = self.fit_train_predict(clf, test_size=test_size).assign(trial=i)
            predictions.append(p)
        return pd.concat(predictions, ignore_index=True)


class Logistic(BaseModel):

    def model(self):
        lr = LogisticRegression(fit_intercept=False)
        clf = self.build_model(lr, self.n_vars, self.c_vars)
        return clf

class SVM(BaseModel):

    def model(self):
        sv =  SVC(probability=True)
        clf = self.build_model(sv, self.n_vars, self.c_vars)
        return clf


def plot_results(self, predictions):
    pass


def LR_seasonal_load(df, y_col):
    """
    """
    model = Logistic(df, y_col, ["load"], ["is_weekday", "month"])
    results = model.run()
    return results


def LR_seasonal_load_with_weather(df, y_col):
    """
    curtailment_event ~ load + C(is_weekday) + C(month) + t_mean + t_absmin + t_absmax + dswrf_mean + dswrf_absmax
    """
    model = Logistic(
        df,
        y_col,
        ["load", "t_mean", "t_absmax", "t_absmin", "dswrf_mean", "dswrf_absmax"],
        ["is_weekday", "month",],
    )
    results = model.run()
    return results


def LR_seasonal_load_with_weather_capacity_weighted(df, y_col):
    model = Logistic(
        df,
        y_col,
        ["load", "t_wmean", "t_wmin", "t_wmax", "dswrf_wmean", "dswrf_absmax"],
        ["is_weekday", "month",],
    )
    results = model.run()
    return results


def SVM_seasonal_load_with_weather_capacity_weighted(df, y_col):
    model = SVM(
        df,
        y_col,
        ["load", "t_wmean", "t_wmin", "t_wmax", "dswrf_wmean", "dswrf_absmax"],
        ["is_weekday", "month",],
    )
    results = model.run()
    return results

registry = [
    LR_seasonal_load,
    LR_seasonal_load_with_weather,
    LR_seasonal_load_with_weather_capacity_weighted,
    SVM_seasonal_load_with_weather_capacity_weighted,
]


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(exist_ok=True)
    data = pd.read_parquet(INPUT_DIR / "1_labeled_curtailment_events.parquet")

    # ad-hoc minute feature labeling to support sklearn
    data["month"] = data["timestamp"].dt.month

    events = data.columns[data.columns.str.match(r"curtailment_event_\d.\d\d")]
    results = {}
    for m in registry:
        for event in events:
            logger.info("Training {m} on {event}", event=event, m=m.__name__)
            predictions = m(data, event)
            results[m.__name__] = {event: predictions}
            fn = OUTPUT_DIR / f"predictions-{m.__name__}-{event}.parquet"
            predictions.to_parquet(fn, index=False)
    