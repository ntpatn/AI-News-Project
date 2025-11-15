from src.etl.validators.function.base_quality_validator import BaseDQEngine


class PandasDQEngine(BaseDQEngine):
    def schema(self, df, columns):
        return set(columns) - set(df.columns)

    def not_null(self, df, columns):
        errors = []
        for col in columns:
            if df[col].isnull().sum() > 0:
                errors.append(col)
        return errors

    def unique(self, df, columns):
        return df.duplicated(subset=columns).sum()

    def dtypes(self, df, rules):
        wrong = []
        for col, dtype in rules.items():
            if str(df[col].dtype) != dtype:
                wrong.append(col)
        return wrong
