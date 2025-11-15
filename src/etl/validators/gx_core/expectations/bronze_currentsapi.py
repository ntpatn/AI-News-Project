# dq/expectations/bronze_currentsapi.py
import great_expectations as gx


def bronze_expectations_currentsapi():
    E = gx.expectations

    return [
        # required columns exist
        E.ExpectColumnToExist(column="id"),
        E.ExpectColumnToExist(column="url"),
        E.ExpectColumnToExist(column="title"),
        E.ExpectColumnToExist(column="published"),
        E.ExpectColumnToExist(column="language"),
        # basic not-null on critical fields
        E.ExpectColumnValuesToNotBeNull(column="id"),
        E.ExpectColumnValuesToNotBeNull(column="url"),
        E.ExpectColumnValuesToNotBeNull(column="title"),
        # url should look like URL (not strict)
        E.ExpectColumnValuesToMatchRegex(column="url", regex=r"^https?://", mostly=0.9),
        # published date looks like timestamp
        E.ExpectColumnValuesToMatchRegex(
            column="published", regex=r"^\d{4}-\d{2}-\d{2}", mostly=0.9
        ),
    ]
