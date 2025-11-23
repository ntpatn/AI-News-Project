import great_expectations as gx


def silver_expectations_currentsapi():
    E = gx.expectations

    return [
        # ---------- Column existence ----------
        E.ExpectColumnToExist(column="id"),
        E.ExpectColumnToExist(column="url"),
        E.ExpectColumnToExist(column="title"),
        E.ExpectColumnToExist(column="published_at"),
        E.ExpectColumnToExist(column="sf_id"),
        E.ExpectColumnToExist(column="language"),
        E.ExpectColumnToExist(column="layer"),
        E.ExpectColumnToExist(column="source_system"),
        E.ExpectColumnToExist(column="source_news"),
        # ---------- Strict Not null ----------
        E.ExpectColumnValuesToNotBeNull(column="id"),
        E.ExpectColumnValuesToNotBeNull(column="url"),
        # E.ExpectColumnValuesToNotBeNull(column="title", mostly=0.95),
        E.ExpectColumnValuesToNotBeNull(column="published_at"),
        E.ExpectColumnValuesToNotBeNull(column="sf_id"),
        # ---------- URL must be valid ----------
        E.ExpectColumnValuesToMatchRegex(
            column="url",
            regex=r"^https?://.+",
        ),
        # ---------- published_at must be real date ----------
        E.ExpectColumnValuesToMatchRegex(
            column="published_at",
            regex=r"^\d{4}-\d{2}-\d{2}",
        ),
        # ---------- Unique URL ----------
        E.ExpectColumnValuesToBeUnique(column="url"),
        # ---------- Title constraints ----------
        E.ExpectColumnValueLengthsToBeBetween(
            column="title",
            min_value=1,
            max_value=5000,
        ),
        # ---------- Language must be normalized ----------
        # E.ExpectColumnValuesToMatchRegex(
        #     column="language",
        #     regex=r"^[a-z]{2,5}$",
        # ),
        # ---------- layer must = silver ----------
        E.ExpectColumnValuesToBeInSet(
            column="layer",
            value_set=["silver"],
        ),
        # ---------- source_system must = currentsapi ----------
        E.ExpectColumnValuesToBeInSet(
            column="source_system",
            value_set=["currentsapi"],
        ),
    ]
