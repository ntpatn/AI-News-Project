import great_expectations as gx


def silver_expectations_dbt_combined():
    E = gx.expectations

    return [
        # ---------- Column existence ----------
        E.ExpectColumnToExist(column="sf_id"),
        E.ExpectColumnToExist(column="source_system"),
        E.ExpectColumnToExist(column="source_name"),
        E.ExpectColumnToExist(column="article_id"),
        E.ExpectColumnToExist(column="author"),
        E.ExpectColumnToExist(column="title"),
        E.ExpectColumnToExist(column="description"),
        E.ExpectColumnToExist(column="url"),
        E.ExpectColumnToExist(column="image_url"),
        E.ExpectColumnToExist(column="category"),
        E.ExpectColumnToExist(column="language"),
        E.ExpectColumnToExist(column="region"),
        E.ExpectColumnToExist(column="content"),
        E.ExpectColumnToExist(column="published_at"),
        E.ExpectColumnToExist(column="createdate"),
        E.ExpectColumnToExist(column="updatedate"),
        E.ExpectColumnToExist(column="batch_id"),
        E.ExpectColumnToExist(column="layer"),
        E.ExpectColumnToExist(column="usercreate"),
        E.ExpectColumnToExist(column="userupdate"),
        E.ExpectColumnToExist(column="activedata"),

        # ---------- Basic Not Null constraints ----------
        E.ExpectColumnValuesToNotBeNull(column="url"),
        E.ExpectColumnValuesToNotBeNull(column="published_at"),
        E.ExpectColumnValuesToNotBeNull(column="sf_id"),
        E.ExpectColumnValuesToNotBeNull(column="layer"),

        # ---------- URL must be basic valid ----------
        E.ExpectColumnValuesToMatchRegex(
            column="url",
            regex=r"^https?://.+",
        ),

        # ---------- Date format check ----------
        E.ExpectColumnValuesToMatchRegex(
            column="published_at",
            regex=r"^\d{4}-\d{2}-\d{2}",
        ),

        # ---------- Unique natural keys ----------
        E.ExpectColumnValuesToBeUnique(column="url"),

        # ---------- Title length constraint ----------
        E.ExpectColumnValueLengthsToBeBetween(
            column="title",
            min_value=1,
            max_value=5000,
        ),

        # ---------- layer must = silver ----------
        E.ExpectColumnValuesToBeInSet(
            column="layer",
            value_set=["silver"],
        ),

        # ---------- activedata must boolean ----------
        E.ExpectColumnValuesToBeInSet(
            column="activedata",
            value_set=[True, False],
        ),
    ]
