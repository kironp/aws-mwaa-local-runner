import re

from utils.row import Row


def camel_to_snake(camelStr):
    return re.sub('(?!^)([A-Z]+)', r'_\1', camelStr).lower()


class BaseAthenaResultSetRow:
    def __init__(self, value):
        self.values = [x.get("VarCharValue") for x in value.get('Data')]


class DataRow(BaseAthenaResultSetRow):

    def __init__(self, value, index):
        super().__init__(value)
        self.index = index


class HeaderRow(BaseAthenaResultSetRow):
    def __init__(self, value):
        super().__init__(value)


class Column:
    def __init__(self, value, index):
        self.index = index
        col_metadata = dict(**value)
        for k in col_metadata:
            col_metadata[camel_to_snake(k)] = col_metadata.pop(k)
        self.__dict__.update(**col_metadata)


ATHENA_DATA_TYPE_FUNCTION = {
    'varchar': str,
    'bigint': int,
    'decimal': float,
    'integer': int
}


class AthenaResultSet:

    def _add_columns(self, column_info):
        for index, column in enumerate(column_info):
            r = Column(column, index)
            self.columns.append(r)

    def _add_rows(self, row_info):
        for index, row in enumerate(row_info):
            if index == 0:
                self.header = HeaderRow(row)
            else:
                raw_values = DataRow(row, index).values
                type_functions = [ATHENA_DATA_TYPE_FUNCTION[col.type] for col in self.columns]

                values = []
                for elem in zip(type_functions, raw_values):
                    f, value = elem
                    values.append(f(value))

                self.rows.append(Row(headers=[col.name for col in self.columns],
                                     values=values))

    def __init__(self, result_set):
        self.columns = []
        self.rows = []
        self._add_columns(result_set['ResultSet']['ResultSetMetadata']['ColumnInfo'])
        self._add_rows(result_set['ResultSet']['Rows'])