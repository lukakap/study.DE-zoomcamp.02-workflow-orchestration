if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import re

def camel_to_snake(name):
    import re
    # Insert underscores before capital letters and convert to lowercase
    snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake).lower()


def is_camel_case(s):
    return re.search('[a-z][A-Z]', s) is not None


@transformer
def transform(data, *args, **kwargs):
    # drop zero passenger rows
    print(f"zero passenger counts - {(data['passenger_count'] <= 0).sum()}")
    data = data[data['passenger_count'] > 0]

    # drop zero trip distance rows
    print(f"zero trip distances - {(data['trip_distance'] <= 0).sum()}")
    data = data[data['trip_distance'] > 0]

    # add pickup date
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # change columns to snake format
    # Count and convert camelCase column names
    camel_case_count = 0
    new_columns = []
    print(data.column)

    for col in data.columns:
        if is_camel_case(col):
            camel_case_count += 1
            new_columns.append(camel_to_snake(col))
        else:
            new_columns.append(col)

    print(f"columns changed from camel case to snake - {camel_case_count}")
    data.columns = new_columns

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns
    assert output['passenger_count'].isin([0]).sum() == 0
    assert (output['trip_distance'] <= 0).sum() == 0
