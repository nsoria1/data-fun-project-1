import decimal


def default_value(o):
    if isinstance(o, decimal.Decimal):
        return float(o)
    raise TypeError(f'Type {type(o)} is not serializable')
