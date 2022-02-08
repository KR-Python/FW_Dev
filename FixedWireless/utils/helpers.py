__all__ = ['sanitize_value', 'convert_arcpy_bool', 'set_column_width']


def sanitize_value(name):
    fixed_name = [x for x in name.replace(' ','_') if x.isalnum() or x == '_']
    fixed_string = ''.join(fixed_name)
    return fixed_string


def convert_arcpy_bool(value):
    if value == 'true':
        return True
    elif value == 'false':
        return False
    else:
        raise ValueError(f"Invalid Value: {value} is not a member of ('true', 'false')")


def set_column_width(sheet_name, df, writer):
    """sets the length of a column in the output excel table to the longest string in a series"""
    worksheet = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns):
        series = df[col]
        max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
        worksheet.set_column(idx, idx, max_len)