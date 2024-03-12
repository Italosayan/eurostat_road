# I'm trying to match the values in the following article:
# https://ec.europa.eu/eurostat/statistics-explained/index.php?title=File:Top_20_country-to-country_flows_in_intra-EU_road_freight_transport,_2022_(million_tonnes).png

from typing import List, Any
import pandas as pd

def process_road_euro_stat(
    df: pd.DataFrame, cols: List[str], first_col: str
) -> pd.DataFrame:
    """Splits a specified column into multiple columns, removes the original column, and then melts the DataFrame.

    Args:
        df: The DataFrame to process.
        cols: The list of column names for the newly split columns.
        first_col: The column name to split and remove.

    Returns:
        A melted DataFrame with specified id_vars.
    """
    split_columns = df[first_col].str.split(',', expand=True)
    split_columns.columns = cols
    df = df.drop(columns=[first_col])
    df = pd.concat([split_columns, df], axis=1)
    df.columns = [col.strip() for col in df.columns]
    id_vars = split_columns.columns
    return pd.melt(df, id_vars=id_vars, var_name='TIME_PERIOD', value_name='value')

def get_country_codes(excel_path: str) -> pd.DataFrame:
    """Reads country codes from an Excel file, applies updates, and adds new entries.

    Args:
        excel_path: Path to the Excel file containing country codes.

    Returns:
        A DataFrame with updated country codes and names.
    """
    country_codes = pd.read_excel(excel_path)
    country_updates = {"US": "United States", "GB": "United Kingdom", "NL": "Netherlands", "MD": "Moldova"}
    code_updates = {"Greece": "EL", "United Kingdom": "UK"}

    for code, country in country_updates.items():
        country_codes.loc[country_codes["Alpha-2 code"] == code, "Country"] = country

    for country, code in code_updates.items():
        country_codes.loc[country_codes.Country == country, "Alpha-2 code"] = code

    new_rows = [['Kosovo', 'XK', 'XKO', 9999]]
    new_row_df = pd.DataFrame(new_rows, columns=['Country', 'Alpha-2 code', 'Alpha-3 code', 'Numeric'])
    country_codes = pd.concat([country_codes, new_row_df], ignore_index=True)

    return country_codes

def merge_and_rename(
    df: pd.DataFrame, merge_on: str, new_column_name: str
) -> pd.DataFrame:
    """Merges the input DataFrame with country codes and renames the merged columns.

    Args:
        df: The DataFrame to merge.
        merge_on: The column name on which to merge.
        new_column_name: The new name for the merged column.

    Returns:
        The merged and renamed DataFrame.
    """
    country_codes = get_country_codes("data/country_code.xlsx")
    df = df.merge(
        country_codes[["Country", "Alpha-2 code"]], 
        how="left", 
        left_on=merge_on, 
        right_on="Alpha-2 code"
    )
    df = df.rename(columns={"Country": new_column_name})
    df.drop(columns=["Alpha-2 code"], inplace=True)
    return df

lgtt = pd.read_csv("data/estat_road_go_ia_lgtt.tsv", sep='\t', na_values=[': ',': z',": u"])
ugtt = pd.read_csv("data/estat_road_go_ia_ugtt.tsv", sep='\t', na_values=[': ',': z',": u"])
cross = pd.read_csv("data/estat_road_go_cta_gtt.tsv", sep='\t', na_values=[': ',': z',": u"])

#dataset columns
lgtt_cols = ['freq', 'tra_type', 'c_unload', 'nst07' ,'unit', 'geo']
lgtt_fc = 'freq,tra_type,c_unload,nst07,unit,geo\TIME_PERIOD'

ugtt_cols = ['freq', 'tra_type', 'c_load', 'nst07' ,'unit', 'geo']
ugtt_fc = 'freq,tra_type,c_load,nst07,unit,geo\TIME_PERIOD'

cross_cols = ['freq', 'tra_type', 'c_load', 'c_unload','nst07' ,'unit', 'geo']
cross_fc = 'freq,tra_type,c_load,c_unload,nst07,unit,geo\TIME_PERIOD'

#processing
lgtt = process_road_euro_stat(lgtt,lgtt_cols,lgtt_fc)
ugtt = process_road_euro_stat(ugtt,ugtt_cols,ugtt_fc)
cross = process_road_euro_stat(cross,cross_cols,cross_fc)

# Applying the function to each DataFrame
lgtt = merge_and_rename(lgtt, "c_unload", "unload_country")
lgtt = merge_and_rename(lgtt, "geo", "geo_country")

ugtt = merge_and_rename(ugtt, "c_load", "load_country")
ugtt = merge_and_rename(ugtt, "geo", "geo_country")

cross = merge_and_rename(cross, "c_load", "load_country")
cross = merge_and_rename(cross, "c_unload", "unload_country")

lgtt = lgtt[(lgtt.tra_type=="TOTAL") &
        (lgtt.unit=="THS_T") &
        (lgtt.nst07=="TOTAL") & 
        (lgtt.TIME_PERIOD=="2022")&
        (lgtt.c_unload.str.len() == 2)]

ugtt = ugtt[(ugtt.tra_type=="TOTAL") &
        (ugtt.unit=="THS_T") &
        (ugtt.nst07=="TOTAL") & 
        (ugtt.TIME_PERIOD=="2022")&
        (ugtt.c_load.str.len() == 2)]

cross = cross[(cross.tra_type=="TOTAL") &
        (cross.unit=="THS_T") &
        (cross.nst07=="TOTAL") & 
        (cross.TIME_PERIOD=="2022")&
        (cross.c_load.str.len() == 2)&
        (cross.c_unload.str.len() == 2)]

cross_be_fr = cross[(cross.load_country == "Belgium")&(cross.unload_country=="France")]["value"].sum()
lgtt_be_fr = lgtt[(lgtt.geo_country == "Belgium")&(lgtt.unload_country=="France")].value.values[0]
ugtt_be_fr = ugtt[(ugtt.load_country == "Belgium")&(ugtt.geo_country=="France")].value.values[0]

print(cross_be_fr+lgtt_be_fr+ugtt_be_fr) #I get 31.8 M but! 56.6 M is the reported value in the article