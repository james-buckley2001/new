from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd

wd = Path(r'C:\Users\BUCKLEJ1\OneDrive - Jacobs\Documents\River_Tolka\python_files')
catchment_fp = wd / 'target_inflows.csv'

df = pd.read_csv(catchment_fp)

def area_weight_pcds(df, hep_id, pcd_to_weight):
    # Get the row for the current HEP_ID
    row = df.loc[df['HEP_ID'] == int(hep_id)]
    print(row['HEP_ID'].values[0]) #useful for checking - shows the HEP the code is looking at
    if row.empty:
        return 'one row not found'
    # Calculate the product of AREA and the PCD for the current row
    area_weighted_pcd  = row['Distributed_AREA'].values[0] * row[pcd_to_weight].values[0]
    #get the cumulative area for the current row to be used to calculate the final area weighted values at the end
    total_area_at_start_hep = row['AREA'].values[0]
    # Get the upstream HEP_IDs from the current row
    upstream_hep_ids = row['Upstream HEP_ID'].values[0]
    # If there are two upstream HEP_IDs, split them and recursively process each one
    if isinstance(upstream_hep_ids, str) and ',' in upstream_hep_ids:
        upstream_hep_ids = [id.strip() for id in upstream_hep_ids.split(',')]
        upstream_summed_area_weighted_pcd = sum(area_weight_pcds(df, id, pcd_to_weight)[0] for id in upstream_hep_ids)
    # If there's a single upstream HEP_ID, recursively process it
    elif pd.notna(upstream_hep_ids):
        upstream_summed_area_weighted_pcd, _ = area_weight_pcds(df, upstream_hep_ids, pcd_to_weight)
    else:
    #when there are no more upstream HEPs just add 0 to the existing total
        upstream_summed_area_weighted_pcd = 0
    return area_weighted_pcd + upstream_summed_area_weighted_pcd, total_area_at_start_hep

def create_cumulative_netlen(df, hep_id):
    # Get the row for the current HEP_ID
    row = df.loc[df['HEP_ID'] == int(hep_id)]
    print(row['HEP_ID'].values[0]) #useful for checking - shows the HEP the code is looking at
    if row.empty:
        return 'one row not found'
    current_netlen  = row['Inflow_NETLEN'].values[0]
    # Get the upstream HEP_IDs from the current row
    upstream_hep_ids = row['Upstream HEP_ID'].values[0]
    # If there are two upstream HEP_IDs, split them and recursively process each one
    if isinstance(upstream_hep_ids, str) and ',' in upstream_hep_ids:
        upstream_hep_ids = [id.strip() for id in upstream_hep_ids.split(',')]
        upstream_summed_netlen = sum(create_cumulative_netlen(df, id) for id in upstream_hep_ids)
    # If there's a single upstream HEP_ID, recursively process it
    elif pd.notna(upstream_hep_ids):
        upstream_summed_netlen = create_cumulative_netlen(df, upstream_hep_ids)
    else:
    #when there are no more upstream HEPs just add 0 to the existing total
        upstream_summed_netlen = 0
    return current_netlen + upstream_summed_netlen

if __name__ == '__main__':
    #example use of code for checking purposes
    upstream_product, total_area = area_weight_pcds(df, hep_id = 0, pcd_to_weight='Inflow_Urbext')
    final_area_weighted_value = upstream_product / total_area

    #use for all data
    df_copy = df.copy()
    df_copy['cumulative_urbext'] = df_copy['HEP_ID'].apply(
        lambda hep_id: area_weight_pcds(df_copy, hep_id, pcd_to_weight='Inflow_Urbext')[0] /
                    area_weight_pcds(df_copy, hep_id, pcd_to_weight='Inflow_Urbext')[1])
    
    df_copy['cumulative_S1085'] = df_copy['HEP_ID'].apply(
        lambda hep_id: area_weight_pcds(df_copy, hep_id, pcd_to_weight='Inflow_S1085')[0] /
                    area_weight_pcds(df_copy, hep_id, pcd_to_weight='Inflow_S1085')[1])
    
    df_copy['cumulative_SAAR'] = df_copy['HEP_ID'].apply(
        lambda hep_id: area_weight_pcds(df_copy, hep_id, pcd_to_weight='Inflow_SAAR')[0] /
                    area_weight_pcds(df_copy, hep_id, pcd_to_weight='Inflow_SAAR')[1])
    
    df_copy['cumulative_BFISOILS'] = df_copy['HEP_ID'].apply(
        lambda hep_id: area_weight_pcds(df_copy, hep_id, pcd_to_weight='Inflow_BFISOILS')[0] /
                    area_weight_pcds(df_copy, hep_id, pcd_to_weight='Inflow_BFISOILS')[1])
    
    df_copy['cumulative_NETLEN'] = df_copy['HEP_ID'].apply(lambda hep_id: create_cumulative_netlen(df_copy, hep_id))

    df_copy['DRAIND'] = df_copy['cumulative_NETLEN'] / df_copy['AREA']

    output_fp = wd / 'target_inflows_output.csv'
    df_copy.to_csv(output_fp)

    print(df_copy)

















