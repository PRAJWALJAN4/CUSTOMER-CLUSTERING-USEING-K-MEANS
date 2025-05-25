import pandas as pd
import numpy as np

def load_data(csv_path):
    df = pd.read_csv(csv_path)
    print("\n📦 Loaded Original Data (first 5 rows):")
    print(df.head())
    return df

def normalize(df):
    df_norm = df.copy()
    for column in ['total_spent', 'purchase_count', 'recency']:
        min_val = df[column].min()
        max_val = df[column].max()
        df_norm[f'norm_{column}'] = (df[column] - min_val) / (max_val - min_val)
    
    df_norm = df_norm[['customer_id', 'norm_total_spent', 'norm_purchase_count', 'norm_recency']]
    print("\n📊 Normalized Data (first 5 rows):")
    print(df_norm.head())
    return df_norm

def assign_random_clusters(df_norm):
    df_norm['cluster'] = np.random.randint(1, 5, size=len(df_norm))
    print("\n🔀 Randomly Assigned Clusters (first 5 rows):")
    print(df_norm.head())
    return df_norm

def compute_centroids(df):
    centroids = df.groupby('cluster')[['norm_total_spent', 'norm_purchase_count', 'norm_recency']].mean()
    print("\n📌 Cluster Centroids:")
    print(centroids)
    return centroids

def reassign_clusters(df, centroids):
    def closest_cluster(row):
        distances = centroids.apply(lambda c: 
            abs(row['norm_total_spent'] - c['norm_total_spent']) + 
            abs(row['norm_purchase_count'] - c['norm_purchase_count']) + 
            abs(row['norm_recency'] - c['norm_recency']), axis=1)
        return distances.idxmin()
    df['cluster'] = df.apply(closest_cluster, axis=1)
    return df

def display_statistics(df):
    stats = df.groupby('cluster')[['norm_total_spent', 'norm_purchase_count', 'norm_recency']].agg(['mean', 'count'])
    print("\n📈 Final Cluster Statistics:")
    print(stats)

def main():
    # 📌 Update this path as needed
    csv_path = r"C:\Users\User\OneDrive\Desktop\DMDW\customer_data.csv"
    df_raw = load_data(csv_path)

    df_norm = normalize(df_raw)
    df_clustered = assign_random_clusters(df_norm)

    for i in range(3):
        print(f"\n🔁 Refining clusters (Iteration {i + 1})...")
        centroids = compute_centroids(df_clustered)
        df_clustered = reassign_clusters(df_clustered, centroids)

    display_statistics(df_clustered)

    # Save outputs
    base_path = r"C:\Users\User\OneDrive\Desktop\DMDW"
    df_norm.to_csv(f"{base_path}\\normalized_data.csv", index=False)
    df_clustered.to_csv(f"{base_path}\\clustered_output.csv", index=False)
    centroids.to_csv(f"{base_path}\\final_centroids.csv")
    
    print("\n✅ Output files saved:")
    print(f"- Normalized: {base_path}\\normalized_data.csv")
    print(f"- Clustered: {base_path}\\clustered_output.csv")
    print(f"- Centroids: {base_path}\\final_centroids.csv")

if __name__ == "__main__":
    main()
