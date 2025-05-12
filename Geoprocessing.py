import time
import requests
import fitz
import re
import pandas as pd
import geopandas as gpd
from rapidfuzz import process, fuzz
import os

# ---------------------------
# Configuration
# ---------------------------

sale_number = "231"
pdf_filename = f"Sale-List-{sale_number}.pdf"
csv_filename = f"property_tax_sale_{sale_number}.csv"
standardized_csv_filename = f"standardized_property_tax_sale_{sale_number}.csv"
matched_output_filename = f"matched_addresses_{sale_number}.csv"

pdf_url = f"https://www.stlouis-mo.gov/government/departments/sheriff/documents/upload/{pdf_filename}"
shapefile_path = "/home/bradstricherz/PycharmProjects/STLTaxAuctionScripts/shapefiles/prcl_address.shp"


# ---------------------------
# Step 1: Check if PDF exists
# ---------------------------

def check_pdf(url, retries=3, delay=3):
    """
    Check if a PDF exists at the given URL, retrying on failure.
    - retries: number of attempts
    - delay: seconds to wait between retries
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.head(url, allow_redirects=True, timeout=25)
            if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
                print("✅ PDF is available.")
                return True
            else:
                print(f"❌ PDF not found (status {response.status_code}), attempt {attempt}/{retries}")
        except requests.RequestException as e:
            print(f"⚠️ Request error on attempt {attempt}/{retries}: {e}")

        if attempt < retries:
            time.sleep(delay)

    print("❌ All attempts failed. PDF not available.")
    return False


# ---------------------------
# Step 2: Download PDF if available
# ---------------------------

def download_pdf(url, save_path):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            print(f"📥 PDF downloaded to {save_path}")
            return True
        else:
            print(f"❌ Failed to download PDF. Status: {response.status_code}")
            return False
    except Exception as e:
        print(f"⚠️ Error downloading PDF: {e}")
        return False


# ---------------------------
# Step 3: Extract Property Data
# ---------------------------

def extract_property_data(pdf_path, output_csv):
    if not os.path.exists(pdf_path):
        print(f"Error: File '{pdf_path}' not found.")
        return False

    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"Error opening PDF: {e}")
        return False

    try:
        full_text = ""
        for page_num, page in enumerate(doc, start=1):
            try:
                full_text += page.get_text()
            except Exception as e:
                print(f"Error reading page {page_num}: {e}")
                continue
    finally:
        doc.close()

    pattern = r"(\d{3}-\d{3}(?:-\d{3})?)\s+(.+?)\s+(\d{1,5}\s[\w\s\.\'\-]+?)\s+\$(\d{1,3}(?:,\d{3})*(?:\.\d{2}))"
    try:
        matches = re.findall(pattern, full_text)
        if not matches:
            print("Warning: No matches found.")
            return False
    except re.error as e:
        print(f"Regex error: {e}")
        return False

    try:
        df = pd.DataFrame(matches, columns=["Land Tax #", "Owner", "Address", "Total Owed"])
        df.to_csv(output_csv, index=False)
        print(f"✅ Data saved to '{output_csv}'")
        return True
    except Exception as e:
        print(f"Error saving CSV: {e}")
        return False


# ---------------------------
# Step 4: Fuzzy Match to Shapefile
# ---------------------------

def get_best_match(address, choices, scorer=fuzz.token_sort_ratio, threshold=25):
    match, score, _ = process.extractOne(address, choices, scorer=scorer)
    if score >= threshold:
        return match, score
    else:
        return None, score


def fuzzy_match_addresses(csv_path, shapefile_path, output_path):
    # Load data
    csv_df = pd.read_csv(csv_path)
    shapefile_gdf = gpd.read_file(shapefile_path)

    # Columns of addresses
    csv_df['Standard Address'] = csv_df['Address']
    shapefile_gdf['Standard Address'] = shapefile_gdf['SITEADDR']

    # Match each CSV address to shapefile address
    matches = []
    for addr in csv_df['Standard Address']:
        best_match, score = get_best_match(addr, shapefile_gdf['Standard Address'])
        matches.append({'Standard Address': addr, 'Matched Address': best_match, 'Score': score})

    # Merge match results back to CSV data
    matches_df = pd.DataFrame(matches)
    result_df = pd.merge(csv_df, matches_df, on='Standard Address', how='left')

    # Optionally merge in spatial attributes
    result_df = result_df.merge(shapefile_gdf, left_on='Matched Address', right_on='Standard Address',
                                suffixes=('', '_shp'))

    # Save final matched result
    result_df.to_csv(output_path, index=False)
    print(f"✅ Matching complete. Saved to {output_path}")

    # Save imperfect matches (Score < 100)
    imperfect_matches = result_df[result_df['Score'] < 100]
    imperfect_matches.to_csv("imperfect_matches.csv", index=False)
    print(f"⚠️  {len(imperfect_matches)} imperfect matches saved to imperfect_matches.csv")


# ---------------------------
# MAIN: Run Sequence
# ---------------------------

if check_pdf(pdf_url):
    if download_pdf(pdf_url, pdf_filename):
        if extract_property_data(pdf_filename, csv_filename):
            df = pd.read_csv(csv_filename)
            df['Standard Address'] = df['Address']
            df.to_csv(standardized_csv_filename, index=False)
            print(f"✅ Addresses copied for matching in {standardized_csv_filename}")

            fuzzy_match_addresses(
                csv_path=standardized_csv_filename,
                shapefile_path=shapefile_path,
                output_path=matched_output_filename
            )
            for f in [pdf_filename, csv_filename, standardized_csv_filename]:
                try:
                    os.remove(f)
                    print(f"🗑️  Deleted intermediate file: {f}")
                except Exception as e:
                    print(f"⚠️  Could not delete {f}: {e}")
        else:
            print("❌ Failed to extract property data.")
    else:
        print("❌ Failed to download PDF.")
else:
    print("🔁 No new sale list PDF available.")
